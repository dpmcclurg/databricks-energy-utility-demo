# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Forecast: one automated HDD model for every region
# MAGIC
# MAGIC The method FP&A already trusts, a heating-degree-day regression, run the same way for every region
# MAGIC and per customer, the standard practice for gas utility load forecasts. For each region:
# MAGIC
# MAGIC `use per customer per day = a + b x max(0, balance_point - tmean) + c x holiday_flag`
# MAGIC
# MAGIC `volume_dth = customers x use per customer per day`
# MAGIC
# MAGIC - The balance point is chosen per region by grid search (45F to 65F), instead of a fixed 65F.
# MAGIC - The holiday term captures second-home occupancy in the mountains.
# MAGIC - Modeling use per customer separates customer growth from weather and behavior.
# MAGIC - It is fit with `groupBy(region).applyInPandas`, so a new region is new data, not a new workbook.
# MAGIC - Training uses the five gas years before the forecast date.
# MAGIC
# MAGIC **Season plan (published October 1):** every complete historical winter since 1995-96 is run through
# MAGIC the model with the planned customer counts (the September count grown at its three-year trend). The
# MAGIC spread gives P10 (warm), P50 (expected), and P90 (cold) Dth by region and month.
# MAGIC
# MAGIC **Backtest:** the same procedure, and an "Excel-style" baseline (monthly volume on monthly HDD65,
# MAGIC forecast with average monthly HDD), are run for the three winters before the plan using only data
# MAGIC available before each winter.
# MAGIC
# MAGIC The plan is fixed once it is made: if the champion model was already fit for this forecast date, the
# MAGIC task leaves it as is, so later runs in the season track against the plan the trading desk received.

# COMMAND ----------

# MAGIC %pip install -q "mlflow>=3.1"
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("experiment", "")
dbutils.widgets.text("season", "2026")
CATALOG = dbutils.widgets.get("catalog")
EXPERIMENT = dbutils.widgets.get("experiment")
SEASON = int(dbutils.widgets.get("season"))
assert CATALOG and EXPERIMENT, "catalog and experiment parameters are required"
FPA = f"`{CATALOG}`.cgu_fpa"
MODEL_NAME = f"{CATALOG}.cgu_fpa.volume_forecaster"

FORECAST_DATE = f"{SEASON}-10-01"
SEASON_LABEL = f"{SEASON}-{str(SEASON + 1)[2:]}"
SCENARIO_YEARS = range(1995, SEASON)          # winters 1995-96 to the one before the plan
BACKTEST_SEASONS = [SEASON - 3, SEASON - 2, SEASON - 1]
TRAIN_YEARS = 5
MIN_WINTER_DAYS = 145                          # of 151; thinner station records are left out of the scenarios

# COMMAND ----------

import json

import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

mlflow.set_registry_uri("databricks-uc")
client = mlflow.MlflowClient()
try:
    current = client.get_model_version_by_alias(MODEL_NAME, "champion")
except Exception:
    current = None
if current is not None and current.tags.get("forecast_date") == FORECAST_DATE:
    print(f"The {SEASON_LABEL} plan was made on {FORECAST_DATE} with model v{current.version}; leaving it unchanged.")
    dbutils.notebook.exit(json.dumps({"skipped": True, "reason": f"plan for {FORECAST_DATE} already made",
                                      "model": {"name": "cgu_fpa.volume_forecaster", "version": current.version, "alias": "champion"}}))

mlflow.set_experiment(EXPERIMENT)

gold = spark.table(f"{FPA}.gold_daily_region").where("tmean IS NOT NULL AND customers IS NOT NULL")
weather = (spark.table(f"`{CATALOG}`.cgu_core.silver_weather_daily").where("region_id IS NOT NULL")
           .select("region_id", "gas_day", "tmean").toPandas())
weather["gas_day"] = pd.to_datetime(weather.gas_day)
calendar = spark.table(f"{FPA}.ref_holiday_calendar").toPandas()
calendar["gas_day"] = pd.to_datetime(calendar.gas_day)
holiday = calendar.set_index("gas_day").holiday_flag
cust = (spark.table(f"`{CATALOG}`.cgu_core.silver_customer_count")
        .select("region_id", "month", (F.col("residential") + F.col("commercial")).alias("total")).toPandas())
cust["month"] = pd.to_datetime(cust.month)

# Winters with enough station days to use as a weather scenario, per region
winter = weather[weather.gas_day.dt.month.isin([11, 12, 1, 2, 3])]
winter = winter.assign(season=winter.gas_day.dt.year.where(winter.gas_day.dt.month >= 11, winter.gas_day.dt.year - 1))
winter_days = winter.groupby(["region_id", "season"]).size()
usable = {r: {int(y) for (rr, y), n in winter_days.items() if rr == r and n >= MIN_WINTER_DAYS} for r in weather.region_id.unique()}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit per region with `applyInPandas`

# COMMAND ----------

PARAM_SCHEMA = ("fit_key STRING, region_id STRING, balance_point DOUBLE, intercept DOUBLE, slope DOUBLE, "
                "holiday DOUBLE, r2 DOUBLE, mape_daily DOUBLE, rmse DOUBLE, x_mean DOUBLE, x_sxx DOUBLE, n_days INT")


def fit_region(pdf: pd.DataFrame) -> pd.DataFrame:
    """Change-point HDD regression on use per customer: grid-search the balance point, least squares for
    the rest. Keeps the residual error and HDD spread for the design-day forecast-error band."""
    y = pdf.use_per_customer.to_numpy()
    best = None
    for bp in np.arange(45.0, 65.01, 0.5):
        x = np.maximum(0, bp - pdf.tmean.to_numpy())
        X = np.column_stack([np.ones(len(pdf)), x, pdf.holiday_flag.to_numpy()])
        coef, *_ = np.linalg.lstsq(X, y, rcond=None)
        sse = float(((X @ coef - y) ** 2).sum())
        if best is None or sse < best[0]:
            best = (sse, bp, coef, X @ coef, x)
    sse, bp, coef, fitted, x = best
    r2 = 1 - sse / float(((y - y.mean()) ** 2).sum())
    return pd.DataFrame([{
        "fit_key": pdf.fit_key.iloc[0], "region_id": pdf.region_id.iloc[0], "balance_point": bp,
        "intercept": coef[0], "slope": coef[1], "holiday": coef[2], "r2": r2,
        "mape_daily": float(np.mean(np.abs(fitted - y) / y)), "rmse": float(np.sqrt(sse / (len(y) - 3))),
        "x_mean": float(x.mean()), "x_sxx": float(((x - x.mean()) ** 2).sum()), "n_days": len(pdf),
    }])


def training_window(season: int):
    """Five gas years (Oct 1 to Sep 30) before the winter being forecast."""
    return f"{season - TRAIN_YEARS}-10-01", f"{season}-09-30"


fits = []
for season in [SEASON] + BACKTEST_SEASONS:
    lo, hi = training_window(season)
    fits.append(gold.where(F.col("gas_day").between(lo, hi)).withColumn("fit_key", F.lit(str(season))))
train = fits[0]
for f_ in fits[1:]:
    train = train.unionByName(f_)

params = train.groupBy("fit_key", "region_id").applyInPandas(fit_region, schema=PARAM_SCHEMA).toPandas()
display(params.sort_values(["fit_key", "region_id"]))
param_dict = {k: {r.region_id: r._asdict() for r in g.itertuples(index=False)} for k, g in params.groupby("fit_key")}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Planned customers
# MAGIC What FP&A knows on October 1: the September meter count, grown at its three-year trend.

# COMMAND ----------

def customer_plan(season: int) -> pd.DataFrame:
    known = cust[cust.month <= pd.Timestamp(f"{season}-09-01")]
    out = []
    for region, g in known.groupby("region_id"):
        g = g.sort_values("month")
        last = g.iloc[-1]
        prior = g[g.month == last.month - pd.DateOffset(years=3)]
        rate = (last.total / prior.total.iloc[0]) ** (1 / 3) - 1 if len(prior) else 0.0
        for m in pd.date_range(f"{season}-11-01", f"{season + 1}-03-01", freq="MS"):
            out.append({"region_id": region, "month": m, "growth_rate": rate,
                        "customers_planned": last.total * (1 + rate) ** ((m - last.month).days / 365.25)})
    return pd.DataFrame(out)


plan_customers = customer_plan(SEASON)
display(plan_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather scenarios: P10 / P50 / P90

# COMMAND ----------

def use_per_customer(p: dict, tmean, hol):
    return p["intercept"] + p["slope"] * np.maximum(0, p["balance_point"] - tmean) + p["holiday"] * hol


def scenarios(p: dict, region: str, season: int, years, customers: pd.Series) -> pd.DataFrame:
    """Run each historical winter's daily temperatures through the model for the target winter."""
    days = pd.date_range(f"{season}-11-01", f"{season + 1}-03-31")
    hol = holiday.reindex(days).fillna(0).to_numpy()
    n = customers.reindex(days.to_period("M").to_timestamp()).to_numpy()
    wx = weather[weather.region_id == region].set_index("gas_day").tmean
    out = []
    for y in years:
        if y not in usable[region]:
            continue
        src = days - pd.DateOffset(years=season - y)
        t = wx.reindex(src).interpolate().bfill().ffill().to_numpy()
        out.append(pd.DataFrame({"weather_year": y, "gas_day": days, "month": days.to_period("M").astype(str),
                                 "tmean": t, "dth": n * use_per_customer(p, t, hol)}))
    return pd.concat(out)


def quantiles(sc: pd.DataFrame) -> pd.DataFrame:
    monthly = sc.groupby(["weather_year", "month"]).dth.sum().reset_index()
    season = monthly.groupby("weather_year").dth.sum().reset_index().assign(month="Nov-Mar")
    both = pd.concat([monthly, season])
    q = both.groupby("month").dth.quantile([0.1, 0.5, 0.9]).unstack()
    q.columns = ["p10", "p50", "p90"]
    return q.reset_index().rename(columns={"month": "period"})


forecast, daily_plan = [], []
for region, p in param_dict[str(SEASON)].items():
    pc = plan_customers[plan_customers.region_id == region].set_index("month").customers_planned
    sc = scenarios(p, region, SEASON, SCENARIO_YEARS, pc)
    q = quantiles(sc)
    q["customers_planned"] = [pc.mean() if per == "Nov-Mar" else pc[pd.Timestamp(per + "-01")] for per in q.period]
    forecast.append(q.assign(region_id=region, scenario_winters=sc.weather_year.nunique()))
    # Daily plan: the average scenario day, scaled so each month adds up to its P50
    d = sc.groupby(["gas_day", "month"]).agg(plan_dth=("dth", "mean"), normal_tmean=("tmean", "mean")).reset_index()
    d["plan_dth"] *= d.month.map(q.set_index("period").p50) / d.groupby("month").plan_dth.transform("sum")
    d["customers_planned"] = pc.reindex(d.gas_day.dt.to_period("M").dt.to_timestamp()).to_numpy()
    daily_plan.append(d.assign(region_id=region))
forecast = pd.concat(forecast)
forecast["period_type"] = np.where(forecast.period == "Nov-Mar", "season", "month")
forecast[["p10", "p50", "p90", "customers_planned"]] = forecast[["p10", "p50", "p90", "customers_planned"]].round(0)
daily_plan = pd.concat(daily_plan)
display(forecast)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Backtest against the Excel-style baseline

# COMMAND ----------

monthly_actual = (spark.table(f"{FPA}.gold_daily_region").withColumn("month", F.date_format("gas_day", "yyyy-MM"))
                  .groupBy("region_id", "month")
                  .agg(F.sum("volume_dth").alias("dth"), F.sum("hdd65").alias("hdd65"), F.count("hdd65").alias("days"))
                  .toPandas())
wx_monthly = weather.assign(month=weather.gas_day.dt.to_period("M").astype(str),
                            hdd65=(65 - weather.tmean).clip(lower=0))
wx_monthly = wx_monthly.groupby(["region_id", "month"]).hdd65.sum().reset_index()


def excel_baseline(region: str, season: int) -> pd.DataFrame:
    """What the spreadsheet does: monthly Dth on monthly HDD65 over the last 5 years, refit once a year,
    then forecast with the 10-year average HDD for each calendar month. One number, no range."""
    hist = monthly_actual[(monthly_actual.region_id == region) & (monthly_actual.month < f"{season}-10")
                          & (monthly_actual.month >= f"{season - 5}-10") & (monthly_actual.days >= 28)]
    b, a = np.polyfit(hist.hdd65, hist.dth, 1)
    norm = wx_monthly[(wx_monthly.region_id == region) & (wx_monthly.month >= f"{season - 10}-10")
                      & (wx_monthly.month < f"{season}-10")].copy()
    norm["cal_month"] = norm.month.str[5:].astype(int)
    normal_hdd = norm.groupby("cal_month").hdd65.mean()
    months = pd.period_range(f"{season}-11", f"{season + 1}-03", freq="M").astype(str)
    actual_hdd = wx_monthly[wx_monthly.region_id == region].set_index("month").hdd65
    return pd.DataFrame({"month": months, "baseline": [a + b * normal_hdd[int(m[5:])] for m in months],
                         "baseline_actual_wx": [a + b * actual_hdd[m] for m in months]})


def model_actual_weather(p: dict, region: str, season: int) -> pd.DataFrame:
    """The model given the weather and customers that actually happened: isolates model error from
    weather uncertainty."""
    days = pd.date_range(f"{season}-11-01", f"{season + 1}-03-31")
    t = weather[weather.region_id == region].set_index("gas_day").tmean.reindex(days).interpolate().bfill().ffill()
    n = (cust[cust.region_id == region].set_index("month").total
         .reindex(days.to_period("M").to_timestamp()).ffill().to_numpy())
    v = n * use_per_customer(p, t.to_numpy(), holiday.reindex(days).fillna(0).to_numpy())
    return pd.DataFrame({"month": days.to_period("M").astype(str), "model_actual_wx": v}).groupby("month").sum().reset_index()


bt = []
for season in BACKTEST_SEASONS:
    pcs = customer_plan(season)
    for region, p in param_dict[str(season)].items():
        pc = pcs[pcs.region_id == region].set_index("month").customers_planned
        q = quantiles(scenarios(p, region, season, range(1995, season), pc)).rename(columns={"period": "month"})
        q = q[q.month != "Nov-Mar"]
        base = excel_baseline(region, season)
        act = monthly_actual[monthly_actual.region_id == region][["month", "dth"]].rename(columns={"dth": "actual"})
        bt.append(q.merge(base, on="month").merge(model_actual_weather(p, region, season), on="month")
                  .merge(act, on="month").assign(region_id=region, season=f"{season}-{str(season + 1)[2:]}"))
bt = pd.concat(bt)
bt["ape_model"] = (bt.p50 - bt.actual).abs() / bt.actual
bt["ape_baseline"] = (bt.baseline - bt.actual).abs() / bt.actual
bt["ape_baseline_actual_wx"] = (bt.baseline_actual_wx - bt.actual).abs() / bt.actual
bt["ape_model_actual_wx"] = (bt.model_actual_wx - bt.actual).abs() / bt.actual
bt["in_p10_p90"] = (bt.actual >= bt.p10) & (bt.actual <= bt.p90)

metrics = {"mape_baseline": "ape_baseline", "mape_model": "ape_model",
           "mape_baseline_actual_weather": "ape_baseline_actual_wx", "mape_model_actual_weather": "ape_model_actual_wx",
           "p10_p90_coverage": "in_p10_p90"}
backtest = (bt.groupby(["season", "region_id"])
            .agg(**{k: (v, "mean") for k, v in metrics.items()}, months=("month", "count"))
            .reset_index())
backtest[list(metrics)] = backtest[list(metrics)].round(4)
display(backtest)
overall = {k: round(float(bt[v].mean()), 4) for k, v in metrics.items()}
print(overall)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log to MLflow and register in Unity Catalog

# COMMAND ----------

class VolumeForecaster(mlflow.pyfunc.PythonModel):
    """Per-region change-point HDD model of use per customer. Input: region_id, tmean (F), holiday_flag,
    customers. Output: daily Dth = customers x use per customer."""

    def __init__(self, params: dict):
        self.params = params

    def predict(self, context, model_input: pd.DataFrame, params=None):
        out = np.full(len(model_input), np.nan)
        for region, p in self.params.items():
            m = (model_input.region_id == region).to_numpy()
            use = (p["intercept"] + p["slope"] * np.maximum(0, p["balance_point"] - model_input.tmean.to_numpy()[m])
                   + p["holiday"] * model_input.holiday_flag.to_numpy()[m])
            out[m] = model_input.customers.to_numpy()[m] * use
        return pd.Series(out, name="volume_dth")


champion = {r: {k: float(v[k]) for k in ("balance_point", "intercept", "slope", "holiday")}
            for r, v in param_dict[str(SEASON)].items()}
example = pd.DataFrame({"region_id": ["summit", "pueblo"], "tmean": [20.0, 35.0], "holiday_flag": [1, 0],
                        "customers": [3600.0, 8500.0]})

with mlflow.start_run(run_name=f"volume_forecaster_{FORECAST_DATE}") as run:
    mlflow.log_params({"forecast_date": FORECAST_DATE, "season": SEASON_LABEL, "training_window": f"{TRAIN_YEARS} gas years",
                       "target": "use per customer per day", "scenario_winters": f"{min(SCENARIO_YEARS)}-{max(SCENARIO_YEARS) + 1}",
                       "regions": len(champion)})
    for r, v in param_dict[str(SEASON)].items():
        mlflow.log_metrics({f"{r}.r2": v["r2"], f"{r}.mape_daily": v["mape_daily"], f"{r}.balance_point": v["balance_point"]})
    mlflow.log_metrics({f"backtest.{k}": v for k, v in overall.items()})
    mlflow.log_dict(champion, "region_params.json")
    info = mlflow.pyfunc.log_model(
        name="model", python_model=VolumeForecaster(champion), input_example=example,
        registered_model_name=MODEL_NAME,
    )
    run_id = run.info.run_id

version = info.registered_model_version
client.set_registered_model_alias(MODEL_NAME, "champion", version)
client.set_model_version_tag(MODEL_NAME, version, "forecast_date", FORECAST_DATE)
client.set_model_version_tag(MODEL_NAME, version, "season", SEASON_LABEL)
client.update_registered_model(MODEL_NAME, description=(
    "Colorado Gas Utility (fictional) daily gas volume per region: change-point HDD regression of use per customer "
    "with an occupancy term. Input region_id, tmean (F), holiday_flag, customers; output Dth."))
print(f"registered {MODEL_NAME} v{version} as champion, run {run_id}")

# COMMAND ----------

fc = spark.createDataFrame(forecast.assign(season=SEASON_LABEL, forecast_date=FORECAST_DATE,
                                           model_version=str(version), run_id=run_id))
(fc.selectExpr("season", "region_id", "period_type", "period", "p10", "p50", "p90", "customers_planned",
               "CAST(scenario_winters AS INT) scenario_winters", "CAST(forecast_date AS DATE) forecast_date",
               "model_version", "run_id")
   .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FPA}.forecast_season"))
spark.sql(f"COMMENT ON TABLE {FPA}.forecast_season IS 'Winter {SEASON_LABEL} volume plan (Dth) by region and month plus the Nov-Mar "
          f"total, made {FORECAST_DATE}. P10 = warm winter, P50 = expected, P90 = cold winter, from historical winters run "
          "through the champion model with the planned customer counts.'")

dp_out = daily_plan.assign(season=SEASON_LABEL, model_version=str(version))
dp_out["gas_day"] = dp_out.gas_day.dt.date
dp_out[["plan_dth", "normal_tmean", "customers_planned"]] = dp_out[["plan_dth", "normal_tmean", "customers_planned"]].round(2)
(spark.createDataFrame(dp_out[["season", "region_id", "gas_day", "plan_dth", "normal_tmean", "customers_planned", "model_version"]])
   .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FPA}.forecast_daily"))
spark.sql(f"COMMENT ON TABLE {FPA}.forecast_daily IS 'The plan by day: the average scenario day per region, scaled so each month "
          "adds up to its P50, with the normal (scenario-average) mean temperature and planned customers.'")

(spark.createDataFrame(params.assign(model_version=str(version)))
   .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FPA}.model_params"))

(spark.createDataFrame(backtest.assign(model_version=str(version)))
   .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FPA}.backtest_results"))
spark.sql(f"COMMENT ON TABLE {FPA}.backtest_results IS 'Per held-out winter and region: monthly MAPE of the Excel-style baseline "
          "and the new model, both forecasting with normal weather (mape_baseline, mape_model) and given the actual weather "
          "(*_actual_weather), plus the share of months where the actual fell inside P10-P90.'")

season_tot = forecast[forecast.period_type == "season"][["region_id", "p10", "p50", "p90", "customers_planned", "scenario_winters"]]
dbutils.notebook.exit(json.dumps({
    "skipped": False,
    "model": {"name": "cgu_fpa.volume_forecaster", "version": str(version), "alias": "champion"},
    "region_params": {r: {k: round(v, 5) for k, v in p.items()} for r, p in champion.items()},
    "season_total_dth": season_tot.round(0).to_dict("records"),
    "backtest_overall": overall,
}, default=str))
