# Databricks notebook source
# MAGIC %md
# MAGIC # 06 In-season re-forecast and design day
# MAGIC
# MAGIC **Re-forecast.** For each region, as of the run's as-of date, the season is rebuilt from three pieces:
# MAGIC
# MAGIC | Piece | Days | Weather | Customers |
# MAGIC |---|---|---|---|
# MAGIC | Actual to date | November 1 to the as-of date | Real NOAA | Actual volume from the source |
# MAGIC | Next 7 days | The days the NWS forecast covers | Real NWS forecast | Current meter count |
# MAGIC | Rest of season | Every other day to March 31 | Each historical winter | Current meter count |
# MAGIC
# MAGIC The days between the as-of date and the first forecast day (stations not yet reported) are part of
# MAGIC the rest of season. Real days replace forecast days every run, so the P10 to P90 range narrows.
# MAGIC
# MAGIC **Variance to date, three parts** (they add up to actual minus plan):
# MAGIC
# MAGIC | Part | Meaning |
# MAGIC |---|---|
# MAGIC | `weather` | The model on the actual weather with the planned customers, minus the plan |
# MAGIC | `customers` | The model with the actual customers, minus the same with the planned customers |
# MAGIC | `other` | Actual minus the model with actual weather and customers (occupancy, behavior) |
# MAGIC
# MAGIC **Design day.** A generalized extreme value (GEV) fit of each station's annual coldest day (maximum
# MAGIC daily HDD65) gives the 1-in-30 and 1-in-50 design HDD. Peak-day Dth = today's customers x the model's
# MAGIC use per customer at that temperature, with a 90% forecast-error band, compared with firm transportation.

# COMMAND ----------

# MAGIC %pip install -q "mlflow>=3.1" scipy
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("season", "2026")
CATALOG = dbutils.widgets.get("catalog")
SEASON = int(dbutils.widgets.get("season"))
assert CATALOG
FPA = f"`{CATALOG}`.cgu_fpa"
CORE = f"`{CATALOG}`.cgu_core"
MODEL_NAME = f"{CATALOG}.cgu_fpa.volume_forecaster"
SEASON_LABEL = f"{SEASON}-{str(SEASON + 1)[2:]}"
SEASON_START, SEASON_END = f"{SEASON}-11-01", f"{SEASON + 1}-03-31"
MIN_WINTER_DAYS = 145
EARLY_SIGNAL_DAYS = 21

# COMMAND ----------

import json

import mlflow
import numpy as np
import pandas as pd
from scipy.stats import genextreme

ctx = spark.sql(f"SELECT * FROM {CORE}.run_context ORDER BY recorded_at DESC LIMIT 1").first()
BATCH, AS_OF, EXTRACT_DATE = ctx.batch, pd.Timestamp(ctx.as_of_date), pd.Timestamp(ctx.extract_date)
print(f"batch {BATCH}, as of {AS_OF.date()}, extracted {EXTRACT_DATE.date()}")

mlflow.set_registry_uri("databricks-uc")
model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion")
version = mlflow.MlflowClient().get_model_version_by_alias(MODEL_NAME, "champion").version
params = (spark.table(f"{FPA}.model_params").where(f"fit_key = '{SEASON}' AND model_version = '{version}'")
          .toPandas().set_index("region_id"))
REGIONS = list(params.index)


def predict(region, tmean, hol, customers):
    n = len(tmean)
    if n == 0:
        return np.array([])
    return model.predict(pd.DataFrame({"region_id": [region] * n, "tmean": np.asarray(tmean, float),
                                       "holiday_flag": np.asarray(hol).astype(int),
                                       "customers": np.asarray(customers, float)})).to_numpy()


calendar = spark.table(f"{FPA}.ref_holiday_calendar").toPandas()
holiday = calendar.assign(gas_day=pd.to_datetime(calendar.gas_day)).set_index("gas_day").holiday_flag
weather = (spark.table(f"{CORE}.silver_weather_daily").where("region_id IS NOT NULL")
           .select("region_id", "gas_day", "tmean", "hdd65").toPandas())
weather["gas_day"] = pd.to_datetime(weather.gas_day)
names = spark.table(f"{CORE}.silver_region").select("region_id", "name").toPandas().set_index("region_id").name
cust = spark.sql(f"SELECT region_id, month, residential + commercial AS customers FROM {CORE}.silver_customer_count").toPandas()
cust["month"] = pd.to_datetime(cust.month)
# Today's meters: the latest monthly count in the extract (counts are known on the 1st, ahead of the weather data)
current_customers = (cust[cust.month <= EXTRACT_DATE].sort_values("month").groupby("region_id").last().customers)

plan = spark.table(f"{FPA}.forecast_daily").toPandas()
plan["gas_day"] = pd.to_datetime(plan.gas_day)
published = spark.table(f"{FPA}.forecast_season").where("period_type = 'season'").toPandas().set_index("region_id")

winter = weather[weather.gas_day.dt.month.isin([11, 12, 1, 2, 3])]
winter = winter.assign(season=winter.gas_day.dt.year.where(winter.gas_day.dt.month >= 11, winter.gas_day.dt.year - 1))
winter_days = winter.groupby(["region_id", "season"]).size()
usable = {r: sorted(int(y) for (rr, y), n in winter_days.items() if rr == r and n >= MIN_WINTER_DAYS and y < SEASON)
          for r in REGIONS}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Actual to date

# COMMAND ----------

actual = spark.sql(f"""
  SELECT region_id, gas_day, volume_dth AS actual, customers, tmean, hdd65, holiday_flag, read_status
  FROM {FPA}.gold_daily_region
  WHERE gas_day BETWEEN '{SEASON_START}' AND '{AS_OF.date()}' AND tmean IS NOT NULL""").toPandas()
actual["gas_day"] = pd.to_datetime(actual.gas_day)
actual = actual.merge(plan[["region_id", "gas_day", "plan_dth", "customers_planned"]], on=["region_id", "gas_day"])
parts = []
for region, d in actual.groupby("region_id"):
    d = d.copy()
    d["expected_planned_customers"] = predict(region, d.tmean, d.holiday_flag, d.customers_planned)
    d["expected"] = predict(region, d.tmean, d.holiday_flag, d.customers)
    parts.append(d)
actual = pd.concat(parts) if parts else actual.assign(expected_planned_customers=[], expected=[])
actual = actual.rename(columns={"plan_dth": "plan"})
actual["weather_part"] = actual.expected_planned_customers - actual.plan
actual["customers_part"] = actual.expected - actual.expected_planned_customers
actual["other_part"] = actual.actual - actual.expected
actual["segment"] = "actual"
print(f"{actual.gas_day.nunique()} days of actuals")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next 7 days (NWS) and the rest of the season (historical winters)

# COMMAND ----------

nws = spark.sql(f"""
  SELECT region_id, forecast_date AS gas_day, tmean, hdd65, issued_at
  FROM {CORE}.weather_forecast
  WHERE fetched_at = (SELECT MAX(fetched_at) FROM {CORE}.weather_forecast)""").toPandas()
nws["gas_day"] = pd.to_datetime(nws.gas_day)
nws_issued = str(nws.issued_at.max()) if len(nws) else None
nws = nws[(nws.gas_day > AS_OF) & (nws.gas_day >= SEASON_START) & (nws.gas_day <= SEASON_END)]

# Normal HDD by calendar day: 30-winter average, smoothed over a week
clim = weather[(weather.gas_day.dt.year >= SEASON - 30) & (weather.gas_day < f"{SEASON}-10-01")].copy()
clim["md"] = clim.gas_day.dt.strftime("%m-%d")
normal_hdd = clim.groupby(["region_id", "md"]).hdd65.mean()
normal_hdd = normal_hdd.groupby(level=0, group_keys=False).apply(
    lambda s: pd.concat([s.iloc[-3:], s, s.iloc[:3]]).rolling(7, center=True).mean().iloc[3:-3])

season_days = pd.date_range(SEASON_START, SEASON_END)
rest_rows, nws_rows, totals, reforecast = [], [], [], []
for region in REGIONS:
    n_now = float(current_customers[region])
    wx = weather[weather.region_id == region].set_index("gas_day").tmean
    f = nws[nws.region_id == region].copy()
    f["forecast_dth"] = predict(region, f.tmean, holiday.reindex(f.gas_day).fillna(0), [n_now] * len(f))
    f["normal_hdd65"] = [normal_hdd.get((region, d.strftime("%m-%d")), np.nan) for d in f.gas_day]
    f["segment"] = "nws_forecast"
    nws_rows.append(f.assign(region_id=region))

    rest = season_days[(season_days > AS_OF) & ~season_days.isin(f.gas_day)]
    hol = holiday.reindex(rest).fillna(0).to_numpy()
    sims = []
    for y in usable[region]:
        t = wx.reindex(rest - pd.DateOffset(years=SEASON - y)).interpolate().bfill().ffill().to_numpy()
        sims.append(predict(region, t, hol, [n_now] * len(rest)))
    sims = np.array(sims) if len(rest) else np.zeros((len(usable[region]), 0))
    awaiting = (rest < f.gas_day.min()) if len(f) else np.zeros(len(rest), bool)  # stations not yet reported
    rest_rows.append(pd.DataFrame({
        "region_id": region, "gas_day": rest, "forecast_dth": sims.mean(axis=0) if len(rest) else [],
        "segment": np.where(awaiting, "awaiting_observations", "scenario")}))

    a = actual[actual.region_id == region]
    actual_to_date = float(a.actual.sum())
    season_sims = actual_to_date + float(f.forecast_dth.sum()) + sims.sum(axis=1)
    p10, p50, p90 = np.quantile(season_sims, [0.1, 0.5, 0.9])
    pub = published.loc[region]
    reforecast.append({
        "region_id": region, "region_name": names[region], "batch": BATCH, "as_of_date": AS_OF.date(),
        "nws_issued_at": nws_issued, "days_actual": int(len(a)), "actual_to_date": actual_to_date,
        "awaiting_days": int(awaiting.sum()), "nws_days": int(len(f)), "nws_dth": float(f.forecast_dth.sum()),
        "remainder_days": int(len(rest)), "scenario_winters": len(usable[region]),
        "reforecast_p10": p10, "reforecast_p50": p50, "reforecast_p90": p90,
        "published_p10": float(pub.p10), "published_p50": float(pub.p50), "published_p90": float(pub.p90),
        "customers_planned": float(a.customers_planned.mean()) if len(a) else float(plan[plan.region_id == region].customers_planned.iloc[0]),
        "customers_current": n_now,
        "plan_to_date": float(a.plan.sum()), "expected_to_date": float(a.expected.sum()),
        "weather_variance": float(a.weather_part.sum()), "customer_variance": float(a.customers_part.sum()),
        "other_variance": float(a.other_part.sum()), "total_variance": float((a.actual - a.plan).sum()),
        "estimated_days": int((a.read_status == "ESTIMATED").sum()),
        "next7_hdd": float(f.hdd65.sum()), "next7_normal_hdd": float(f.normal_hdd65.sum()),
        "next7_dth": float(f.forecast_dth.sum()),
        "next7_plan_dth": float(plan[(plan.region_id == region) & plan.gas_day.isin(f.gas_day)].plan_dth.sum()),
    })

# COMMAND ----------

def status(pct):
    a = abs(pct)
    return "on plan" if a < 3 else ("watch" if a < 7 else "off plan")


summary = pd.DataFrame(reforecast)
summary["reforecast_vs_plan"] = summary.reforecast_p50 - summary.published_p50
summary["reforecast_vs_plan_pct"] = (100 * summary.reforecast_vs_plan / summary.published_p50).round(1)
for c in ("weather_variance", "customer_variance", "other_variance", "total_variance"):
    summary[f"{c}_pct"] = (100 * summary[c] / summary.plan_to_date.where(summary.plan_to_date > 0)).round(1)
summary["status"] = summary.reforecast_vs_plan_pct.map(status)
summary["signal"] = np.where(summary.days_actual < EARLY_SIGNAL_DAYS, "early signal", "established")
summary["model_version"] = str(version)
summary["refreshed_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("s")
num = [c for c in summary.columns if summary[c].dtype == float and not c.endswith("_pct")]
summary[num] = summary[num].round(0)
summary[["next7_hdd", "next7_normal_hdd"]] = summary[["next7_hdd", "next7_normal_hdd"]].round(1)
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily table: plan, actual, and outlook through March 31

# COMMAND ----------

cols = ["region_id", "gas_day", "segment", "plan", "actual", "expected", "expected_planned_customers", "forecast_dth",
        "weather_part", "customers_part", "other_part", "customers", "tmean", "hdd65", "normal_hdd65", "holiday_flag",
        "read_status"]
future = pd.concat(nws_rows + rest_rows, ignore_index=True)
future = future.merge(plan[["region_id", "gas_day", "plan_dth"]].rename(columns={"plan_dth": "plan"}), on=["region_id", "gas_day"])
future["customers"] = future.region_id.map(current_customers).astype(float)
future["holiday_flag"] = holiday.reindex(future.gas_day).fillna(0).astype(int).to_numpy()
daily = pd.concat([actual, future], ignore_index=True).reindex(columns=cols).sort_values(["region_id", "gas_day"])
daily["region_name"] = daily.region_id.map(names)
daily["outlook"] = daily.actual.where(daily.segment == "actual", daily.forecast_dth)
for c in ("plan", "actual", "expected", "outlook"):
    daily[f"cum_{c}"] = daily.groupby("region_id")[c].cumsum()
daily.loc[daily.segment != "actual", ["cum_actual", "cum_expected"]] = np.nan
daily[daily.select_dtypes("float").columns] = daily.select_dtypes("float").round(1)
daily["gas_day"] = daily.gas_day.dt.date
daily["read_status"] = daily.read_status.astype(object).where(daily.read_status.notna(), None)
daily["as_of_date"] = AS_OF.date()

spark.createDataFrame(daily).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FPA}.season_tracker")
spark.createDataFrame(summary).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FPA}.season_tracker_summary")
spark.sql(f"COMMENT ON TABLE {FPA}.season_tracker IS 'Winter {SEASON_LABEL} by region and day: plan, actual, and the model to date; "
          "then the outlook (NWS forecast days, days awaiting observations, scenario days) to March 31. segment says which.'")
spark.sql(f"COMMENT ON TABLE {FPA}.season_tracker_summary IS 'Per region as of the as-of date: variance to date in three parts "
          "(weather, customers, other; they add up to actual minus plan), the re-forecast season P10/P50/P90 against the "
          "published plan, and the next 7 days. Status on the re-forecast P50: on plan (<3%), watch (3-7%), off plan (>=7%).'")

# One row per region and run, so the range can be compared run to run
hist = spark.createDataFrame(summary.assign(recorded_at=summary.refreshed_at))
if spark.catalog.tableExists(f"{CATALOG}.cgu_fpa.season_reforecast"):
    spark.sql(f"DELETE FROM {FPA}.season_reforecast WHERE batch = '{BATCH}'")
hist.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{FPA}.season_reforecast")
spark.sql(f"COMMENT ON TABLE {FPA}.season_reforecast IS 'Re-forecast history: one row per region and run (batch), with the "
          "pieces (actual to date, NWS days, remainder) and the season P10/P50/P90 against the published plan.'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design day and firm transportation

# COMMAND ----------

capacity = spark.table(f"{FPA}.firm_capacity").toPandas().set_index("region_id")
stations = spark.table(f"{FPA}.ref_weather_station").toPandas().set_index("region_id")
design = []
for region in REGIONS:
    wx = weather[weather.region_id == region]
    hy = wx.gas_day.dt.year.where(wx.gas_day.dt.month >= 7, wx.gas_day.dt.year - 1)
    annual_max = wx.groupby(hy).hdd65.max()
    annual_max = annual_max[annual_max.index.isin(usable[region])]
    shape, loc, scale = genextreme.fit(annual_max.to_numpy())
    h30, h50 = (float(genextreme.ppf(1 - 1 / T, shape, loc, scale)) for T in (30, 50))
    p = params.loc[region]
    n = float(current_customers[region])

    def peak(h):
        x0 = max(0.0, p.balance_point - (65 - h))
        se = p.rmse * np.sqrt(1 + 1 / p.n_days + (x0 - p.x_mean) ** 2 / p.x_sxx)
        return n * (p.intercept + p.slope * x0), n * 1.645 * se

    pk30, band30 = peak(h30)
    pk50, _ = peak(h50)
    cap = float(capacity.loc[region].contracted_dth_per_day)
    design.append({
        "region_id": region, "region_name": names[region], "station": stations.loc[region].station,
        "station_name": stations.loc[region].station_name, "winters_used": int(len(annual_max)),
        "max_hdd_observed": float(annual_max.max()), "design_hdd_1in30": h30, "design_hdd_1in50": h50,
        "design_tmean_1in30": 65 - h30, "customers": n, "peak_dth_1in30": pk30,
        "peak_dth_1in30_low": pk30 - band30, "peak_dth_1in30_high": pk30 + band30, "peak_dth_1in50": pk50,
        "contracted_dth_per_day": cap, "pipeline": capacity.loc[region].pipeline,
        "headroom_dth": cap - pk30, "headroom_pct": 100 * (cap - pk30) / pk30,
        "gap_to_1in50_dth": max(0.0, pk50 - cap), "status": "short" if pk30 > cap else "covered",
        "as_of_date": AS_OF.date(), "model_version": str(version),
    })
design = pd.DataFrame(design)
design[[c for c in design.columns if design[c].dtype == float]] = design[[c for c in design.columns if design[c].dtype == float]].round(1)
display(design)
spark.createDataFrame(design).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FPA}.design_day")
spark.sql(f"COMMENT ON TABLE {FPA}.design_day IS 'Peak-day requirement per region at the 1-in-30 design day (planning standard) and "
          "1-in-50, from a GEV fit of annual maximum daily HDD65, with today''s customers and a 90% forecast-error band, "
          "against the (fictional) contracted firm transportation in cgu_fpa.firm_capacity.'")

dbutils.notebook.exit(json.dumps({
    "as_of_date": str(AS_OF.date()), "model_version": str(version), "nws_issued_at": nws_issued,
    "summary": summary[["region_id", "days_actual", "plan_to_date", "actual_to_date", "weather_variance_pct",
                        "customer_variance_pct", "other_variance_pct", "total_variance_pct", "published_p50",
                        "reforecast_p10", "reforecast_p50", "reforecast_p90", "reforecast_vs_plan_pct", "status", "signal",
                        "nws_days", "next7_hdd", "next7_normal_hdd"]].to_dict("records"),
    "design_day": design[["region_id", "design_hdd_1in30", "design_hdd_1in50", "peak_dth_1in30", "contracted_dth_per_day",
                          "headroom_pct", "status"]].to_dict("records"),
}, default=str))
