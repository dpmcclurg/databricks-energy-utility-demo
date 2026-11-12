# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Statewide backtest on real data
# MAGIC
# MAGIC The regional volumes in this demo are synthetic, so a backtest on them proves the code, not the
# MAGIC method. This notebook runs the same method on **real** data: EIA's monthly Colorado residential plus
# MAGIC commercial gas volumes against a customer-weighted HDD built from real NOAA stations.
# MAGIC
# MAGIC - Target: volume per customer per day (MMcf per day / meters). Meters come from EIA's annual consumer
# MAGIC   counts, interpolated by month and extended at the three-year trend past the latest published year.
# MAGIC - Weather: the daily mean temperature weighted by `state_hdd_weight` in `cgu_fpa.ref_weather_station`
# MAGIC   (Denver 0.65, Colorado Springs 0.20, Dillon 0.10, Pueblo 0.05, an assumption about where Colorado's
# MAGIC   gas customers live), then the monthly average of max(0, balance_point - tmean).
# MAGIC - For each held-out winter: train on the five gas years before it, grid-search the balance point,
# MAGIC   then predict November to March **given the actual weather**. The error is the method's own error.
# MAGIC
# MAGIC Units: MMcf (million cubic feet), as EIA publishes; 1 MMcf is about 1,037 Dth.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("season", "2026")
CATALOG = dbutils.widgets.get("catalog")
SEASON = int(dbutils.widgets.get("season"))
assert CATALOG
FPA = f"`{CATALOG}`.cgu_fpa"
TEST_SEASONS = [SEASON - 3, SEASON - 2, SEASON - 1]

import json

import numpy as np
import pandas as pd

g = spark.table(f"{FPA}.gold_state_monthly").toPandas()
g["month"] = pd.to_datetime(g.month)
g = g.sort_values("month").dropna(subset=["total_mmcf"])

# Meters by month: EIA counts are annual (mid-year); interpolate, then extend at the three-year trend
cons = g.dropna(subset=["residential_consumers"]).groupby(g.month.dt.year)[["residential_consumers", "commercial_consumers"]].first()
cons = cons[cons.index >= 2000].sum(axis=1)
trend = (cons.iloc[-1] / cons.iloc[-4]) ** (1 / 3)
t = g.month.dt.year + (g.month.dt.month - 6) / 12
g["meters"] = np.where(t <= cons.index.max(), np.interp(t, cons.index, cons.values),
                       cons.iloc[-1] * trend ** (t - cons.index.max()))
g["use_per_meter"] = g.mmcf_per_day * 1e6 / g.meters / 1000  # Mcf per meter per day

wx = spark.sql(f"""
  SELECT w.gas_day, SUM(w.tmean * s.state_hdd_weight) / SUM(s.state_hdd_weight) AS tmean
  FROM `{CATALOG}`.cgu_core.silver_weather_daily w JOIN {FPA}.ref_weather_station s USING (station)
  WHERE s.state_hdd_weight > 0 GROUP BY w.gas_day""").toPandas()
wx["gas_day"] = pd.to_datetime(wx.gas_day)
wx = wx.set_index("gas_day").tmean.sort_index()

# COMMAND ----------

def hdd_per_day(bp: float) -> pd.Series:
    return np.maximum(0, bp - wx).groupby(wx.index.to_period("M").to_timestamp()).mean()


rows, fits = [], []
for s in TEST_SEASONS:
    train = g[(g.month >= f"{s - 5}-10-01") & (g.month < f"{s}-10-01")]
    test = g[(g.month >= f"{s}-11-01") & (g.month < f"{s + 1}-04-01")]
    best = None
    for bp in range(50, 67):
        x = hdd_per_day(bp).reindex(train.month).to_numpy()
        b, a = np.polyfit(x, train.use_per_meter, 1)
        sse = float(((a + b * x - train.use_per_meter) ** 2).sum())
        if best is None or sse < best[0]:
            r2 = 1 - sse / float(((train.use_per_meter - train.use_per_meter.mean()) ** 2).sum())
            best = (sse, bp, a, b, r2)
    _, bp, a, b, r2 = best
    x = hdd_per_day(bp).reindex(test.month).to_numpy()
    pred = (a + b * x) * test.meters.to_numpy() * 1000 / 1e6 * test.days_in_month.to_numpy()
    fits.append({"season": f"{s}-{str(s + 1)[2:]}", "balance_point": bp, "base_mcf_per_meter_day": round(a, 4),
                 "slope_mcf_per_meter_hdd": round(b, 5), "r2_train": round(r2, 3), "train_months": len(train)})
    rows.append(test.assign(season=f"{s}-{str(s + 1)[2:]}", predicted_mmcf=pred, balance_point=bp, r2_train=r2))

bt = pd.concat(rows)
bt["ape"] = (bt.predicted_mmcf - bt.total_mmcf).abs() / bt.total_mmcf
out = bt[["season", "month", "total_mmcf", "predicted_mmcf", "ape", "hdd65_weighted", "meters", "balance_point", "r2_train"]].copy()
out = out.rename(columns={"total_mmcf": "actual_mmcf"})
out["month"] = out.month.dt.date
out[["actual_mmcf", "predicted_mmcf", "meters"]] = out[["actual_mmcf", "predicted_mmcf", "meters"]].round(0)
out[["ape", "r2_train"]] = out[["ape", "r2_train"]].round(4)
display(out)

spark.createDataFrame(out).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FPA}.state_backtest")
spark.sql(f"COMMENT ON TABLE {FPA}.state_backtest IS 'Real-data check of the method: EIA Colorado residential plus commercial "
          "gas volume (MMcf) by winter month against the per-customer HDD model trained on the five gas years before each "
          "winter, given the actual statewide customer-weighted weather. ape = absolute percentage error.'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Winter totals: why last winter is a poor anchor

# COMMAND ----------

w = g[g.month.dt.month.isin([11, 12, 1, 2, 3])].copy()
w["season"] = w.month.dt.year.where(w.month.dt.month >= 11, w.month.dt.year - 1)
complete = w.groupby("season").size() == 5
totals = (w.groupby("season").agg(bcf=("total_mmcf", lambda v: round(v.sum() / 1000, 1)), hdd65=("hdd65_weighted", "sum"))
          [complete].tail(8).reset_index())
display(totals)

summary = {f["season"]: dict(f, mape=round(float(bt[bt.season == f["season"]].ape.mean()), 4)) for f in fits}
dbutils.notebook.exit(json.dumps({
    "by_season": summary, "mape_overall": round(float(bt.ape.mean()), 4),
    "winter_bcf": {f"{int(r.season)}-{str(int(r.season) + 1)[2:]}": r.bcf for r in totals.itertuples()},
}, default=str))
