# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Simulate the source system (gas measurement system on MySQL)
# MAGIC
# MAGIC Stands in for the utility's MySQL gas measurement system. It writes what a CDC connector reads
# MAGIC from the MySQL binlog: Debezium-style change events (`op`, `before`, `after`, `source`), one
# MAGIC folder per table, plus a control-totals file per extract, the way a real extract reports
# MAGIC record counts. Three source tables: `region`, `customer_count` (monthly meters), `daily_volume`.
# MAGIC
# MAGIC - `batch_000`: initial snapshot, all history from October 2016 through the run's as-of date.
# MAGIC - `batch_001` onward (one per job run, daily when scheduled): the days since the previous extract, the
# MAGIC   previous extract's estimated reads turned into actual reads, and new monthly meter counts.
# MAGIC   `batch_001` also deletes a row the snapshot sent twice.
# MAGIC
# MAGIC Volumes are synthetic and driven by the **real** NOAA temperatures:
# MAGIC `customers x (base + slope x max(0, balance_point - Tmean)) x occupancy x behavior + noise`, where
# MAGIC behavior is a small monthly swing per region (sd 2%) that weather does not explain. Base and slope per
# MAGIC customer are calibrated to EIA Colorado residential and commercial use per customer (about 0.077 Dth/day
# MAGIC base and 0.016 Dth/day per degree below a 58F balance point statewide, 2018-23), adjusted per region
# MAGIC for housing and second-home use. Defects are injected with a fixed seed so caught counts can be checked
# MAGIC against injected counts. In production this notebook is replaced by Lakeflow Connect reading the
# MAGIC MySQL binlog.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("season", "2026")
CATALOG = dbutils.widgets.get("catalog")
SEASON = int(dbutils.widgets.get("season"))
assert CATALOG, "catalog is required"
CDC = f"/Volumes/{CATALOG}/cgu_landing/cdc"
NOAA = f"/Volumes/{CATALOG}/cgu_landing/noaa"

# COMMAND ----------

import json
import os

import numpy as np
import pandas as pd

ctx = spark.sql(f"SELECT * FROM `{CATALOG}`.cgu_core.run_context ORDER BY recorded_at DESC LIMIT 1").first()
BATCH, N = ctx.batch, int(ctx.batch)
AS_OF, EXTRACT_DATE = pd.Timestamp(ctx.as_of_date), pd.Timestamp(ctx.extract_date)
HISTORY_START = pd.Timestamp(f"{SEASON - 10}-10-01")
HORIZON_END = pd.Timestamp(f"{SEASON + 1}-03-31")    # fixed grid, so ids and random draws match across runs
FORECAST_DATE = pd.Timestamp(f"{SEASON}-10-01")
SEASON_START = pd.Timestamp(f"{SEASON}-11-01")
if N == 0:
    SNAPSHOT_END, PREV_END, PREV_EXTRACT, PREV_ESTIMATED = AS_OF, None, None, []
else:
    first_control = json.load(open(f"{CDC}/_control/batch_000.json"))
    prev_control = json.load(open(f"{CDC}/_control/batch_{N - 1:03d}.json"))
    SNAPSHOT_END = pd.Timestamp(first_control["as_of_date"])
    PREV_END, PREV_EXTRACT = pd.Timestamp(prev_control["as_of_date"]), pd.Timestamp(prev_control["extract_date"])
    PREV_ESTIMATED = prev_control["estimated_volume_ids"]
    assert AS_OF >= PREV_END, f"as-of {AS_OF.date()} is before the previous extract's {PREV_END.date()}"
print(f"batch {BATCH}: " + (f"snapshot through {AS_OF.date()}" if N == 0 else
      f"days {PREV_END.date()} to {AS_OF.date()} (new: {(AS_OF - PREV_END).days})") + f", extracted {EXTRACT_DATE.date()}")

# Per-region behavior, per customer per day. Summit is second-home heavy (holiday occupancy); Teller has
# older, leakier housing (steeper slope); the Front Range regions track the EIA statewide numbers.
REGIONS = {
    "summit":         dict(station="USC00052281", name="Summit (Blue River)",    customers=3200, base=0.055, slope=0.0135, bp=60, occ_holiday=1.25),
    "teller":         dict(station="USC00054742", name="Teller (Cripple Creek)", customers=4800, base=0.070, slope=0.0190, bp=60, occ_holiday=1.00),
    "park_foothills": dict(station="USC00053530", name="Park and Foothills",     customers=6400, base=0.065, slope=0.0170, bp=59, occ_holiday=1.04),
    "pueblo":         dict(station="USW00093058", name="Pueblo",                 customers=7500, base=0.075, slope=0.0160, bp=58, occ_holiday=1.00),
    "eastern_plains": dict(station="USW00003017", name="Eastern Plains",         customers=4800, base=0.077, slope=0.0164, bp=58, occ_holiday=1.00),
}
ANNUAL_GROWTH = 0.012
RESIDENTIAL_SHARE = 0.92
holidays = (spark.table(f"`{CATALOG}`.cgu_fpa.ref_holiday_calendar").toPandas()
            .assign(gas_day=lambda d: pd.to_datetime(d.gas_day)).set_index("gas_day").holiday_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers (monthly meter counts)
# MAGIC Steady growth of 1.2% a year. The Eastern Plains connects a new subdivision for the first winter
# MAGIC (+4% of meters on the November 1 count), after the October 1 plan was published.

# COMMAND ----------

months = pd.date_range(HISTORY_START, HORIZON_END, freq="MS")
cust_rows = []
for region, p in REGIONS.items():
    n = p["customers"] * (1 + ANNUAL_GROWTH) ** ((months - HISTORY_START).days / 365.25)
    if region == "eastern_plains":
        n = np.where(months >= SEASON_START, n * 1.04, n)
    for m, total in zip(months, n):
        res = int(round(total * RESIDENTIAL_SHARE))
        cust_rows.append({"region_id": region, "month": m, "residential": res, "commercial": int(round(total)) - res})
customers = pd.DataFrame(cust_rows)
customers["total"] = customers.residential + customers.commercial

# COMMAND ----------

def tmean_series(station):
    w = pd.read_csv(f"{NOAA}/{station}.csv", parse_dates=["date"]).dropna(subset=["tmax", "tmin"])
    return ((w.tmax + w.tmin) / 2).set_axis(w.date)

rng = np.random.default_rng(20261001)
days = pd.date_range(HISTORY_START, HORIZON_END)
frames = []
for region, p in REGIONS.items():
    t = tmean_series(p["station"]).reindex(days).interpolate(limit_area="inside")
    hol = holidays.reindex(days).fillna(0).values
    occ = np.where(hol == 1, p["occ_holiday"], 1.0)
    if region == "summit":  # more second homes occupied through the 2026-27 season (early snow, remote work)
        occ = np.where(days >= SEASON_START, occ * 1.05, occ)
    cust = (customers[customers.region_id == region].set_index("month").total
            .reindex(days.to_period("M").to_timestamp()).to_numpy())
    behavior = pd.Series(rng.normal(0, 0.02, len(months)), index=months)  # monthly swings weather does not explain
    vol = cust * (p["base"] + p["slope"] * np.maximum(0, p["bp"] - t.values)) * occ
    vol = vol * behavior.reindex(days.to_period("M").to_timestamp()).add(1).to_numpy() * (1 + rng.normal(0, 0.03, len(days)))
    frames.append(pd.DataFrame({"region_id": region, "gas_day": days, "volume": np.round(vol, 1), "cust": cust}))

vol = pd.concat(frames, ignore_index=True).sort_values(["gas_day", "region_id"], ignore_index=True)
vol["volume_id"] = np.arange(1, len(vol) + 1)
vol["unit"] = "DTH"
vol["read_status"] = "ACTUAL"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Injected defects (fixed seed)
# MAGIC | Defect | Where | Expected rule |
# MAGIC |---|---|---|
# MAGIC | Null or negative volume | History, random days | `volume_present` (drop) |
# MAGIC | Unit mix-up (therms recorded as Dth, about 10x) | History, cold days | `volume_plausible` (drop) |
# MAGIC | Unknown region code | History, a few rows | `known_region` (drop) |
# MAGIC | Zero residential meters in a monthly count | History, one month | `customer_count_valid` (drop) |
# MAGIC | Estimated read | The last days of each extract | `actual_read` (warn) |
# MAGIC | Duplicated row | Pueblo, three days before the snapshot | removed by a CDC delete in `batch_001` |

# COMMAND ----------

hist_idx = vol.index[vol.gas_day < FORECAST_DATE].to_numpy()
winter_idx = vol.index[(vol.gas_day < FORECAST_DATE) & (vol.volume / vol.cust > 0.5)].to_numpy()
spike_idx = rng.choice(winter_idx, size=12, replace=False)  # a 10x error on a cold day is far out of range
pick = rng.choice(np.setdiff1d(hist_idx, spike_idx), size=17, replace=False)
null_idx, neg_idx, region_idx = pick[:8], pick[8:14], pick[14:17]
vol["volume"] = vol["volume"].astype(object)
vol.loc[null_idx, "volume"] = None
vol.loc[neg_idx, "volume"] = -vol.loc[neg_idx, "volume"].astype(float)
vol.loc[spike_idx, "volume"] = np.round(vol.loc[spike_idx, "volume"].astype(float) * 10, 1)
vol.loc[region_idx, "region_id"] = "summit_cty"  # legacy code no longer in the region table

bad_month = (customers.region_id == "park_foothills") & (customers.month == pd.Timestamp(f"{SEASON - 7}-03-01"))
customers.loc[bad_month, "residential"] = 0  # a meter-count export that lost its residential rows

# Estimated reads at the end of each extract window (meters not yet read): the snapshot's last 3 days, each
# increment's last 2 new days, in the two mountain regions. The next extract turns them into actual reads.
window_start = SNAPSHOT_END - pd.Timedelta(days=3) if N == 0 else max(PREV_END, AS_OF - pd.Timedelta(days=2))
est_now = vol.index[(vol.gas_day > window_start) & (vol.gas_day <= AS_OF) & vol.region_id.isin(["summit", "teller"])]
vol.loc[est_now, "read_status"] = "ESTIMATED"

# Duplicate row in the snapshot (same region and day, new volume_id), deleted in batch_001
dup = vol[(vol.region_id == "pueblo") & (vol.gas_day == SNAPSHOT_END - pd.Timedelta(days=3))].copy()
dup["volume_id"] = vol.volume_id.max() + 1
vol = pd.concat([vol, dup], ignore_index=True)

# COMMAND ----------

def event(op, before, after, table, file, pos, ts):
    return {"op": op, "before": before, "after": after, "ts_ms": ts,
            "source": {"connector": "mysql", "db": "gms", "table": table, "file": file, "pos": pos, "ts_ms": ts}}

def row(r, updated_at):
    v = r["volume"]
    return {"volume_id": int(r["volume_id"]), "region_id": r["region_id"], "gas_day": r["gas_day"].strftime("%Y-%m-%d"),
            "volume": None if v is None or (isinstance(v, float) and np.isnan(v)) else float(v),
            "unit": r["unit"], "read_status": r["read_status"], "updated_at": updated_at}

def cust_row(r, updated_at):
    return {"region_id": r["region_id"], "month": r["month"].strftime("%Y-%m-%d"), "residential": int(r["residential"]),
            "commercial": int(r["commercial"]), "updated_at": updated_at}

def region_row(region, month, updated_at):
    total = int(customers[(customers.region_id == region) & (customers.month == month)].total.iloc[0])
    return {"region_id": region, "name": REGIONS[region]["name"], "customers": total, "updated_at": updated_at}

def write_batch(batch, events_by_table):
    for table, events in events_by_table.items():
        d = f"{CDC}/gms.{table}"
        os.makedirs(d, exist_ok=True)
        with open(f"{d}/batch_{batch}.json", "w") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")
    volume_events = events_by_table["daily_volume"]
    ops = pd.Series([e["op"] for e in volume_events]).value_counts().to_dict()
    control = {
        "batch": batch,
        "snapshot_end": str(SNAPSHOT_END.date()),
        "as_of_date": str(AS_OF.date()),
        "extract_date": str(EXTRACT_DATE.date()),
        "estimated_volume_ids": [int(i) for i in vol.loc[est_now, "volume_id"]],
        "extracted_at": ts,
        "tables": {
            "region": {"events": len(events_by_table["region"])},
            "customer_count": {"events": len(events_by_table["customer_count"])},
            "daily_volume": {
                "events": len(volume_events),
                "events_by_op": {k: int(v) for k, v in ops.items()},
                "sum_after_volume": round(sum((e["after"] or {}).get("volume") or 0.0 for e in volume_events), 1),
            },
        },
    }
    os.makedirs(f"{CDC}/_control", exist_ok=True)
    with open(f"{CDC}/_control/batch_{batch}.json", "w") as f:
        json.dump(control, f)
    return control

extract_at = EXTRACT_DATE + pd.Timedelta(hours=2)
ts = int(extract_at.timestamp() * 1000)
stamp = extract_at.strftime("%Y-%m-%d %H:%M:%S")
file = f"mysql-bin.{41 + N:06d}"
this_month = min(EXTRACT_DATE, HORIZON_END).to_period("M").to_timestamp()  # meter counts are known on the 1st

if N == 0:
    region_events = [event("c", None, region_row(k, this_month, stamp), "region", file, i, ts) for i, k in enumerate(REGIONS)]
    cust_events = [event("c", None, cust_row(r, stamp), "customer_count", file, 10 + i, ts)
                   for i, r in enumerate(customers[customers.month <= this_month].to_dict("records"))]
    snap = vol[vol.gas_day <= SNAPSHOT_END]
    volume_events = [event("c", None, row(r, stamp), "daily_volume", file, 1000 + i, ts)
                     for i, r in enumerate(snap.to_dict("records"))]
    updates = []
else:
    prev = (PREV_EXTRACT + pd.Timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
    prev_month = min(PREV_EXTRACT, HORIZON_END).to_period("M").to_timestamp()
    region_events = ([event("u", region_row(k, prev_month, prev), region_row(k, this_month, stamp), "region", file, i, ts)
                      for i, k in enumerate(REGIONS)] if this_month > prev_month else [])
    cust_events = [event("c", None, cust_row(r, stamp), "customer_count", file, 10 + i, ts)
                   for i, r in enumerate(customers[(customers.month > prev_month) & (customers.month <= this_month)].to_dict("records"))]
    volume_events, pos = [], 1000
    for r in vol[(vol.gas_day > PREV_END) & (vol.gas_day <= AS_OF)].to_dict("records"):
        volume_events.append(event("c", None, row(r, stamp), "daily_volume", file, pos, ts)); pos += 1
    # The previous extract's estimated reads become actual reads
    updates = vol[vol.volume_id.isin(PREV_ESTIMATED)].to_dict("records")
    for r in updates:
        before = row(dict(r, read_status="ESTIMATED"), prev)
        r2 = dict(r, read_status="ACTUAL", volume=round(float(r["volume"]) * 1.02, 1))
        volume_events.append(event("u", before, row(r2, stamp), "daily_volume", file, pos, ts)); pos += 1
    if N == 1:  # delete the row the snapshot sent twice
        volume_events.append(event("d", row(dup.iloc[0].to_dict(), prev), None, "daily_volume", file, pos, ts))

control = write_batch(BATCH, {"region": region_events, "customer_count": cust_events, "daily_volume": volume_events})
print(json.dumps(control, indent=2))

# COMMAND ----------

# Ground truth for the injected-versus-caught check (evidence/injected_vs_caught.md)
first = N == 0
truth = {
    "batch": BATCH,
    "volume_present": int(len(null_idx) + len(neg_idx)) if first else 0,
    "volume_plausible": int(len(spike_idx)) if first else 0,
    "known_region": int(len(region_idx)) if first else 0,
    "customer_count_valid": int(bad_month.sum()) if first else 0,
    "actual_read_warn": int(len(est_now)),
    "duplicate_rows": 1 if first else 0,
    "deletes": 1 if N == 1 else 0,
    "updates": len(updates),
    "duplicate_gas_day": str((SNAPSHOT_END - pd.Timedelta(days=3)).date()),
}
with open(f"{CDC}/_control/injected_batch_{BATCH}.json", "w") as f:
    json.dump(truth, f)
print(json.dumps(truth, indent=2))

dbutils.notebook.exit(json.dumps({"control": control, "injected": truth}))
