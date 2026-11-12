# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Ingest weather: NOAA observations and the NWS 7-day forecast
# MAGIC
# MAGIC Replaces FP&A copying temperatures from the NOAA and weather.gov websites by hand.
# MAGIC
# MAGIC - **Observed:** daily TMAX and TMIN for one station per service region, plus Colorado Springs for the
# MAGIC   statewide check, from the public NCEI Access Data Service (GHCN-Daily, no key), 1995 to yesterday.
# MAGIC - **Outages:** each download retries for about 5 minutes. If NOAA is still unavailable, the station's last
# MAGIC   stored file is used and the as-of date holds; if the NWS is, the last stored issuance is used.
# MAGIC - **As-of date:** the latest gas day every region's station has reported. Co-op stations report a few
# MAGIC   days late, so this trails today. A fixed `as_of_date` parameter overrides it.
# MAGIC - **Forecast:** the National Weather Service gridpoint forecast for each station's location, daily high
# MAGIC   and low for the next 7 days. One file per issuance, kept, so each run records the forecast it used.
# MAGIC
# MAGIC - **Extract number:** `batch=auto` takes the next number after the last extract in the landing volume
# MAGIC   (`000` on a clean slate), so a scheduled daily run needs no parameters.
# MAGIC
# MAGIC The resolved batch, as-of date, extract date, and NWS issuance are written to `cgu_core.run_context`;
# MAGIC every later task reads the latest row from there.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("batch", "auto")
dbutils.widgets.text("season", "2026")
dbutils.widgets.text("as_of_date", "auto")
CATALOG = dbutils.widgets.get("catalog")
BATCH_PARAM = dbutils.widgets.get("batch").strip().lower()
SEASON = int(dbutils.widgets.get("season"))
AS_OF_PARAM = dbutils.widgets.get("as_of_date").strip().lower()
assert CATALOG, "catalog parameter is required"
NOAA = f"/Volumes/{CATALOG}/cgu_landing/noaa"
NWS = f"/Volumes/{CATALOG}/cgu_landing/nws"
CONTROL = f"/Volumes/{CATALOG}/cgu_landing/cdc/_control"

# COMMAND ----------

import io
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

DENVER = ZoneInfo("America/Denver")
TODAY = datetime.now(DENVER).date()
START, END = "1995-01-01", str(TODAY - timedelta(days=1))
SEASON_START, SEASON_END = pd.Timestamp(f"{SEASON}-11-01"), pd.Timestamp(f"{SEASON + 1}-03-31")
stations = spark.table(f"`{CATALOG}`.cgu_fpa.ref_weather_station").toPandas()
REGION_STATIONS = stations[stations.region_id.notna()]

BASE = "https://www.ncei.noaa.gov/access/services/data/v1"


def get(url: str, headers: dict, timeout: int = 120, attempts: int = 5) -> bytes:
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=timeout) as r:
                return r.read()
        except Exception as e:  # NCEI and NWS throttle bursts and have short outages; back off and retry
            if attempt == attempts - 1:
                raise
            print(f"retry {url[:90]} after {e}")
            time.sleep(30 * (attempt + 1))


# COMMAND ----------

# MAGIC %md
# MAGIC ## NOAA GHCN-Daily

# COMMAND ----------

screen = []
for s in stations.itertuples():
    url = (f"{BASE}?dataset=daily-summaries&stations={s.station}&dataTypes=TMAX,TMIN"
           f"&startDate={START}&endDate={END}&units=standard&format=csv")
    stored = f"{NOAA}/{s.station}.csv"
    try:
        df = pd.read_csv(io.StringIO(get(url, {"User-Agent": "cgu-winter-volume-demo"}, 180).decode()))
        df.columns = [c.lower() for c in df.columns]
        df.insert(0, "region_id", s.region_id or "")
        df.insert(2, "station_name", s.station_name)
        df.to_csv(stored, index=False)
        noaa_status = "downloaded"
    except Exception as e:  # NCEI outage: keep the last stored observations; the as-of date then holds
        if not os.path.exists(stored):
            raise
        df = pd.read_csv(stored)
        noaa_status = f"unavailable ({type(e).__name__}); stored file used"
        print(f"NOAA {s.station}: {noaa_status}: {e}")

    df["date"] = pd.to_datetime(df["date"])
    full = df.dropna(subset=["tmax", "tmin"])
    in_season = full[full.date >= SEASON_START]
    screen.append({
        "region_id": s.region_id, "station": s.station, "station_name": s.station_name,
        "first_day": full.date.min().date(), "last_complete_day": full.date.max().date(),
        "complete_pct": round(100 * len(full) / ((pd.Timestamp(END) - pd.Timestamp(START)).days + 1), 1),
        "season_days_reported": len(in_season), "noaa_status": noaa_status,
    })

for f in os.listdir(NOAA):  # stations no longer in the reference table stop feeding the pipeline
    if f.endswith(".csv") and f[:-4] not in set(stations.station):
        os.remove(f"{NOAA}/{f}")
        print(f"removed retired station file {f}")

screen = pd.DataFrame(screen)
display(screen)

# COMMAND ----------

done = sorted(int(m.group(1)) for f in (os.listdir(CONTROL) if os.path.exists(CONTROL) else [])
              if (m := re.fullmatch(r"batch_(\d+)\.json", f)))
BATCH = (f"{done[-1] + 1:03d}" if done else "000") if BATCH_PARAM == "auto" else BATCH_PARAM

region_screen = screen[screen.region_id.notna()]
latest_complete = pd.Timestamp(region_screen.last_complete_day.min())
if AS_OF_PARAM == "auto":
    as_of, as_of_mode = min(latest_complete, SEASON_END), "auto"
else:
    as_of, as_of_mode = pd.Timestamp(AS_OF_PARAM), "fixed"
    assert as_of <= latest_complete, f"as_of_date {as_of.date()} is after the latest complete NOAA day {latest_complete.date()}"
# The source system extracts in the early morning: today for a live run; the day after a fixed as-of date for a replay
extract_date = pd.Timestamp(TODAY) if as_of_mode == "auto" else as_of + pd.Timedelta(days=1)
print(f"batch {BATCH}; as-of date {as_of.date()} ({as_of_mode}); latest day all region stations reported: "
      f"{latest_complete.date()}; extract date {extract_date.date()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## NWS 7-day forecast
# MAGIC `api.weather.gov/points/{lat},{lon}` resolves each station to its forecast grid cell; the cell's
# MAGIC gridpoint data gives the forecast daily maximum and minimum temperature. The daily mean and HDD65 are
# MAGIC computed the same way as for observations.

# COMMAND ----------

NWS_HEADERS = {"User-Agent": "cgu-winter-volume-demo (fictional utility demo)", "Accept": "application/geo+json"}


def duration(iso: str) -> timedelta:
    m = re.fullmatch(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?)?", iso)
    d, h, mi = (int(x) if x else 0 for x in m.groups())
    return timedelta(days=d, hours=h, minutes=mi)


def daily(values: list, use_end: bool) -> dict:
    """Map NWS validTime intervals to local dates: highs to the day they start, overnight lows to the
    morning they end. Values are degrees C."""
    out = {}
    for v in values:
        start_s, dur = v["validTime"].split("/")
        start = datetime.fromisoformat(start_s)
        t = (start + duration(dur)) if use_end else start
        if v["value"] is not None:
            out[t.astimezone(DENVER).date()] = round(v["value"] * 9 / 5 + 32, 1)
    return out


fetched_at = datetime.now(timezone.utc).replace(microsecond=0)
rows, nws_status = [], "ok"
try:
    for s in REGION_STATIONS.itertuples():
        point = json.loads(get(f"https://api.weather.gov/points/{s.lat:.4f},{s.lon:.4f}", NWS_HEADERS))["properties"]
        grid = json.loads(get(point["forecastGridData"], NWS_HEADERS))["properties"]
        highs = daily(grid["maxTemperature"]["values"], use_end=False)
        lows = daily(grid["minTemperature"]["values"], use_end=True)
        days = sorted(d for d in highs if d in lows and d >= TODAY)[:7]
        for d in days:
            rows.append({"region_id": s.region_id, "station": s.station,
                         "grid": f"{point['gridId']} {point['gridX']},{point['gridY']}",
                         "issued_at": grid["updateTime"], "fetched_at": fetched_at.isoformat(),
                         "forecast_date": d.isoformat(), "tmax": highs[d], "tmin": lows[d]})
except Exception as e:
    nws_status = f"unavailable ({type(e).__name__}); the last stored issuance is used"
    print(f"NWS forecast {nws_status}: {e}")

if rows and nws_status == "ok":
    with open(f"{NWS}/issued_{fetched_at:%Y%m%dT%H%M%SZ}.json", "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
nws = pd.DataFrame(rows)
if len(nws):
    nws["tmean"] = (nws.tmax + nws.tmin) / 2
    nws["hdd65"] = (65 - nws.tmean).clip(lower=0).round(1)
    display(nws)
    issued_at = max(nws.issued_at)
else:
    issued_at = None

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS `{CATALOG}`.cgu_core.run_context (
  batch STRING, season INT, as_of_date DATE, as_of_mode STRING, latest_complete_noaa_day DATE, extract_date DATE,
  nws_issued_at STRING, nws_status STRING, recorded_at TIMESTAMP)
COMMENT 'One row per job run: the resolved as-of date and the NWS forecast issuance every later task uses.'""")
spark.createDataFrame([(BATCH, SEASON, as_of.date(), as_of_mode, latest_complete.date(), extract_date.date(), issued_at,
                        nws_status, fetched_at.replace(tzinfo=None))],
                      "batch STRING, season INT, as_of_date DATE, as_of_mode STRING, latest_complete_noaa_day DATE, "
                      "extract_date DATE, nws_issued_at STRING, nws_status STRING, recorded_at TIMESTAMP") \
    .write.mode("append").saveAsTable(f"`{CATALOG}`.cgu_core.run_context")

dbutils.notebook.exit(json.dumps({
    "batch": BATCH, "extract_date": str(extract_date.date()), "as_of_date": str(as_of.date()), "as_of_mode": as_of_mode, "latest_complete_noaa_day": str(latest_complete.date()),
    "stations": screen.assign(first_day=screen.first_day.astype(str), last_complete_day=screen.last_complete_day.astype(str))
                      .to_dict("records"),
    "nws": {"status": nws_status, "issued_at": issued_at, "rows": len(nws),
            "days": sorted(nws.forecast_date.unique().tolist()) if len(nws) else []},
}, default=str))
