# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Ingest EIA: real Colorado gas deliveries and customer counts
# MAGIC
# MAGIC Public U.S. Energy Information Administration series (no key needed), used for the statewide
# MAGIC real-data check of the method and to calibrate the synthetic regions' usage per customer:
# MAGIC
# MAGIC | Series | What | Frequency |
# MAGIC |---|---|---|
# MAGIC | `N3010CO2` | Colorado residential natural gas consumption, MMcf | Monthly |
# MAGIC | `N3020CO2` | Colorado commercial natural gas deliveries, MMcf | Monthly |
# MAGIC | `NA1501_SCO_8` | Colorado residential natural gas consumers, count | Annual |
# MAGIC | `NA1531_SCO_8` | Colorado commercial natural gas consumers, count | Annual |
# MAGIC
# MAGIC Raw spreadsheets are kept in the `eia` volume. If a download fails, the last cached copy is used and
# MAGIC the run says so. The parser checks each sheet's layout before reading it.

# COMMAND ----------

# MAGIC %pip install -q xlrd
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "")
CATALOG = dbutils.widgets.get("catalog")
assert CATALOG
EIA = f"/Volumes/{CATALOG}/cgu_landing/eia"

import json
import os
import time
import urllib.request

import pandas as pd

SERIES = {
    "N3010CO2": ("m", "residential_mmcf", "MMcf"),
    "N3020CO2": ("m", "commercial_mmcf", "MMcf"),
    "NA1501_SCO_8": ("a", "residential_consumers", "count"),
    "NA1531_SCO_8": ("a", "commercial_consumers", "count"),
}
os.makedirs(f"{EIA}/raw", exist_ok=True)

# COMMAND ----------

status, frames = {}, []
for sid, (freq, measure, unit) in SERIES.items():
    path = f"{EIA}/raw/{sid}{freq}.xls"
    try:
        for attempt in range(3):
            try:
                req = urllib.request.Request(f"https://www.eia.gov/dnav/ng/hist_xls/{sid}{freq}.xls",
                                             headers={"User-Agent": "Mozilla/5.0 (cgu-winter-volume-demo)"})
                with urllib.request.urlopen(req, timeout=60) as r:
                    body = r.read()
                break
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(5 * (attempt + 1))
        assert body[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", "not an Excel file"
        with open(path, "wb") as f:
            f.write(body)
        status[sid] = "downloaded"
    except Exception as e:
        assert os.path.exists(path), f"{sid}: download failed and no cached copy ({e})"
        status[sid] = f"download failed ({type(e).__name__}); cached copy used"

    sheet = pd.read_excel(path, sheet_name="Data 1", header=None, engine="xlrd")
    assert str(sheet.iloc[1, 0]) == "Sourcekey" and str(sheet.iloc[1, 1]) == sid, f"{sid}: unexpected sheet layout"
    d = sheet.iloc[3:, :2].dropna()
    d.columns = ["period_date", "value"]
    d["period_date"] = pd.to_datetime(d.period_date)
    d["period"] = d.period_date.dt.strftime("%Y-%m") if freq == "m" else d.period_date.dt.strftime("%Y")
    frames.append(d.assign(series_id=sid, measure=measure, unit=unit, frequency="monthly" if freq == "m" else "annual",
                           description=str(sheet.iloc[2, 1]))[["series_id", "measure", "frequency", "period", "value",
                                                                 "unit", "description"]])

series = pd.concat(frames, ignore_index=True)
series["value"] = series.value.astype(float)
series.to_csv(f"{EIA}/eia_series.csv", index=False)
latest = series.groupby("series_id").period.max().to_dict()
print(status, latest)

dbutils.notebook.exit(json.dumps({"status": status, "rows": len(series), "latest_period": latest}))
