# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup: schemas, volumes, reference data, and FP&A-owned data quality rules
# MAGIC
# MAGIC Creates the schemas inside the workspace catalog, the landing volumes, the `cgu_fpa.dq_rules`
# MAGIC table that drives the pipeline's expectations, the reference tables (weather stations, occupancy
# MAGIC calendar, firm transportation contracts), and the governance metadata (comments and tags). Safe to
# MAGIC rerun: rules are seeded only when the table is empty.
# MAGIC
# MAGIC | Schema | Role |
# MAGIC |---|---|
# MAGIC | `cgu_landing` | Raw files: source-system CDC extracts, NOAA observations, NWS forecasts, EIA volumes |
# MAGIC | `cgu_core` | Bronze and silver tables, quarantine, run context |
# MAGIC | `cgu_fpa` | FP&A-owned rules, reference data, gold tables, metric view, model, plan, re-forecast |
# MAGIC | `cgu_serving` | Lakebase synced tables that serve the app |

# COMMAND ----------

dbutils.widgets.text("catalog", "")
CATALOG = dbutils.widgets.get("catalog")
assert CATALOG, "catalog parameter is required"
spark.sql(f"USE CATALOG `{CATALOG}`")

# COMMAND ----------

schemas = {
    "cgu_landing": "Raw landing zone: CDC extracts from the gas measurement system, NOAA GHCN-Daily observations, NWS forecasts, EIA volumes.",
    "cgu_core": "Validated core data: bronze CDC events, silver current-state tables, quarantine, weather forecasts, run context.",
    "cgu_fpa": "FP&A data product: validation rules, reference data, gold tables, metric view, forecast model, season plan and re-forecast.",
    "cgu_serving": "Lakebase synced tables (read-only UC entries) that serve the Winter Volume Plan app.",
}
for name, comment in schemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {name} COMMENT '{comment}'")
    spark.sql(f"COMMENT ON SCHEMA {name} IS '{comment}'")
spark.sql("ALTER SCHEMA cgu_fpa SET TAGS ('domain' = 'finance')")
spark.sql("ALTER SCHEMA cgu_core SET TAGS ('domain' = 'finance')")

for vol, comment in {
    "cdc": "Debezium-style CDC JSON from the gas measurement system (MySQL), one folder per table, plus _control totals.",
    "noaa": "NOAA GHCN-Daily station CSVs (TMAX, TMIN) for the five service regions and the statewide reference station.",
    "nws": "National Weather Service gridpoint forecasts, one file per issuance (daily high and low per region).",
    "eia": "EIA Colorado natural gas deliveries and consumer counts (public downloads), raw and parsed.",
}.items():
    spark.sql(f"CREATE VOLUME IF NOT EXISTS cgu_landing.{vol} COMMENT '{comment}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rules as data
# MAGIC FP&A owns these rules. The pipeline reads this table when each update starts, so a rule change
# MAGIC is a SQL `UPDATE` and takes effect on the next run. `DESCRIBE HISTORY` shows who changed what.

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS cgu_fpa.dq_rules (
  rule_name      STRING NOT NULL COMMENT 'Unique rule name; appears in pipeline expectation metrics',
  target         STRING NOT NULL COMMENT 'Dataset the rule applies to',
  constraint_sql STRING NOT NULL COMMENT 'SQL boolean expression a valid row must satisfy',
  action         STRING NOT NULL COMMENT 'warn = keep and flag, drop = quarantine, fail = stop the update',
  owner          STRING NOT NULL COMMENT 'Accountable team',
  description    STRING COMMENT 'Plain-language meaning of the rule',
  CONSTRAINT dq_rules_pk PRIMARY KEY (rule_name)
)
COMMENT 'FP&A-owned data quality rules. The Lakeflow pipeline builds its expectations from this table.'
""")
spark.sql("ALTER TABLE cgu_fpa.dq_rules SET TAGS ('domain' = 'finance')")

if spark.table("cgu_fpa.dq_rules").count() == 0:
    spark.sql("""
    INSERT INTO cgu_fpa.dq_rules VALUES
      ('volume_present',      'daily_volume', 'volume IS NOT NULL AND volume >= 0',
       'drop', 'FP&A', 'A daily volume must exist and cannot be negative (telemetry error).'),
      ('volume_plausible',    'daily_volume', 'volume IS NULL OR customers IS NULL OR volume <= 2.0 * customers',
       'drop', 'FP&A', 'Daily volume above 2 Dth per customer is implausible (likely a therm/Dth unit mix-up).'),
      ('known_region',        'daily_volume', 'customers IS NOT NULL',
       'drop', 'FP&A', 'Every volume must belong to a known service region.'),
      ('actual_read',         'daily_volume', "read_status = 'ACTUAL'",
       'warn', 'FP&A', 'Estimated reads are kept but flagged until the actual read arrives.'),
      ('customer_count_valid', 'customers',   'residential > 0 AND commercial >= 0',
       'drop', 'FP&A', 'Monthly meter counts must be positive; the model multiplies usage per customer by them.'),
      ('temp_order',          'weather',      'tmax >= tmin',
       'drop', 'FP&A', 'Daily maximum temperature cannot be below the minimum.'),
      ('temp_range',          'weather',      'tmin >= -50 AND tmax <= 110',
       'drop', 'FP&A', 'Temperatures outside -50F to 110F are sensor errors for Colorado.')
    """)

display(spark.table("cgu_fpa.dq_rules"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather stations
# MAGIC One NOAA GHCN-Daily station per service region, plus Colorado Springs for the statewide check.
# MAGIC `state_hdd_weight` is the share of Colorado's residential and commercial gas customers each station
# MAGIC stands for in the statewide weighted HDD (an assumption, documented in the README).

# COMMAND ----------

stations = spark.createDataFrame([
    ("USC00052281", "Dillon 1 E",               "summit",         39.6244, -106.0336, 2768.0, 0.10),
    ("USC00054742", "Lake George 8 SW",         "teller",         38.9069, -105.4719, 2585.0, 0.0),
    ("USC00053530", "Grant",                    "park_foothills", 39.4608, -105.6786, 2652.0, 0.0),
    ("USW00093058", "Pueblo Memorial Airport",  "pueblo",         38.2886, -104.5058, 1425.0, 0.05),
    ("USW00003017", "Denver International Airport", "eastern_plains", 39.8467, -104.6561, 1647.0, 0.65),
    ("USW00093037", "Colorado Springs Municipal Airport", None,   38.8094, -104.6886, 1884.0, 0.20),
], "station STRING, station_name STRING, region_id STRING, lat DOUBLE, lon DOUBLE, elevation_m DOUBLE, state_hdd_weight DOUBLE")
stations.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("cgu_fpa.ref_weather_station")
spark.sql("COMMENT ON TABLE cgu_fpa.ref_weather_station IS 'NOAA GHCN-Daily station per service region (region_id NULL = statewide "
          "reference only), coordinates for the NWS forecast lookup, and weights for the statewide customer-weighted HDD.'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Firm transportation contracts (fictional)
# MAGIC Contracted firm pipeline capacity per region, Dth/day. Sized when signed to the prior winter's
# MAGIC 1-in-30 design day plus a margin. Fictional contracts and pipeline names.

# COMMAND ----------

spark.createDataFrame([
    ("summit",         4200.0,  "Blue River Lateral (fictional)",   "2028-10-31"),
    ("teller",         9700.0,  "Ute Pass Transmission (fictional)", "2029-10-31"),
    ("park_foothills", 9800.0,  "Kenosha Lateral (fictional)",      "2028-10-31"),
    ("pueblo",         10150.0, "Arkansas Valley Line (fictional)", "2030-10-31"),
    ("eastern_plains", 6800.0,  "High Plains Lateral (fictional)",  "2027-10-31"),
], "region_id STRING, contracted_dth_per_day DOUBLE, pipeline STRING, contract_end STRING") \
    .selectExpr("region_id", "contracted_dth_per_day", "pipeline", "CAST(contract_end AS DATE) contract_end") \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("cgu_fpa.firm_capacity")
spark.sql("COMMENT ON TABLE cgu_fpa.firm_capacity IS 'FICTIONAL firm transportation contracts per region (Dth/day). "
          "Sized to the prior winter design day plus a margin. Not real contracts or pipelines.'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Occupancy calendar
# MAGIC High-occupancy days for mountain second homes: Thanksgiving (Wednesday to Sunday), December 20 to
# MAGIC January 2, MLK and Presidents Day weekends, mid-March spring break.

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS cgu_fpa.ref_holiday_calendar (
  gas_day      DATE NOT NULL,
  holiday_flag INT  NOT NULL COMMENT '1 on high-occupancy days for mountain second homes',
  holiday_name STRING,
  CONSTRAINT ref_holiday_calendar_pk PRIMARY KEY (gas_day)
) COMMENT 'FP&A calendar of high-occupancy periods (Thanksgiving, Dec 20 to Jan 2, MLK and Presidents Day weekends, mid-March spring break).'
""")

import datetime as dt
import pandas as pd

def nth_weekday(year, month, weekday, n):
    first = dt.date(year, month, 1)
    return first + dt.timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))

rows = []
for d in pd.date_range("1990-01-01", "2027-12-31").date:
    name = None
    thanksgiving = nth_weekday(d.year, 11, 3, 4)
    if d.month == 11 and thanksgiving - dt.timedelta(days=1) <= d <= thanksgiving + dt.timedelta(days=3):
        name = "Thanksgiving"
    elif (d.month == 12 and d.day >= 20) or (d.month == 1 and d.day <= 2):
        name = "Christmas to New Year"
    elif d.month in (1, 2):
        tm = nth_weekday(d.year, d.month, 0, 3)
        if tm - dt.timedelta(days=2) <= d <= tm:
            name = "MLK weekend" if d.month == 1 else "Presidents Day weekend"
    elif d.month == 3 and 8 <= d.day <= 22:
        name = "Spring break"
    rows.append((d, 1 if name else 0, name))

(spark.createDataFrame(pd.DataFrame(rows, columns=["gas_day", "holiday_flag", "holiday_name"]))
 .selectExpr("CAST(gas_day AS DATE) gas_day", "CAST(holiday_flag AS INT) holiday_flag", "holiday_name")
 .write.mode("overwrite").option("overwriteSchema", "false").saveAsTable("cgu_fpa.ref_holiday_calendar"))

display(spark.sql("SELECT holiday_name, COUNT(*) days FROM cgu_fpa.ref_holiday_calendar WHERE holiday_flag = 1 GROUP BY 1"))

# COMMAND ----------

dbutils.notebook.exit(f"setup complete: rules={spark.table('cgu_fpa.dq_rules').count()}")
