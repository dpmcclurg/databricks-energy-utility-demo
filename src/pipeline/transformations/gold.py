"""Gold: the FP&A data product in cgu_fpa.

- gold_daily_region: one row per region and gas day, volume joined to weather, customers, and the occupancy calendar.
- gold_state_monthly: real EIA Colorado residential and commercial volumes and consumer counts, with the
  statewide customer-weighted HDD65.
- gold_reconciliation: what bronze received against the control totals the source reported.
- gold_dq_summary: rows checked, failed, and action per FP&A rule and extract batch.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities import dq

CATALOG = spark.conf.get("cgu.catalog")
FPA = f"{CATALOG}.cgu_fpa"


@dp.materialized_view(
    name=f"{FPA}.gold_daily_region",
    comment="Daily gas volume (Dth) per service region with customers (meters), use per customer, HDD65, mean "
            "temperature, and occupancy flag.",
)
def gold_daily_region():
    vol = (spark.read.table("silver_daily_volume")
           .groupBy("region_id", "gas_day")
           .agg(F.round(F.sum("volume"), 1).alias("volume_dth"),
                F.count("*").alias("source_rows"),
                F.when(F.max(F.col("read_status") == "ESTIMATED"), "ESTIMATED").otherwise("ACTUAL").alias("read_status")))
    wx = spark.read.table("silver_weather_daily").where("region_id IS NOT NULL").select("region_id", "gas_day", "tmean", "hdd65", "tmean_source")
    cal = spark.read.table(f"{FPA}.ref_holiday_calendar").select("gas_day", "holiday_flag")
    regions = spark.read.table("silver_region").select("region_id", F.col("name").alias("region_name"))
    cust = spark.read.table("silver_customer_count").select(
        "region_id", "month", (F.col("residential") + F.col("commercial")).alias("customers"))
    return (vol.join(regions, "region_id")
            .withColumn("month", F.trunc("gas_day", "month"))
            .join(cust, ["region_id", "month"], "left")
            .join(wx, ["region_id", "gas_day"], "left")
            .join(cal, "gas_day", "left")
            .withColumn("holiday_flag", F.coalesce("holiday_flag", F.lit(0)))
            .withColumn("use_per_customer", F.round(F.col("volume_dth") / F.col("customers"), 5))
            .select("region_id", "region_name", "gas_day", "volume_dth", "customers", "use_per_customer", "hdd65",
                    "tmean", "tmean_source", "holiday_flag", "read_status", "source_rows"))


@dp.materialized_view(
    name=f"{FPA}.gold_state_monthly",
    comment="Real EIA Colorado residential and commercial gas volumes (MMcf) by month, annual consumer counts, and the "
            "statewide customer-weighted HDD65 from cgu_fpa.ref_weather_station. Used for the statewide backtest.",
)
def gold_state_monthly():
    eia = spark.read.table("bronze_eia_series")
    vol = (eia.where("frequency = 'monthly'").groupBy("period")
           .pivot("measure", ["residential_mmcf", "commercial_mmcf"]).agg(F.first("value")))
    cons = (eia.where("frequency = 'annual'").groupBy(F.col("period").alias("year"))
            .pivot("measure", ["residential_consumers", "commercial_consumers"]).agg(F.first("value")))
    weights = spark.read.table(f"{FPA}.ref_weather_station").where("state_hdd_weight > 0").select("station", "state_hdd_weight")
    wx = (spark.read.table("silver_weather_daily").join(weights, "station")
          .groupBy("gas_day").agg((F.sum(F.col("tmean") * F.col("state_hdd_weight")) / F.sum("state_hdd_weight")).alias("tmean"))
          .groupBy(F.date_format("gas_day", "yyyy-MM").alias("period"))
          .agg(F.round(F.sum(F.greatest(F.lit(0.0), 65 - F.col("tmean"))), 1).alias("hdd65_weighted"),
               F.count("*").alias("weather_days")))
    return (vol.withColumn("month", F.to_date(F.concat("period", F.lit("-01"))))
            .withColumn("year", F.substring("period", 1, 4))
            .join(cons, "year", "left")
            .join(wx, "period", "left")
            .withColumn("total_mmcf", F.col("residential_mmcf") + F.col("commercial_mmcf"))
            .withColumn("days_in_month", F.dayofmonth(F.last_day("month")))
            .select("month", "residential_mmcf", "commercial_mmcf", "total_mmcf", "days_in_month",
                    F.round(F.col("total_mmcf") / F.col("days_in_month"), 2).alias("mmcf_per_day"),
                    "residential_consumers", "commercial_consumers", "hdd65_weighted", "weather_days"))


@dp.materialized_view(
    name=f"{FPA}.gold_reconciliation",
    comment="Per extract batch: change events and total volume received in bronze against the source control totals.",
)
def gold_reconciliation():
    received = (spark.read.table("bronze_daily_volume_cdc")
                .groupBy(F.col("_batch").alias("batch"))
                .agg(F.count("*").alias("bronze_events"),
                     F.round(F.sum(F.coalesce(F.col("after.volume"), F.lit(0.0))), 1).alias("bronze_sum_volume")))
    control = (spark.read.table("bronze_control")
               .select("batch", F.col("tables.daily_volume.events").alias("control_events"),
                       F.col("tables.daily_volume.sum_after_volume").alias("control_sum_volume")))
    return (control.join(received, "batch", "left")
            .withColumn("events_match", F.col("control_events") == F.col("bronze_events"))
            .withColumn("volume_match", F.abs(F.col("control_sum_volume") - F.col("bronze_sum_volume")) < 0.5)
            .withColumn("status", F.when(F.col("events_match") & F.col("volume_match"), "BALANCED").otherwise("OUT OF BALANCE")))


@dp.materialized_view(
    name=f"{FPA}.gold_dq_summary",
    comment="Rows checked and failed per FP&A data quality rule and extract batch, evaluated from cgu_fpa.dq_rules.",
)
def gold_dq_summary():
    rules = spark.read.table(f"{FPA}.dq_rules").collect()
    regions = spark.read.table("silver_region").select("region_id", "customers")
    volume = (spark.read.table("bronze_daily_volume_cdc")
              .where("op <> 'd'")
              .select(F.col("_batch").alias("batch"), "after.*")
              .join(regions, "region_id", "left"))
    weather = spark.read.table("bronze_weather").where("tmax IS NOT NULL AND tmin IS NOT NULL").withColumn("batch", F.lit("noaa"))
    customers = (spark.read.table("bronze_customer_count_cdc").where("op <> 'd'")
                 .select(F.col("_batch").alias("batch"), "after.*"))
    sources = {"daily_volume": volume, "weather": weather, "customers": customers}
    parts = []
    for r in rules:
        src = sources[r.target]
        parts.append(src.groupBy("batch").agg(
            F.count("*").alias("rows_checked"),
            F.sum(F.when(~F.coalesce(F.expr(r.constraint_sql), F.lit(False)), 1).otherwise(0)).alias("rows_failed"))
            .select(F.lit(r.rule_name).alias("rule_name"), F.lit(r.target).alias("target"),
                    F.lit(r.action).alias("action"), F.lit(r.owner).alias("owner"), "batch", "rows_checked", "rows_failed"))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.withColumn("pass_rate", F.round(1 - F.col("rows_failed") / F.col("rows_checked"), 6))
