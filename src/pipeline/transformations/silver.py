"""Silver: validated current state of the source tables.

Validation rules come from cgu_fpa.dq_rules (owned by FP&A). Rows that fail a drop rule are kept
out of silver and written to the quarantine table with the names of the rules they failed.
AUTO CDC applies inserts, updates, and deletes in binlog order (file, position), SCD Type 1.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from utilities import dq

CATALOG = spark.conf.get("cgu.catalog")
VOLUME_RULES = dq.load_rules(spark, CATALOG, "daily_volume")
WEATHER_RULES = dq.load_rules(spark, CATALOG, "weather")
CUSTOMER_RULES = dq.load_rules(spark, CATALOG, "customers")
MAX_GAP_DAYS = 3  # isolated missing station days are interpolated and flagged; longer gaps stay missing


# Regions -------------------------------------------------------------------------------------

@dp.temporary_view()
def region_changes():
    return (spark.readStream.table("bronze_region_cdc")
            .select("op", "after.*", F.struct("source.file", "source.pos").alias("_seq")))


dp.create_streaming_table("silver_region", comment="Current service regions and customer counts from gms.region.")
dp.create_auto_cdc_flow(
    target="silver_region", source="region_changes", keys=["region_id"], sequence_by="_seq",
    apply_as_deletes="op = 'd'", except_column_list=["op", "_seq"], stored_as_scd_type=1,
)


# Customers (monthly meter counts) ------------------------------------------------------------

@dp.table(comment="Validated monthly meter-count change events. Expectations are built from cgu_fpa.dq_rules.")
@dq.expectations({action: dq.skip_deletes(rules) for action, rules in CUSTOMER_RULES.items()})
def silver_customer_changes():
    return (spark.readStream.table("bronze_customer_count_cdc")
            .select("op", F.coalesce("after", "before").alias("r"), "source.file", "source.pos", "_batch")
            .select("op", "r.*", F.col("file").alias("src_file"), F.col("pos").alias("src_pos"), "_batch"))


dp.create_streaming_table(
    "silver_customer_count",
    comment="Current residential and commercial meter counts per region and month (count on the first of the month).",
)
dp.create_auto_cdc_flow(
    target="silver_customer_count", source="silver_customer_changes", keys=["region_id", "month"],
    sequence_by=F.struct("src_file", "src_pos"), apply_as_deletes="op = 'd'",
    except_column_list=["op", "src_file", "src_pos"], stored_as_scd_type=1,
)


# Daily volume --------------------------------------------------------------------------------

@dp.temporary_view()
def volume_changes():
    """Flatten each change event and attach the region's customer count for the plausibility rule."""
    regions = spark.read.table("silver_region").select("region_id", "customers")
    events = (spark.readStream.table("bronze_daily_volume_cdc")
              .select("op", F.coalesce("after", "before").alias("r"), "source.file", "source.pos", "_batch")
              .select("op", "r.*", F.col("file").alias("src_file"), F.col("pos").alias("src_pos"), "_batch"))
    return events.join(regions, "region_id", "left")


VOLUME_RULES = {action: dq.skip_deletes(rules) for action, rules in VOLUME_RULES.items()}
DROP_RULES = VOLUME_RULES["drop"]


@dp.table(comment="Validated daily-volume change events. Expectations are built from cgu_fpa.dq_rules.")
@dq.expectations(VOLUME_RULES)
def silver_volume_changes():
    return spark.readStream.table("volume_changes")


@dp.table(comment="Daily-volume change events that failed at least one FP&A drop rule, with the rules they failed.")
def quarantine_daily_volume():
    failed = F.array_compact(F.array(*[
        F.when(~F.coalesce(F.expr(cond), F.lit(False)), F.lit(name)) for name, cond in DROP_RULES.items()
    ])) if DROP_RULES else F.array().cast("array<string>")
    return (spark.readStream.table("volume_changes")
            .withColumn("failed_rules", failed)
            .where(F.size("failed_rules") > 0)
            .withColumn("quarantined_at", F.current_timestamp()))


dp.create_streaming_table(
    "silver_daily_volume",
    comment="Current daily volume per region and gas day (Dth), as in the source system after all changes.",
)
dp.create_auto_cdc_flow(
    target="silver_daily_volume", source="silver_volume_changes", keys=["volume_id"],
    sequence_by=F.struct("src_file", "src_pos"), apply_as_deletes="op = 'd'",
    except_column_list=["op", "src_file", "src_pos", "customers"], stored_as_scd_type=1,
)


# Weather -------------------------------------------------------------------------------------

@dp.temporary_view()
def weather_filled():
    """One row per station and day between its first and last report. Gaps of up to MAX_GAP_DAYS are
    filled by linear interpolation between the reports on either side and flagged as interpolated."""
    obs = spark.read.table("bronze_weather").where("tmax IS NOT NULL AND tmin IS NOT NULL")
    stations = obs.groupBy("station").agg(F.max("region_id").alias("region_id"), F.max("station_name").alias("station_name"),
                                          F.min("date").alias("d0"), F.max("date").alias("d1"))
    spine = stations.select("station", "region_id", "station_name",
                            F.explode(F.sequence("d0", "d1")).alias("gas_day"))
    x = spine.join(obs.select("station", F.col("date").alias("gas_day"), "tmax", "tmin"), ["station", "gas_day"], "left")
    before = Window.partitionBy("station").orderBy("gas_day").rowsBetween(Window.unboundedPreceding, 0)
    after = Window.partitionBy("station").orderBy("gas_day").rowsBetween(0, Window.unboundedFollowing)
    seen = F.when(F.col("tmax").isNotNull(), F.col("gas_day"))
    x = (x.withColumn("d_prev", F.last(seen, ignorenulls=True).over(before))
          .withColumn("d_next", F.first(seen, ignorenulls=True).over(after))
          .withColumn("tmax_prev", F.last("tmax", ignorenulls=True).over(before))
          .withColumn("tmax_next", F.first("tmax", ignorenulls=True).over(after))
          .withColumn("tmin_prev", F.last("tmin", ignorenulls=True).over(before))
          .withColumn("tmin_next", F.first("tmin", ignorenulls=True).over(after))
          .withColumn("gap", F.datediff("d_next", "d_prev") - 1)
          .withColumn("frac", F.datediff("gas_day", "d_prev") / F.datediff("d_next", "d_prev")))
    interp = lambda c: F.round(F.col(f"{c}_prev") + (F.col(f"{c}_next") - F.col(f"{c}_prev")) * F.col("frac"), 1)
    return (x.where(F.col("tmax").isNotNull() | (F.col("gap") <= MAX_GAP_DAYS))
             .select("region_id", "station", "station_name", "gas_day",
                     F.coalesce("tmax", interp("tmax")).alias("tmax"), F.coalesce("tmin", interp("tmin")).alias("tmin"),
                     F.when(F.col("tmax").isNotNull(), "observed").otherwise("interpolated").alias("tmean_source")))


@dp.materialized_view(comment="Validated daily weather per station with HDD65 = max(0, 65 - (TMAX + TMIN) / 2). "
                              "Gaps of up to 3 days are interpolated (tmean_source = 'interpolated').")
@dq.expectations(WEATHER_RULES)
def silver_weather_daily():
    return (spark.read.table("weather_filled")
            .withColumn("tmean", (F.col("tmax") + F.col("tmin")) / 2)
            .withColumn("hdd65", F.greatest(F.lit(0.0), 65 - F.col("tmean"))))


@dp.materialized_view(comment="NWS daily forecasts per region and issuance, with mean temperature and HDD65. "
                              "Every issuance is kept, so each run's forecast can be compared with what happened.")
@dq.expectations(WEATHER_RULES)
def weather_forecast():
    return (spark.read.table("bronze_weather_forecast")
            .select("region_id", "station", "grid", "issued_at", "fetched_at", "forecast_date", "tmax", "tmin")
            .dropDuplicates(["region_id", "issued_at", "forecast_date"])
            .withColumn("tmean", (F.col("tmax") + F.col("tmin")) / 2)
            .withColumn("hdd65", F.greatest(F.lit(0.0), 65 - F.col("tmean"))))
