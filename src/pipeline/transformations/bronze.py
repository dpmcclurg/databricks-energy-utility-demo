"""Bronze: raw CDC events and control totals from the gas measurement system, NOAA observations,
NWS forecasts, and EIA Colorado volumes.

Auto Loader picks up each new extract file once. Nothing is filtered here, so bronze is the
complete record of what the source sent.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("cgu.catalog")
CDC = f"/Volumes/{CATALOG}/cgu_landing/cdc"
NOAA = f"/Volumes/{CATALOG}/cgu_landing/noaa"
NWS = f"/Volumes/{CATALOG}/cgu_landing/nws"
EIA = f"/Volumes/{CATALOG}/cgu_landing/eia"

SOURCE = "source STRUCT<connector STRING, db STRING, `table` STRING, file STRING, pos BIGINT, ts_ms BIGINT>"
VOLUME_ROW = ("STRUCT<volume_id BIGINT, region_id STRING, gas_day DATE, volume DOUBLE, unit STRING, "
              "read_status STRING, updated_at TIMESTAMP>")
REGION_ROW = "STRUCT<region_id STRING, name STRING, customers INT, updated_at TIMESTAMP>"
CUSTOMER_ROW = "STRUCT<region_id STRING, month DATE, residential INT, commercial INT, updated_at TIMESTAMP>"


def cdc_stream(table: str, row_type: str):
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(f"op STRING, before {row_type}, after {row_type}, ts_ms BIGINT, {SOURCE}, _rescued_data STRING")
            .load(f"{CDC}/gms.{table}")
            .withColumn("_batch", F.regexp_extract("_metadata.file_path", r"batch_(\d+)", 1))
            .withColumn("_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp()))


@dp.table(comment="Raw CDC events for gms.daily_volume (MySQL binlog via the CDC extract), one row per change event.")
def bronze_daily_volume_cdc():
    return cdc_stream("daily_volume", VOLUME_ROW)


@dp.table(comment="Raw CDC events for gms.region (service regions and customer counts).")
def bronze_region_cdc():
    return cdc_stream("region", REGION_ROW)


@dp.table(comment="Raw CDC events for gms.customer_count (monthly residential and commercial meter counts per region).")
def bronze_customer_count_cdc():
    return cdc_stream("customer_count", CUSTOMER_ROW)


@dp.table(comment="Control totals reported by the source with each extract, used for reconciliation.")
def bronze_control():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("pathGlobFilter", "batch_*.json")
            .schema("batch STRING, extracted_at BIGINT, tables STRUCT<region: STRUCT<events: BIGINT>, "
                    "customer_count: STRUCT<events: BIGINT>, daily_volume: STRUCT<events: BIGINT, events_by_op: MAP<STRING, BIGINT>, sum_after_volume: DOUBLE>>")
            .load(f"{CDC}/_control")
            .withColumn("_file", F.col("_metadata.file_path")))


@dp.materialized_view(comment="NOAA GHCN-Daily TMAX and TMIN (F) per station; region_id is empty for the statewide reference station.")
def bronze_weather():
    return (spark.read.format("csv").option("header", True)
            .schema("region_id STRING, station STRING, station_name STRING, date DATE, tmax DOUBLE, tmin DOUBLE")
            .load(NOAA)
            .withColumn("region_id", F.expr("nullif(region_id, '')"))
            .withColumn("_file", F.col("_metadata.file_path")))


@dp.table(comment="NWS gridpoint forecasts as issued: daily high and low (F) per region, one file per issuance, append-only.")
def bronze_weather_forecast():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema("region_id STRING, station STRING, grid STRING, issued_at TIMESTAMP, fetched_at TIMESTAMP, "
                    "forecast_date DATE, tmax DOUBLE, tmin DOUBLE")
            .load(NWS)
            .withColumn("_file", F.col("_metadata.file_path")))


@dp.materialized_view(comment="EIA Colorado natural gas series as published: monthly volumes (MMcf) and annual consumer counts.")
def bronze_eia_series():
    return (spark.read.format("csv").option("header", True)
            .schema("series_id STRING, measure STRING, frequency STRING, period STRING, value DOUBLE, unit STRING, description STRING")
            .load(f"{EIA}/eia_series.csv"))
