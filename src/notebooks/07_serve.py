# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Serve: metric view and Lakebase
# MAGIC
# MAGIC - Publishes the `cgu_fpa.gas_volume_metrics` metric view, the governed definition of the volume
# MAGIC   measures that Genie, dashboards, and the Excel Add-in all share.
# MAGIC - Keeps Snapshot synced tables of the plan, re-forecast, design day, and data quality summary in
# MAGIC   Lakebase for low-latency reads by the app.
# MAGIC - Ensures the governed hand-off table for the trading desk, `cgu_fpa.plan_published`, exists.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.150"
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("lakebase_project", "cgu-fpa-gas")
dbutils.widgets.text("lakebase_branch", "production")
CATALOG = dbutils.widgets.get("catalog")
PROJECT = dbutils.widgets.get("lakebase_project")
BRANCH = f"projects/{PROJECT}/branches/{dbutils.widgets.get('lakebase_branch')}"
PG_DATABASE = "databricks_postgres"
assert CATALOG
FPA = f"`{CATALOG}`.cgu_fpa"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric view

# COMMAND ----------

yaml_text = open("../metric_views/gas_volume_metrics.yaml").read().replace("__CATALOG__", CATALOG)
spark.sql(f"CREATE OR REPLACE VIEW {FPA}.gas_volume_metrics WITH METRICS LANGUAGE YAML AS $$\n{yaml_text}\n$$")
spark.sql(f"ALTER VIEW {FPA}.gas_volume_metrics SET TAGS ('domain' = 'finance', 'system.certification_status' = 'certified')")
display(spark.sql(f"""
  SELECT `Gas Season`, `Region`, MEASURE(`Total Volume Dth`) AS dth, MEASURE(`Dth per HDD`) AS dth_per_hdd
  FROM {FPA}.gas_volume_metrics WHERE `Gas Season` <> 'Off season' 
  GROUP BY ALL ORDER BY 1, 2"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase: Snapshot synced tables
# MAGIC Each synced table is a managed copy of a gold table in Lakebase Postgres (schema `cgu_serving`),
# MAGIC with a read-only entry in Unity Catalog at `<catalog>.cgu_serving.<table>` for lineage.

# COMMAND ----------

import json
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import postgres as pg

w = WorkspaceClient()
SERVING = "cgu_serving"

SYNCS = {
    "forecast_season": ["region_id", "period"],
    "season_tracker_summary": ["region_id"],
    "season_tracker": ["region_id", "gas_day"],
    "season_reforecast": ["region_id", "batch"],
    "design_day": ["region_id"],
    "gold_dq_summary": ["rule_name", "batch"],
    "gold_reconciliation": ["batch"],
    "backtest_results": ["season", "region_id"],
}


def sync(table: str, pk: list) -> dict:
    """Create the synced table on first run; on later runs start a new snapshot of it."""
    target = f"{CATALOG}.{SERVING}.{table}"
    source_cols = spark.table(f"{FPA}.{table}").columns
    try:
        st = w.postgres.get_synced_table(name=f"synced_tables/{target}")
        if spark.catalog.tableExists(target) and spark.table(target).columns != source_cols:
            raise RuntimeError(f"{table}: source columns changed since the synced table was created; "
                               "run scripts/reset_serving.py, then rerun this task")
        update = w.pipelines.start_update(pipeline_id=st.status.pipeline_id, full_refresh=True)
        return {"table": target, "action": "refreshed", "pipeline_id": st.status.pipeline_id, "update_id": update.update_id}
    except NotFound:
        w.postgres.create_synced_table(
            synced_table_id=target,
            synced_table=pg.SyncedTable(spec=pg.SyncedTableSyncedTableSpec(
                source_table_full_name=f"{CATALOG}.cgu_fpa.{table}",
                primary_key_columns=pk,
                scheduling_policy=pg.SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy.SNAPSHOT,
                branch=BRANCH, postgres_database=PG_DATABASE, create_database_objects_if_missing=True,
                new_pipeline_spec=pg.NewPipelineSpec(storage_catalog=CATALOG, storage_schema="cgu_core"),
            )),
        ).wait()
        deadline = time.time() + 1200  # the first snapshot runs after the create returns
        while time.time() < deadline:
            state = str(w.postgres.get_synced_table(name=f"synced_tables/{target}").status.detailed_state)
            if "ONLINE" in state or "FAILED" in state:
                break
            time.sleep(15)
        return {"table": target, "action": "created", "state": "COMPLETED" if "ONLINE" in state else state}


results = [sync(t, pk) for t, pk in SYNCS.items()]

# Wait for every refresh to finish its snapshot
deadline = time.time() + 1200
for r in results:
    while "update_id" in r and "state" not in r and time.time() < deadline:
        u = w.pipelines.get_update(pipeline_id=r["pipeline_id"], update_id=r["update_id"]).update
        if u.state.value in ("COMPLETED", "FAILED", "CANCELED"):
            r["state"] = u.state.value
        else:
            time.sleep(10)
    r.pop("pipeline_id", None); r.pop("update_id", None)
    print(r)
assert all(r.get("state") == "COMPLETED" for r in results), "a sync did not complete"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Published plan
# MAGIC The app writes sign-offs and published versions to Lakebase (schema `app`, owned by the app's
# MAGIC service principal). Each publish is also appended to `cgu_fpa.plan_published` in Unity Catalog,
# MAGIC the governed table the trading desk opens with the Excel Add-in.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FPA}.plan_published (
  plan_version INT, season STRING, region_id STRING, period STRING, p10 DOUBLE, p50 DOUBLE, p90 DOUBLE,
  model_version STRING, published_by STRING, published_at TIMESTAMP)
COMMENT 'Winter volume requirement published to the trading desk (Dth). Append-only: each publish is a new plan_version.'
""")
published = spark.sql(f"SELECT plan_version, MAX(published_at) published_at, COUNT(*) rows FROM {FPA}.plan_published GROUP BY 1 ORDER BY 1").toPandas()
print(published.to_string() if len(published) else "no published versions yet")

dbutils.notebook.exit(json.dumps({"metric_view": "cgu_fpa.gas_volume_metrics",
                                  "synced_tables": [dict(r, table=r["table"].split(".", 1)[1]) for r in results],
                                  "plan_published_versions": published.to_dict("records")}, default=str))
