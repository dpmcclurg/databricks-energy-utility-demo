"""Reset the workspace to an empty demo before the first run of a season. Destructive.

Usage: python scripts/clean_slate.py --profile <profile> [--target dev] --yes

1. Deletes the registered model cgu_fpa.volume_forecaster (all versions).
2. Drops the tables the notebooks write in cgu_fpa and cgu_core (plan, re-forecast, design day, backtests,
   rules, reference data, run context, published plan) and the metric view. Pipeline-owned tables are left to
   step 5.
3. Deletes the CDC extracts, NWS forecasts, and evidence in the landing volumes, leaving empty CDC folders.
   NOAA and EIA files are kept (each run refreshes them).
4. Runs the job's setup task, which recreates the schemas, rules, and reference tables.
5. Runs a full refresh of the pipeline over the empty CDC folders, so the next job run starts from nothing.
6. Removes the Lakebase synced tables (scripts/reset_serving.py); the serve task recreates them.
The app's own Lakebase schema (sign-offs, published versions) belongs to the app's service principal and is
not touched; the app shows only the current season's rows.
"""
import argparse
import json
import pathlib
import subprocess
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

ROOT = pathlib.Path(__file__).resolve().parents[1]
FPA_TABLES = ["forecast_season", "forecast_daily", "model_params", "backtest_results", "state_backtest", "season_tracker",
              "season_tracker_summary", "season_tracker_history", "season_reforecast", "design_day", "plan_published",
              "dq_rules", "ref_holiday_calendar", "ref_weather_station", "firm_capacity"]
CORE_TABLES = ["run_context"]
CDC_FOLDERS = ["gms.region", "gms.customer_count", "gms.daily_volume", "_control"]

ap = argparse.ArgumentParser()
ap.add_argument("--profile", required=True)
ap.add_argument("--target", default="dev")
ap.add_argument("--yes", action="store_true", help="confirm the reset")
args = ap.parse_args()
if not args.yes:
    sys.exit("This drops demo tables, the registered model, and landing files. Rerun with --yes to confirm.")

ov = json.loads((ROOT / ".databricks" / "bundle" / args.target / "variable-overrides.json").read_text())
catalog, warehouse = ov["catalog"], ov["warehouse_id"]
w = WorkspaceClient(profile=args.profile)


def sql(stmt: str) -> None:
    r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=warehouse, wait_timeout="50s")
    print(r.status.state.value, stmt.replace(catalog, "<catalog>"))


model = f"{catalog}.cgu_fpa.volume_forecaster"
try:
    for alias in w.registered_models.get(model, include_aliases=True).aliases or []:
        w.registered_models.delete_alias(model, alias.alias_name)
    for v in list(w.model_versions.list(model)):
        w.model_versions.delete(model, v.version)
    w.registered_models.delete(model)
    print("deleted registered model cgu_fpa.volume_forecaster and its versions")
except NotFound:
    print("no registered model")

sql(f"DROP VIEW IF EXISTS `{catalog}`.cgu_fpa.gas_volume_metrics")
for t in FPA_TABLES:
    sql(f"DROP TABLE IF EXISTS `{catalog}`.cgu_fpa.{t}")
for t in CORE_TABLES:
    sql(f"DROP TABLE IF EXISTS `{catalog}`.cgu_core.{t}")

landing = f"/Volumes/{catalog}/cgu_landing"


def clear(path: str) -> None:
    try:
        entries = list(w.files.list_directory_contents(path))
    except NotFound:
        return
    for e in entries:
        if e.is_directory:
            clear(e.path)
            w.files.delete_directory(e.path)
        else:
            w.files.delete(e.path)


for vol in ("cdc", "nws", "evidence"):
    clear(f"{landing}/{vol}")
    print(f"cleared {vol}")
for folder in CDC_FOLDERS:
    w.files.create_directory(f"{landing}/cdc/{folder}")

job = next(j for j in w.jobs.list(name="cgu_winter_volume"))
run = w.jobs.run_now_and_wait(job_id=job.job_id, only=["setup"])
print("setup", run.state.result_state.value)

pipeline_id = next(t.pipeline_task.pipeline_id for t in w.jobs.get(job.job_id).settings.tasks if t.pipeline_task)
update = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=True)
while True:
    u = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update.update_id).update
    if u.state.value in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(20)
print("pipeline full refresh", u.state.value)
assert u.state.value == "COMPLETED"

subprocess.run([sys.executable, "-I", str(ROOT / "scripts" / "reset_serving.py"), "--profile", args.profile,
                "--target", args.target], check=True)
print("clean slate complete")
