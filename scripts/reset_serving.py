"""Remove the Lakebase synced tables so the serve task recreates them from the current gold tables.

Usage: python scripts/reset_serving.py --profile <profile> [--target dev]

Deletes each synced table in <catalog>.cgu_serving (the Unity Catalog entry and its sync pipeline), then
drops the Postgres copies in schema cgu_serving. Needed when a source table's columns change, and as part
of a clean slate. The app's own schema (app) is left alone.
"""
import argparse
import json
import pathlib
import time

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

ROOT = pathlib.Path(__file__).resolve().parents[1]
TABLES = ["forecast_season", "season_tracker_summary", "season_tracker", "season_reforecast", "design_day",
          "gold_dq_summary", "gold_reconciliation", "backtest_results"]
ap = argparse.ArgumentParser()
ap.add_argument("--profile", required=True)
ap.add_argument("--target", default="dev")
ap.add_argument("--endpoint", default="projects/cgu-fpa-gas/branches/production/endpoints/primary")
args = ap.parse_args()

catalog = json.loads((ROOT / ".databricks" / "bundle" / args.target / "variable-overrides.json").read_text())["catalog"]
w = WorkspaceClient(profile=args.profile)
for t in TABLES:
    name = f"synced_tables/{catalog}.cgu_serving.{t}"
    try:
        w.postgres.delete_synced_table(name=name)
        while True:
            try:
                w.postgres.get_synced_table(name=name)
                time.sleep(5)
            except NotFound:
                break
        print(f"deleted synced table cgu_serving.{t}")
    except NotFound:
        print(f"no synced table cgu_serving.{t}")

ep = w.postgres.get_endpoint(name=args.endpoint)
token = w.postgres.generate_database_credential(endpoint=args.endpoint).token
with psycopg.connect(host=ep.status.hosts.host, dbname="databricks_postgres", user=w.current_user.me().user_name,
                     password=token, sslmode="require", autocommit=True) as conn:
    for t in TABLES:
        conn.execute(f'DROP TABLE IF EXISTS cgu_serving."{t}"')
        print(f"dropped Postgres table cgu_serving.{t}")
