"""Grant the app's service principal what it needs after `databricks bundle deploy`.

Usage: python scripts/grant_app.py --profile <profile> [--target dev]

- Unity Catalog: USE CATALOG, USE SCHEMA and SELECT on cgu_fpa (Genie answers run as the app), SELECT on
  cgu_core.weather_forecast (the NWS forecast Genie can query), MODIFY on cgu_fpa.plan_published (publish
  hand-off to the trading desk).
- Lakebase: USAGE and SELECT on the synced schema cgu_serving.
The SQL warehouse (CAN_USE), Genie Agent (CAN_RUN), and Lakebase connect/create permissions come
from the app's resources in resources/cgu_app.app.yml. Redeploy the app after the first grant.
"""
import argparse
import json
import pathlib

import psycopg
from databricks.sdk import WorkspaceClient

ROOT = pathlib.Path(__file__).resolve().parents[1]
ap = argparse.ArgumentParser()
ap.add_argument("--profile", required=True)
ap.add_argument("--target", default="dev")
ap.add_argument("--app", default="cgu-winter-volume-plan")
ap.add_argument("--endpoint", default="projects/cgu-fpa-gas/branches/production/endpoints/primary")
args = ap.parse_args()

ov = json.loads((ROOT / ".databricks" / "bundle" / args.target / "variable-overrides.json").read_text())
catalog, warehouse = ov["catalog"], ov["warehouse_id"]
w = WorkspaceClient(profile=args.profile)
sp = w.apps.get(args.app).service_principal_client_id

uc = [
    f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{sp}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA `{catalog}`.cgu_fpa TO `{sp}`",
    f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.cgu_core TO `{sp}`",
    f"GRANT SELECT ON TABLE `{catalog}`.cgu_core.weather_forecast TO `{sp}`",
    f"GRANT MODIFY ON TABLE `{catalog}`.cgu_fpa.plan_published TO `{sp}`",
]
for stmt in uc:
    r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=warehouse, wait_timeout="50s")
    print(r.status.state.value, stmt.replace(catalog, "<catalog>"))

ep = w.postgres.get_endpoint(name=args.endpoint)
token = w.postgres.generate_database_credential(endpoint=args.endpoint).token
me = w.current_user.me().user_name
with psycopg.connect(host=ep.status.hosts.host, dbname="databricks_postgres", user=me, password=token,
                     sslmode="require", autocommit=True) as conn:
    for stmt in [
        f'GRANT USAGE ON SCHEMA cgu_serving TO "{sp}"',
        f'GRANT SELECT ON ALL TABLES IN SCHEMA cgu_serving TO "{sp}"',
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA cgu_serving GRANT SELECT ON TABLES TO "{sp}"',
    ]:
        conn.execute(stmt)
        print("OK", stmt.replace(sp, "<app service principal>"))
