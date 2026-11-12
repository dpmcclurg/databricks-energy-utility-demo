"""Write evidence/app_smoke.md: app status and resources, sign-off and publish log lines, and the published
plan as the trading desk sees it in Unity Catalog.

Usage: python scripts/app_evidence.py --profile <profile> [--target dev]
Run after signing off all regions and publishing once in the deployed app.
"""
import argparse
import json
import pathlib
import re
import subprocess

from databricks.sdk import WorkspaceClient

ROOT = pathlib.Path(__file__).resolve().parents[1]
ap = argparse.ArgumentParser()
ap.add_argument("--profile", required=True)
ap.add_argument("--target", default="dev")
ap.add_argument("--app", default="cgu-winter-volume-plan")
args = ap.parse_args()

ov = json.loads((ROOT / ".databricks" / "bundle" / args.target / "variable-overrides.json").read_text())
catalog, warehouse = ov["catalog"], ov["warehouse_id"]
w = WorkspaceClient(profile=args.profile)
app = w.apps.get(args.app)

logs = subprocess.run(["databricks", "apps", "logs", args.app, "-p", args.profile, "--tail-lines", "2000"],
                      capture_output=True, text=True).stdout.splitlines()
keep = [l for l in logs if re.search(r"\[APP\] (SIGNOFF|PUBLISH)|\[BUILD\].*(Starting app|Deployment successful)", l)]

r = w.statement_execution.execute_statement(
    warehouse_id=warehouse, wait_timeout="50s",
    statement=f"SELECT plan_version, region_id, p10, p50, p90, model_version, published_at "
              f"FROM `{catalog}`.cgu_fpa.plan_published WHERE period = 'Nov-Mar' ORDER BY plan_version, p50 DESC")
cols = [c.name for c in r.manifest.schema.columns]
rows = r.result.data_array or []

out = [
    "# App smoke test", "",
    "The deployed Winter Volume Plan app (Streamlit on Databricks Apps), exercised by a user: all five regions "
    "signed off, then the plan published to the trading desk. Written by `scripts/app_evidence.py`.", "",
    "## App", "",
    f"- Name: `{app.name}`",
    f"- App status: `{app.app_status.state.value if app.app_status else 'unknown'}`, compute: "
    f"`{app.compute_status.state.value if app.compute_status else 'unknown'}`",
    f"- Active deployment: `{app.active_deployment.status.state.value if app.active_deployment else 'none'}`",
    "- Resources: " + ", ".join(f"`{res.name}`" for res in app.resources or []), "",
    "## Log lines: deployment, sign-off, publish", "", "```", *keep, "```", "",
    "Sign-offs and published versions are written to Lakebase (Postgres schema `app`, owned by the app's "
    "service principal). Each publish is also appended to `cgu_fpa.plan_published` in Unity Catalog.", "",
    "## Published plan in Unity Catalog (season totals, Dth)", "",
    "| " + " | ".join(cols) + " |", "|" + "---|" * len(cols),
    *["| " + " | ".join("" if v is None else str(v) for v in row) + " |" for row in rows], "",
]
text = "\n".join(out).replace(catalog, "<catalog>").replace(w.config.host.replace("https://", ""), "<workspace-host>")
text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,}", "<user>", text)
(ROOT / "evidence" / "app_smoke.md").write_text(text + "\n")
print(text)
