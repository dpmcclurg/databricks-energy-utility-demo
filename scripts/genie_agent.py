"""Create or update the Genie Agent from genie/agent.template.json.

Usage: python scripts/genie_agent.py --profile <profile> [--target dev]

Reads the catalog and warehouse from the bundle's local variable overrides
(.databricks/bundle/<target>/variable-overrides.json), renders the template, creates the agent on
first run or updates it in place, and writes the agent ID back to the overrides as genie_space_id so
the app can reference it. Exports the live definition to genie/agent.json with the catalog name
replaced by a placeholder, so nothing workspace-specific is committed.
"""
import argparse
import json
import pathlib

from databricks.sdk import WorkspaceClient

ROOT = pathlib.Path(__file__).resolve().parents[1]

ap = argparse.ArgumentParser()
ap.add_argument("--profile", required=True)
ap.add_argument("--target", default="dev")
args = ap.parse_args()

overrides_path = ROOT / ".databricks" / "bundle" / args.target / "variable-overrides.json"
overrides = json.loads(overrides_path.read_text())
catalog, warehouse = overrides["catalog"], overrides["warehouse_id"]

tpl = json.loads((ROOT / "genie" / "agent.template.json").read_text().replace("__CATALOG__", catalog))
w = WorkspaceClient(profile=args.profile)
body = dict(title=tpl["title"], description=tpl["description"], warehouse_id=warehouse,
            serialized_space=json.dumps(tpl["serialized_space"]))

space_id = overrides.get("genie_space_id")
if space_id and space_id != "none":
    w.genie.update_space(space_id, **body)
    print(f"updated Genie Agent {space_id}")
else:
    space_id = w.genie.create_space(**body).space_id
    overrides["genie_space_id"] = space_id
    overrides_path.write_text(json.dumps(overrides, indent=2))
    print(f"created Genie Agent {space_id}")

live = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}", query={"include_serialized_space": "true"})
export = {"title": live.get("title"), "description": live.get("description"),
          "serialized_space": json.loads(live["serialized_space"])}
(ROOT / "genie" / "agent.json").write_text(json.dumps(export, indent=2).replace(catalog, "__CATALOG__") + "\n")
print("exported genie/agent.json")
