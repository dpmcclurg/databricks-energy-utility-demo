"""Copy run evidence into evidence/ and scrub workspace-specific values.

Usage: python scripts/collect_evidence.py --profile <profile> [--runs 2] [--latest] [--season 2026]

- evidence/job_runs.json: N complete runs of the cgu_winter_volume job for the season (the first N by
  default, the most recent N with --latest), with task states, durations, and each task's output (the
  notebook exit value). Runs where any task was skipped or failed are left out.
- evidence/batch_<n>/*.md: text written by the evidence task of those runs; other batch folders are removed.
Catalog names, hosts, workspace IDs, and email addresses are replaced with placeholders.
"""
import argparse
import json
import pathlib
import re
import shutil

from databricks.sdk import WorkspaceClient

ROOT = pathlib.Path(__file__).resolve().parents[1]
ap = argparse.ArgumentParser()
ap.add_argument("--profile", required=True)
ap.add_argument("--target", default="dev")
ap.add_argument("--runs", type=int, default=2)
ap.add_argument("--latest", action="store_true", help="take the most recent runs instead of the season's first")
ap.add_argument("--season", default="2026")
args = ap.parse_args()

ov = json.loads((ROOT / ".databricks" / "bundle" / args.target / "variable-overrides.json").read_text())
catalog = ov["catalog"]
w = WorkspaceClient(profile=args.profile)
host = w.config.host.replace("https://", "")
me = w.current_user.me().user_name


def scrub(text: str) -> str:
    text = text.replace(catalog, "<catalog>").replace(host, "<workspace-host>")
    text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,}", "<user>", text)
    text = re.sub(r"\?o=\d+", "?o=<workspace-id>", text)
    text = re.sub(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "<uuid>", text)
    return text


job = next(j for j in w.jobs.list(name="cgu_winter_volume"))
full_runs = []  # complete runs of this season, newest first (skips partial and single-task reruns)
for r in w.jobs.list_runs(job_id=job.job_id, completed_only=True):
    run = w.jobs.get_run(r.run_id)
    params = {p.name: p.value or p.default for p in (run.job_parameters or [])}
    if params.get("season") == args.season and all(
            t.state.result_state and t.state.result_state.value == "SUCCESS" for t in run.tasks):
        full_runs.append(run)
full_runs = full_runs[:args.runs] if args.latest else full_runs[-args.runs:]

runs = []
for run in full_runs[::-1]:
    params = {p.name: p.value or p.default for p in (run.job_parameters or [])}
    tasks = []
    for t in sorted(run.tasks, key=lambda t: t.start_time or 0):
        out = None
        if t.notebook_task:
            o = w.jobs.get_run_output(t.run_id)
            out = o.notebook_output.result if o.notebook_output else None
            try:
                out = json.loads(out)
            except (TypeError, ValueError):
                pass
        tasks.append({"task": t.task_key, "result": t.state.result_state.value if t.state.result_state else None,
                      "duration_s": round((t.execution_duration or 0) / 1000), "output": out})
    runs.append({"run_id": run.run_id, "result": run.state.result_state.value,
                 "start_time_ms": run.start_time, "duration_s": round((run.run_duration or 0) / 1000),
                 "parameters": {k: v for k, v in params.items() if k in ("batch", "season", "as_of_date")}, "tasks": tasks})

(ROOT / "evidence").mkdir(exist_ok=True)
(ROOT / "evidence" / "job_runs.json").write_text(scrub(json.dumps({"job": "cgu_winter_volume", "runs": runs}, indent=2)) + "\n")
print(f"wrote evidence/job_runs.json ({len(runs)} runs)")

batches = [next(t["output"]["batch"] for t in r["tasks"] if t["task"] == "ingest_weather") for r in runs]
for old in (ROOT / "evidence").glob("batch_*"):
    shutil.rmtree(old)  # only the selected runs' evidence is kept
base = f"/Volumes/{catalog}/cgu_landing/evidence"
for batch in batches:
    d = f"{base}/batch_{batch}"
    local = ROOT / "evidence" / f"batch_{batch}"
    local.mkdir()
    for f in w.files.list_directory_contents(d):
        text = w.files.download(f.path).contents.read().decode()
        (local / pathlib.Path(f.path).name).write_text(scrub(text))
        print(f"wrote evidence/{local.name}/{pathlib.Path(f.path).name}")
