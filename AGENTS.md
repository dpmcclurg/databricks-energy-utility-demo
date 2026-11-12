# AGENTS.md

Instructions for an AI coding assistant, or a partner team, working in this repo.

## What this is

A Declarative Automation Bundle for a fictional gas utility's winter volume plan. One Lakeflow Job
(`cgu_winter_volume`) runs the whole flow on serverless compute; a Streamlit Databricks App
(`cgu-winter-volume-plan`) and a Genie Agent sit on top. Read `README.md` first.

## Ground rules

- **Never commit workspace-specific values.** Catalog names, warehouse IDs, hosts, workspace IDs, emails, and
  tokens live only in `.databricks/bundle/<target>/variable-overrides.json` (git-ignored). Code reads them
  from bundle variables, job parameters, or app environment variables. Run `scripts/scrub_check.sh` before
  every push.
- **Data stays fictional.** Volumes, customers, and firm transportation contracts are synthetic; weather (NOAA,
  NWS) and the statewide check (EIA) are public data. Do not add real customer or utility data.
- **FP&A owns the rules.** Validation logic belongs in rows of `cgu_fpa.dq_rules`, not in pipeline code. The
  pipeline only knows how to turn rows into expectations (`src/pipeline/utilities/dq.py`).
- **Evidence is text.** Anything a run proves should end up in `evidence/` as markdown or JSON, written by
  `src/notebooks/08_evidence.py` and copied by `scripts/collect_evidence.py`.
- **The plan is fixed once made.** `04_forecast` skips refitting when the champion model was already fit for
  the season's forecast date, so later runs track against the plan the trading desk received.

## Layout and naming

- Schemas live inside one existing catalog: `cgu_landing` (volumes: cdc, noaa, nws, eia, evidence), `cgu_core`
  (bronze, silver, quarantine, NWS forecasts, run context, pipeline event log), `cgu_fpa` (rules, reference data,
  gold, metric view, model, plan, re-forecast, design day, published plan), `cgu_serving` (Lakebase synced tables).
- Notebooks are Databricks source files (`# Databricks notebook source`) so diffs stay readable. Each one
  ends with `dbutils.notebook.exit(json)` summarizing what it did; that value lands in `evidence/job_runs.json`.
- Job parameters: `batch` (`auto` = the next extract number, `000` on a clean slate; each later run is an
  increment), `season` (2026 = winter 2026-27), `as_of_date` (`auto` = the latest day every region's NOAA
  station has reported, or a fixed date). `01_ingest_weather` resolves batch, as-of date, and extract date into
  `cgu_core.run_context`; every later task reads the latest row from there.
- The job is scheduled daily at 6 AM Mountain; `schedule_pause_status` (bundle variable, default `PAUSED`) turns
  it on. A failed run emails the deploying user.
- Pipeline code uses `from pyspark import pipelines as dp`. Gold tables are written to `cgu_fpa` with
  fully-qualified names; everything else defaults to `cgu_core`.

## Build and run loop

```bash
databricks bundle validate --strict
databricks bundle deploy
python scripts/clean_slate.py --profile <profile> --yes  # empty workspace (destructive)
databricks bundle run cgu_winter_volume                   # next extract, season 2026, as of auto
databricks bundle run cgu_winter_volume --params season=2025,as_of_date=2025-11-20  # replay last winter
databricks bundle run cgu_winter_volume --only reforecast,serve                     # rerun stages on the latest extract
python scripts/genie_agent.py --profile <profile>        # create or update the Genie Agent
python scripts/grant_app.py --profile <profile>          # app service principal grants (UC + Lakebase)
python scripts/reset_serving.py --profile <profile>      # drop synced tables after a gold table's columns change
databricks bundle run cgu_volume_plan                    # redeploy the app
```

Test app changes locally with Streamlit's `AppTest` before deploying (Apps enforce OAuth, so a headless
browser cannot reach the deployed UI). Set `APP_SCHEMA` to a schema your own Postgres role owns so local
writes do not take ownership of the app's `app` schema.

## Known platform constraints (FEVM workspaces)

- No `CREATE CATALOG`: everything lives in schemas of the workspace catalog, and the Lakebase database is not
  registered as a UC catalog. Synced tables go to `<catalog>.cgu_serving`; the published plan is appended to
  `cgu_fpa.plan_published` by the app.
- Governed tag policies restrict values: `domain` accepts `finance`; certification uses
  `system.certification_status = certified`.
- The app's service principal is not in the `users` group: it gets warehouse `CAN_USE`, Genie `CAN_RUN`, and
  Lakebase `CAN_CONNECT_AND_CREATE` through app resources, and UC and Postgres grants from
  `scripts/grant_app.py`.
- Lakebase synced tables keep the columns they were created with. After changing a gold table's columns, run
  `scripts/reset_serving.py`, then the `serve` task. Postgres work runs from the scripts, not notebooks:
  importing the `psycopg` binary wheel in a serverless notebook task ended the Python process (SIGABRT).
- NOAA co-op stations report two to five days late, so `as_of_date=auto` trails today. NWS requests need a
  `User-Agent` header.

## Extending

- **New region:** add it to the source simulator, `cgu_fpa.ref_weather_station`, and `cgu_fpa.firm_capacity`. The
  model, re-forecast, design day, app, and Genie pick it up without code changes.
- **New rule:** `INSERT INTO cgu_fpa.dq_rules ...` and rerun the pipeline.
- **Real source:** replace `03_simulate_source` with a Lakeflow Connect MySQL ingestion pipeline landing the
  same change events; keep bronze onward unchanged.
