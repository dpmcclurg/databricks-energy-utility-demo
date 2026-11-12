# Winter gas volume plan on Databricks

**A gas utility buys the wrong winter gas volume because its forecast is a twice-a-year spreadsheet.**

This repo is a working prototype of the fix for a fictional customer, Colorado Gas Utility: one governed,
automated volume forecast for every service region, built straight from the source system, with validation
rules FP&A owns, published to the trading desk, and checked in-season with Genie.

**Context.** The utility's BI runs on an on-prem Microsoft stack: a SQL Server warehouse loaded by SSIS, SSAS cubes,
and Excel. The central BI team can't keep up with requests, and FP&A doesn't trust the warehouse, so it rebuilds
the numbers in county spreadsheets. This prototype is the first data product of a move to Databricks. FP&A owns the
product, and the BI team governs the platform. It is a mock-up built before access to the utility's data, so the
customer counts and gas use come from a simulated source system calibrated to public EIA Colorado volumes. The
weather is real, and the forecasting method is checked on real statewide volumes.

> **Fictional data notice.** Colorado Gas Utility is fictional. Its gas volumes, customer counts, and firm
> transportation contracts are synthetic. Weather is real public data (NOAA GHCN-Daily observations and the
> National Weather Service forecast), and the statewide check uses real public EIA Colorado volumes. Nothing here
> describes a real utility or customer.

## What it shows

| Pillar | Today (spreadsheet process) | In this prototype |
|---|---|---|
| **Trusted data FP&A owns** | Volumes exported from source-system screens; no checks on source volumes | CDC from the source system; every record checked against rules in `cgu_fpa.dq_rules`; failures quarantined; totals reconciled to the source |
| **One automated forecast for every region** | A county workbook per team, refreshed once or twice a year; HDDs copied from the NOAA and weather.gov websites by hand | One HDD model of use per customer, fit the same way for all five regions; P10 / P50 / P90 from 30 historical winters; regional sign-off and versioned publish to the trading desk; the 1-in-30 design day against firm transportation; the method checked on real EIA Colorado volumes |
| **AI in-season check-ins** | No AI on the current platform | A re-forecast every run: actual to date, the NWS 7-day forecast, and historical winters for the rest; variance to date split into weather, customers, and other; a Genie Agent answers in plain language with the SQL shown |

## Architecture

```
NOAA GHCN-Daily (6 stations) ──┐  observations to yesterday
NWS gridpoint forecast ────────┤  7 days, one file per issuance
EIA Colorado gas volumes ──────┤  monthly, plus annual consumer counts
Gas measurement system (MySQL) ┤  simulated: Debezium-style CDC JSON + control totals
                               ▼
Lakeflow Job  cgu_winter_volume  (serverless, 10 tasks)
  setup ─┬ ingest_weather ─ simulate_source ─┬ pipeline ─┬ forecast ─ reforecast ─┬ serve ─ evidence
         └ ingest_eia ──────────────────────┘           └ state_backtest ────────┘
  Lakeflow Spark Declarative Pipeline: Auto Loader -> bronze; expectations built from cgu_fpa.dq_rules;
    quarantine; AUTO CDC (SCD1) -> silver; weather gap-fill; gold, reconciliation, DQ summary
Unity Catalog  <catalog>.cgu_landing | cgu_core | cgu_fpa | cgu_serving
  metric view cgu_fpa.gas_volume_metrics (certified)   model cgu_fpa.volume_forecaster@champion
Lakebase       Snapshot synced tables (plan, re-forecast, design day, DQ) + app write-back (sign-off, versions)
Databricks App Winter Volume Plan (Streamlit): plan, sign-off, publish, design day | season re-forecast + Genie
Genie Agent    Colorado Gas Utility - Winter Volume Plan
AI/BI          CGU Gas Volume Data Quality dashboard
Trading desk   opens cgu_fpa.plan_published with the Databricks Excel Add-in
```

## Results from the first two runs

Winter 2026-27, live. The plan was made on October 1, 2026 (training data through September 30). Run 1 is the
first extract from the source system, through November 4, the latest day every region's weather station had
reported. Run 2 is the next morning's scheduled incremental extract, through November 6. The job then runs every
morning at 6 AM Mountain. Full text evidence is in [`evidence/`](evidence/README.md).

- **Both runs succeeded end to end**, 10 of 10 tasks each, in about 13 and 8 minutes on serverless
  ([`evidence/job_runs.json`](evidence/job_runs.json)).
- **Data quality:** 18,452 source volume records checked; 29 defective records quarantined, one invalid customer
  count dropped, and 10 estimated reads flagged. Caught counts equal injected counts for every rule, and both
  extracts reconcile to the source control totals
  ([`injected_vs_caught.md`](evidence/batch_000/injected_vs_caught.md),
  [`reconciliation.md`](evidence/batch_001/reconciliation.md)).
- **Rules as data:** FP&A tightened `volume_plausible` from 2.0 to 1.8 Dth per customer per day between the runs
  with one SQL `UPDATE`. The second pipeline update used the new rule, and `DESCRIBE HISTORY` records the change
  ([`pipeline_dq.md`](evidence/batch_001/pipeline_dq.md), [`uc_governance.md`](evidence/batch_001/uc_governance.md)).
- **Plan:** season requirement P50 2.57 million Dth (P10 2.38 million, P90 2.77 million) across five regions,
  from 31 real winters of weather ([`forecast_season.md`](evidence/batch_000/forecast_season.md)).
- **Real-data check:** the same method on real EIA Colorado residential and commercial volumes, given the actual
  weather, has a monthly error of 5.1%, 4.0%, and 4.1% for winters 2023-24 to 2025-26 (4.4% overall)
  ([`state_backtest.md`](evidence/batch_000/state_backtest.md)).
- **Synthetic backtest:** given the actual weather, the new model's monthly error is 1.9% against 4.4% for the
  spreadsheet method. Forecasting with normal weather, the spreadsheet method comes out closer in all three
  held-out winters, because all three were warmer than normal and it plans on about 3% fewer customers than the
  winter had; in a cold winter those two errors add up ([`backtest.md`](evidence/batch_000/backtest.md)).
- **Design day:** Eastern Plains' 1-in-30 peak day needs 7,180 Dth against 6,800 Dth of firm transportation, 380
  Dth/day (5.3%) short after a subdivision joined on the November 1 meter count. The other regions have 5.8% to
  13.4% headroom ([`design_day.md`](evidence/batch_001/design_day.md)).
- **In-season (as of November 6):** every region is 8.6% to 16.5% below plan to date, almost all of it warmer
  weather, and the NWS forecast for the next 7 days is warmer than normal too. With six days of actuals the
  re-forecast is within 3% of the plan in every region, labeled early signal. The variance parts add up to actual
  minus plan in every region. The second extract deleted a duplicated Pueblo reading, and Pueblo's "other" variance
  fell from 1,418 to 235 Dth. The P10 to P90 range narrowed in four of five regions and widened 3% in Park and
  Foothills ([`reforecast_asof_2026-11-06.md`](evidence/batch_001/reforecast_asof_2026-11-06.md)).
- **Genie:** five questions (CFO, FP&A analyst, trading desk, CIO) with generated SQL and result rows
  ([`genie/transcript.md`](genie/transcript.md)).
- **App:** a user signed off all five regions and published version 1 to the trading desk; the version is in
  Lakebase and in `cgu_fpa.plan_published` ([`app_smoke.md`](evidence/app_smoke.md)).

## Repo layout

```
databricks.yml              bundle: variables and the dev target
resources/                  job, pipeline, app, dashboard, MLflow experiment
src/notebooks/              00_setup, 01_ingest_weather, 02_ingest_eia, 03_simulate_source, 04_forecast,
                            05_state_backtest, 06_reforecast, 07_serve, 08_evidence
src/pipeline/               transformations/ (bronze, silver, gold) and utilities/dq.py (rules as data)
src/metric_views/           gas_volume_metrics.yaml
src/app/                    Streamlit app
genie/                      agent.template.json (source), agent.json (export), transcript.md
dashboards/                 data_quality.lvdash.json
scripts/                    genie_agent.py, grant_app.py, reset_serving.py, collect_evidence.py, genie_transcript.py,
                            app_evidence.py, scrub_check.sh
evidence/                   text evidence from the two runs
```

## How to run it

**Prerequisites:** a Databricks workspace with serverless compute, a SQL warehouse, a Lakebase Autoscaling
project, and Databricks Apps; Databricks CLI 1.20 or later (bundles deploy with the direct engine, no
Terraform); Python 3.11 or later with `databricks-sdk` and `psycopg[binary]` for the scripts.

1. Authenticate: `databricks auth login --host <workspace-url> --profile <profile>`, and set
   `workspace.profile` in `databricks.yml`.
2. Create `.databricks/bundle/dev/variable-overrides.json` (it is git-ignored):
   `{"catalog": "<catalog>", "warehouse_id": "<warehouse-id>"}`. Set `lakebase_project` there too if your
   project is not `cgu-fpa-gas`.
3. Deploy and run the first extract: `databricks bundle deploy`, then `databricks bundle run cgu_winter_volume`.
   With no parameters the job plans winter 2026-27 (`season=2026`), takes the next extract number
   (`batch=auto`, `000` on a clean slate), and sets the as-of date to the latest day every region's NOAA station
   has reported (`as_of_date=auto`). To replay a finished winter instead:
   `--params season=2025,as_of_date=2025-11-20`. `python scripts/clean_slate.py --profile <profile> --yes`
   resets the workspace to empty first.
4. Create the Genie Agent: `python scripts/genie_agent.py --profile <profile>`. This writes `genie_space_id`
   to the overrides file.
5. Deploy the app with its Genie resource and grant its service principal access:
   `databricks bundle deploy`, `python scripts/grant_app.py --profile <profile>`, then
   `databricks bundle run cgu_volume_plan`.
6. Turn on the daily refresh: set `"schedule_pause_status": "UNPAUSED"` in the overrides file and
   `databricks bundle deploy`. The job then runs at 6 AM Mountain every morning, each run a new incremental
   extract with the latest NOAA days and NWS forecast.
7. In the app, sign off all five regions and publish. Then collect evidence:
   `python scripts/collect_evidence.py --profile <profile>`, `python scripts/genie_transcript.py --profile <profile>`,
   and `python scripts/app_evidence.py --profile <profile>`.
8. Before pushing: `scripts/scrub_check.sh`.

## Design notes

- **Production ingestion.** The source system is simulated. In production, Lakeflow Connect's MySQL
  connector reads the binlog directly; the bronze tables, expectations, and AUTO CDC flow stay the same.
- **Rules as data.** The pipeline reads `cgu_fpa.dq_rules` when each update starts and builds its expectations
  from it, so FP&A changes validation with SQL, not code, and every change is in the table history.
- **Use per customer.** The model predicts gas per customer per day from the weather, then multiplies by the
  meter count, so customer growth is a separate, visible driver rather than something hidden in the weather fit.
- **Three-part variance.** Weather = the model on the actual weather with the planned customers, minus the plan.
  Customers = the model with the actual customers, minus the same with the planned customers. Other = actual
  minus the model with both: occupancy, behavior, data corrections. The three add up to actual minus plan.
- **Re-forecast.** Each run rebuilds the season as actual to date, plus the NWS forecast days, plus every
  historical winter for the remaining days. Real days replace forecast days, so the range narrows.
- **Design day.** A GEV fit of each station's annual coldest day gives the 1-in-30 design HDD (the planning
  standard; 1-in-50 shown too). Season HDD sizes the gas to buy; the design day sizes the firm pipeline capacity.
- **Backtest, read the right way.** Given the weather that happened, the backtest measures the method. Forecasting
  in October with normal weather, it mostly measures how far that winter was from normal. The spreadsheet-style
  baseline fits five years of volume, so it plans on fewer customers than the winter has; in a warm winter that
  low bias offsets the weather miss, and in a cold one the two add up. Both comparisons are reported, with each
  winter's weather and customer growth. Which weather normal to plan on (every earlier winter, a recent window,
  or a trended normal) is a planning assumption FP&A sets, like the data rules.
- **Synthetic regions, real check.** The five regions' volumes are synthetic, so their backtest checks the code.
  `05_state_backtest` runs the same method on real EIA Colorado volumes with a customer-weighted HDD (station
  weights in `cgu_fpa.ref_weather_station`, an assumption about where Colorado's gas customers live).
- **Serving.** The app reads Lakebase synced tables (Snapshot mode, no change data feed needed) and writes
  sign-offs and published versions to Lakebase. Each publish is also appended to `cgu_fpa.plan_published` in
  Unity Catalog, which is what the trading desk opens with the Excel Add-in.
- **Cloud.** The prototype runs on Databricks on AWS. The customer runs Azure; the same bundle deploys to
  Azure Databricks.

## How this was built

Built with Claude Code (Claude Opus) working from a written spec, with the Databricks agent skills for
bundles, pipelines, Lakebase, apps, Genie, and dashboards. [`AGENTS.md`](AGENTS.md) holds the build and run
instructions an AI assistant or a partner team follows to extend it. Claude Code session ID:
`9b98a691-c445-4be5-b1c4-af1301dd13a6`.

## Data sources

- NOAA National Centers for Environmental Information, Global Historical Climatology Network - Daily
  (GHCN-Daily), stations USC00052281 (Dillon 1 E), USC00054742 (Lake George 8 SW), USC00053530 (Grant),
  USW00093058 (Pueblo), USW00003017 (Denver International), and USW00093037 (Colorado Springs), retrieved through
  the NCEI Access Data Service. Public domain.
- National Weather Service API (api.weather.gov), gridpoint forecasts for each station's location. Public domain.
- U.S. Energy Information Administration, Natural Gas Consumption by End Use and Number of Consumers for
  Colorado: series N3010CO2, N3020CO2, NA1501_SCO_8, NA1531_SCO_8. Public domain.
