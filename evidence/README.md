# Execution evidence

Text records from the first two end-to-end runs of the `cgu_winter_volume` job for winter 2026-27 on a
Databricks serverless workspace. After the first run the job runs every morning on a schedule; these are the
season's first snapshot and first increment. Files in `batch_*` are written by the job's own `evidence` task from the tables it just
built (`src/notebooks/08_evidence.py`); `scripts/collect_evidence.py` copies them here and replaces workspace
names, hosts, and emails with placeholders.

| Run | Parameters | What it simulates |
|---|---|---|
| 1 | `batch=auto` (resolves to `000`), `season=2026`, `as_of_date=auto` | Initial snapshot from the gas measurement system: all history from October 2016 through the latest day every region's NOAA station has reported |
| 2 | `batch=auto` (resolves to `001`), `season=2026`, `as_of_date=auto` | The next incremental extract: new days, the previous extract's estimated reads turned actual, one duplicated row deleted. FP&A tightened one rule before this run. The plan made on October 1 is unchanged. |

| File | What it proves |
|---|---|
| [`job_runs.json`](job_runs.json) | Both runs: every task's state, duration, and output summary (as-of date, stations, NWS issuance, EIA downloads, injected defects, model, season plan, backtests, re-forecast, design day, synced tables) |
| `batch_*/nws_forecast.md` | The as-of date each run used, and the NWS forecast it used |
| `batch_*/pipeline_dq.md` | The FP&A rules in force, expectation results from the pipeline event log, and rows checked and failed per rule and extract |
| `batch_*/injected_vs_caught.md` | Caught defects equal injected defects, per rule |
| `batch_*/reconciliation.md` | Every extract balances to the source's control totals |
| `batch_*/forecast_season.md` | Model parameters per region and the P10 / P50 / P90 plan by month |
| `batch_*/backtest.md` | Spreadsheet-style baseline against the new model on the synthetic regions, three held-out winters: given the actual weather and with normal weather, plus each winter's weather against normal |
| `batch_*/state_backtest.md` | The same method on real EIA Colorado volumes, three held-out winters, and Colorado winter totals |
| `batch_*/reforecast_asof_*.md` | Variance to date in three parts (they add up), the re-forecast against the plan, the next 7 days, and every run so far |
| `batch_*/design_day.md` | The 1-in-30 and 1-in-50 design day per region against firm transportation (fictional contracts) |
| `batch_*/uc_governance.md` | Grants, tags, comments, lineage from `system.access.table_lineage`, and the `dq_rules` change history |
| `batch_*/lakebase_reads.md` | Rows read back from the Lakebase synced tables, and published plan versions |
| [`app_smoke.md`](app_smoke.md) | The deployed app: five regional sign-offs and one publish, from the app logs, and the published plan read back from Unity Catalog |
| [`../genie/transcript.md`](../genie/transcript.md) | Genie Agent questions with generated SQL and result rows |
