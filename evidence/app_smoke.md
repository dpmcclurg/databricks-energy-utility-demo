# App smoke test

The deployed Winter Volume Plan app (Streamlit on Databricks Apps), exercised by a user: all five regions signed off, then the plan published to the trading desk. Written by `scripts/app_evidence.py`.

## App

- Name: `cgu-winter-volume-plan`
- App status: `RUNNING`, compute: `ACTIVE`
- Active deployment: `SUCCEEDED`
- Resources: `sql-warehouse`, `postgres`, `genie`

## Log lines: deployment, sign-off, publish

```
2026-11-11T09:21:27Z [BUILD] [INFO] Starting app with command: [streamlit run app.py]
2026-11-11T09:21:29Z [BUILD] [INFO] Deployment successful
2026-11-12T18:27:43Z [APP] SIGNOFF season=2026-27 region=summit forecast_version=1
2026-11-12T18:27:44Z [APP] SIGNOFF season=2026-27 region=teller forecast_version=1
2026-11-12T18:27:45Z [APP] SIGNOFF season=2026-27 region=park_foothills forecast_version=1
2026-11-12T18:27:47Z [APP] SIGNOFF season=2026-27 region=pueblo forecast_version=1
2026-11-12T18:27:49Z [APP] SIGNOFF season=2026-27 region=eastern_plains forecast_version=1
2026-11-12T18:28:07Z [APP] PUBLISH plan_version=1 season=2026-27 rows=30 season_p50=2,566,896 lakebase=app.plan_published uc=cgu_fpa.plan_published
```

Sign-offs and published versions are written to Lakebase (Postgres schema `app`, owned by the app's service principal). Each publish is also appended to `cgu_fpa.plan_published` in Unity Catalog.

## Published plan in Unity Catalog (season totals, Dth)

| plan_version | region_id | p10 | p50 | p90 | model_version | published_at |
|---|---|---|---|---|---|---|
| 1 | park_foothills | 655554.0 | 689997.0 | 739802.0 | 1 | 2026-11-12T18:27:56.000Z |
| 1 | teller | 588780.0 | 632633.0 | 690671.0 | 1 | 2026-11-12T18:27:56.000Z |
| 1 | pueblo | 487450.0 | 541645.0 | 588108.0 | 1 | 2026-11-12T18:27:56.000Z |
| 1 | eastern_plains | 338392.0 | 366627.0 | 396292.0 | 1 | 2026-11-12T18:27:56.000Z |
| 1 | summit | 313900.0 | 335994.0 | 353495.0 | 1 | 2026-11-12T18:27:56.000Z |

