# UK AQ Station AQI AggDaily Cloud Run Setup

This service computes pollutant-specific DAQI + EAQI for stations and writes hourly + daily/monthly outputs into AggDaily DB.

## Runtime

- Service: `workers/uk_aq_aqi_station_aggdaily_cloud_run/run_service.ts`
- Job: `workers/uk_aq_aqi_station_aggdaily_cloud_run/run_job.ts`
- Deploy workflow: `.github/workflows/uk_aq_aqi_station_aggdaily_cloud_run_deploy.yml`

## Required GitHub Variables/Secrets

Required variables:

- `GCP_PROJECT_ID`
- `GCP_REGION`
- `GCP_ARTIFACT_REPO`
- `GCP_AQI_STATION_AGGDAILY_SERVICE_NAME`
- `GCP_AQI_STATION_AGGDAILY_SERVICE_ACCOUNT`
- `SUPABASE_URL`
- `AGGDAILY_SUPABASE_URL`

Required secrets:

- `SB_SECRET_KEY`
- `AGGDAILY_SECRET_KEY`

Google auth (choose one):

- WIF: `GCP_WORKLOAD_IDENTITY_PROVIDER` + `GCP_SERVICE_ACCOUNT`
- SA key: `GCP_SA_KEY`

## Default AQI Scheduling

When `GCP_AQI_STATION_AGGDAILY_SCHEDULER_ENABLED=true`, workflow manages 3 Scheduler jobs:

- fast: `*/5 * * * *` (`run_mode=fast`)
- short reconcile: `7 */2 * * *` (`run_mode=reconcile_short`)
- deep reconcile: `17 3 * * *` (`run_mode=reconcile_deep`)

All jobs call the same Cloud Run service URL with POST JSON payload.

## Trigger Behavior (What Each Mode Computes)

The service first calculates a **mature hour**:

- `mature_hour = floor_utc_hour(now_utc - UK_AQ_AQI_MATURITY_DELAY_HOURS)`
- default delay is 3 hours, so each run avoids very recent late-arriving data.

Then each mode computes:

- `fast`:
  - processes exactly one hour: `[mature_hour, mature_hour + 1h)`
  - used for near-real-time updates every 5 minutes.
- `reconcile_short`:
  - processes the last mature 36 hours (default):
  - `[mature_hour - 35h, mature_hour + 1h)`
  - catches normal late arrivals with low write churn.
- `reconcile_deep`:
  - processes the last mature 14 days (default):
  - `[mature_hour - (14d * 24h - 1h), mature_hour + 1h)`
  - catches slower/rare late data and provides daily deep reconciliation.

Internal source read window:

- each run reads source observations from `target_start - 23h` up to `target_end`
- this is required so PM DAQI rolling-24h means can be calculated at the first target hour.

Write/refresh behavior:

- hourly rows are idempotent upserts in `station_aqi_hourly`
- affected daily/monthly rollups are rebuilt idempotently
- each run logs telemetry into `uk_aq_ops.aqi_compute_runs` and applies run-log retention cleanup.

## Default AQI Settings

- `UK_AQ_AQI_MATURITY_DELAY_HOURS=3`
- `UK_AQ_AQI_SHORT_LOOKBACK_HOURS=36`
- `UK_AQ_AQI_DEEP_LOOKBACK_DAYS=14`
- `UK_AQ_AQI_RUN_LOG_RETENTION_DAYS=7`

## Manual Run

After deploy, execute one run via authenticated HTTP request body such as:

```json
{"trigger_mode":"manual","run_mode":"fast"}
```

Backfill example:

```json
{
  "trigger_mode": "manual",
  "run_mode": "backfill",
  "from_hour_utc": "2026-03-01T00:00:00Z",
  "to_hour_utc": "2026-03-02T23:00:00Z"
}
```
