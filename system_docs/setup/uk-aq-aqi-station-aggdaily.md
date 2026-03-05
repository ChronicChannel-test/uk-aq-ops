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
