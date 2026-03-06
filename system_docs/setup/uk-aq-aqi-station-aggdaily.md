# UK AQ Station AQI AggDaily Cloud Run Setup

This service is now **sync-only**: it reads precomputed AQI helper rows from ingest DB and writes them to AggDaily DB (hourly upsert + daily/monthly rollups + run telemetry).

AQI helper computation is scheduled in ingest DB via Supabase `pg_cron`.

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

## Ingest Cron (Primary Compute)

In ingest DB, `pg_cron` runs hourly at `:10`:

- job: `uk_aq_ingest_station_aqi_hourly_helper_tick`
- formula:
  - `target_hour_end_utc = date_trunc('hour', now() - interval '3 hours 10 minutes')`
- processing rule:
  - use `> start AND <= end` hour-end windowing

This computes/upserts `uk_aq_aggdaily.station_aqi_hourly_helper` in ingest DB.

## Cloud Scheduler (Sync Service)

When `GCP_AQI_STATION_AGGDAILY_SCHEDULER_ENABLED=true`, workflow can manage sync triggers.

Default fast job:

- fast sync: `20 * * * *` (`run_mode=fast`)

Reconcile jobs remain available through the same service (`reconcile_short`, `reconcile_deep`) if enabled.

## Trigger Window Logic

Worker computes a mature **hour-end**:

- `target_hour_end_utc = floor_utc_hour(now_utc - (UK_AQ_AQI_MATURITY_DELAY_HOURS + UK_AQ_AQI_MATURITY_DELAY_BUFFER_MINUTES))`
- defaults: `3h` + `10m`

Mode windows:

- `fast`: `(target_hour_end_utc - 1h, target_hour_end_utc]`
- `reconcile_short`: trailing mature 36h hour-end window (default)
- `reconcile_deep`: trailing mature 14d hour-end window (default)

Sync behavior:

- fetch helper rows from ingest RPC `uk_aq_rpc_station_aqi_hourly_helper_window`
- upsert into AggDaily `station_aqi_hourly`
- refresh daily/monthly rollups for affected window and stations
- log run metrics to `uk_aq_ops.aqi_compute_runs` in AggDaily

## Default AQI Settings

- `UK_AQ_AQI_MATURITY_DELAY_HOURS=3`
- `UK_AQ_AQI_MATURITY_DELAY_BUFFER_MINUTES=10`
- `UK_AQ_AQI_SHORT_LOOKBACK_HOURS=36`
- `UK_AQ_AQI_DEEP_LOOKBACK_DAYS=14`
- `UK_AQ_AQI_RUN_LOG_RETENTION_DAYS=7`

## Manual Run

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
