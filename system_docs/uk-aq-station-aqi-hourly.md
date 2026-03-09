# UK AQ Station AQI Hourly Cloud Run Setup

This service is sync-only. It reads station-hour helper rows from ingest DB and writes hourly/daily/monthly AQI outputs into `obs_aqidb` (`uk_aq_aqilevels`).

AQI helper computation itself is scheduled in ingest DB via `pg_cron`.

## Runtime

- Service: `workers/uk_aq_station_aqi_hourly_cloud_run/run_service.ts`
- Job: `workers/uk_aq_station_aqi_hourly_cloud_run/run_job.ts`
- Deploy workflow: `.github/workflows/uk_aq_station_aqi_hourly_cloud_run_deploy.yml`

## Required GitHub Variables/Secrets

Required variables:

- `GCP_PROJECT_ID`
- `GCP_REGION`
- `GCP_ARTIFACT_REPO`
- `GCP_STATION_AQI_HOURLY_SERVICE_NAME`
- `GCP_STATION_AQI_HOURLY_SERVICE_ACCOUNT`
- `SUPABASE_URL`
- `OBS_AQIDB_SUPABASE_URL`

Required secrets:

- `SB_SECRET_KEY`
- `OBS_AQIDB_SECRET_KEY`

Google auth (choose one):

- WIF: `GCP_WORKLOAD_IDENTITY_PROVIDER` + `GCP_SERVICE_ACCOUNT`
- SA key: `GCP_SA_KEY`

## Ingest Cron (Primary Helper Compute)

In ingest DB, `pg_cron` runs helper computation hourly at `:10`:

- job: `uk_aq_ingest_station_aqi_hourly_helper_tick`
- formula:
  - `target_hour_end_utc = date_trunc('hour', now() - interval '3 hours 10 minutes')`
- processing rule:
  - use `> start AND <= end` hour-end windowing

This writes `uk_aq_aqilevels.station_aqi_hourly_helper` in ingest DB.

## Cloud Scheduler Trigger

When `GCP_STATION_AQI_HOURLY_SCHEDULER_ENABLED=true`, deploy workflow manages one scheduler job:

- job: `uk-aq-station-aqi-hourly-trigger`
- schedule: `20 * * * *`
- payload: `{"trigger_mode":"scheduler","run_mode":"sync_hourly"}`

## Trigger Window Logic

Worker computes a mature hour-end:

- `target_hour_end_utc = floor_utc_hour(now_utc - (UK_AQ_AQI_MATURITY_DELAY_HOURS + UK_AQ_AQI_MATURITY_DELAY_BUFFER_MINUTES))`
- defaults: `3h` + `10m`

Mode windows:

- `sync_hourly`: `(target_hour_end_utc - 1h, target_hour_end_utc]`
- `backfill`: explicit window from manual `from_hour_utc`/`to_hour_utc`

Sync behavior:

- fetch helper rows from ingest RPC `uk_aq_rpc_station_aqi_hourly_helper_window`
- upsert into `uk_aq_aqilevels.station_aqi_hourly`
- refresh daily/monthly rollups for affected window and stations
- log run metrics in `uk_aq_ops.aqi_compute_runs` within `obs_aqidb`

## Manual Run

```json
{"trigger_mode":"manual","run_mode":"sync_hourly"}
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
