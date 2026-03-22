# UK AQ Station AQI Hourly Cloud Run Setup

This service syncs station-hour AQI helper rows from ingest DB into `obs_aqidb` (`uk_aq_aqilevels`) and now also reconciles late-arriving observations over recent rolling windows. AQI helper computation itself is still scheduled in ingest DB via `pg_cron`; this worker reuses the existing helper-window, hourly-upsert, and rollup-refresh RPCs.

## Why reconciliation was added

Late observations frequently land after the worker's original single-hour sync window has already run, which leaves null AQI stripe gaps in station charts until a manual backfill happens. Measured lag from recent investigation showed the hourly-only window is too narrow for real production latency:

- Station `7483`: p50 `00:58:13`, p90 `02:34:36`, p99 `05:24:24`, max `05:50:12`
- Connector lag examples:
  - `uk_air_sos` p99 `06:50:12`
  - `breathelondon` p99 `03:55:30`
  - `openaq` max `20:33:30`
  - `sensorcommunity` is comparatively low-lag and not the main driver

To close those gaps without changing backfill semantics or schema, the worker now supports short and deep reconciliation modes that recompute recent mature windows.

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

When `GCP_STATION_AQI_HOURLY_SCHEDULER_ENABLED=true`, the deploy workflow keeps the existing sync scheduler and can optionally manage extra reconciliation schedulers:

- sync job:
  - job: `uk-aq-station-aqi-hourly-trigger`
  - schedule: `20 * * * *`
  - payload: `{"trigger_mode":"scheduler","run_mode":"sync_hourly"}`
- optional short reconcile job, enabled by `GCP_STATION_AQI_HOURLY_RECONCILE_SHORT_SCHEDULER_ENABLED=true`:
  - default job: `uk-aq-station-aqi-reconcile-short-trigger`
  - default schedule: `35 * * * *`
  - payload: `{"trigger_mode":"scheduler","run_mode":"reconcile_short"}`
- optional deep reconcile job, enabled by `GCP_STATION_AQI_HOURLY_RECONCILE_DEEP_SCHEDULER_ENABLED=true`:
  - default job: `uk-aq-station-aqi-reconcile-deep-trigger`
  - default schedule: `50 */6 * * *`
  - payload: `{"trigger_mode":"scheduler","run_mode":"reconcile_deep"}`

If you do not want the workflow to manage the optional jobs, call the Cloud Run service manually with the same JSON payloads shown above.

## Trigger Window Logic

Worker computes one mature reference hour-end:

- `target_hour_end_utc = floor_utc_hour(now_utc - (UK_AQ_AQI_MATURITY_DELAY_HOURS + UK_AQ_AQI_MATURITY_DELAY_BUFFER_MINUTES))`
- defaults: `3h` + `10m`

Mode windows:

- `sync_hourly`: `(target_hour_end_utc - 1h, target_hour_end_utc]`
- `reconcile_short`: `(target_hour_end_utc - UK_AQ_AQI_RECONCILE_SHORT_HOURS, target_hour_end_utc]`
- `reconcile_deep`: `(target_hour_end_utc - UK_AQ_AQI_RECONCILE_DEEP_HOURS, target_hour_end_utc]`
- `backfill`: explicit window from manual `from_hour_utc`/`to_hour_utc`, preserving existing behavior

Defaults:

- `UK_AQ_AQI_RECONCILE_SHORT_HOURS=8`
- `UK_AQ_AQI_RECONCILE_DEEP_HOURS=36`

Behavior for all non-backfill modes:

- reuse ingest RPC `uk_aq_rpc_station_aqi_hourly_helper_window` for the computed hour window
- reuse `uk_aq_rpc_station_aqi_hourly_upsert` with the existing chunking and retry behavior
- refresh daily/monthly rollups across the actual recomputed window and affected stations
- log `run_mode`, `window_start_utc`, and `window_end_utc` into `uk_aq_ops.aqi_compute_runs` within `obs_aqidb`

`UK_AQ_AQI_STATION_IDS_CSV` still scopes helper fetches and rollup refreshes for manual targeted runs, including targeted backfill or reconciliation.

## Manual Run

Sync example:

```json
{"trigger_mode":"manual","run_mode":"sync_hourly"}
```

Short reconcile example:

```json
{"trigger_mode":"manual","run_mode":"reconcile_short"}
```

Deep reconcile example:

```json
{"trigger_mode":"manual","run_mode":"reconcile_deep"}
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

Targeted reconcile example:

```json
{
  "trigger_mode": "manual",
  "run_mode": "reconcile_short",
  "station_ids": [7483]
}
```
