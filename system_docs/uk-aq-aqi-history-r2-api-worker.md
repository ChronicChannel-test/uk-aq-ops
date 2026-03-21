# UK AQ AQI History R2 API Worker

Repo owner: `uk-aq-ops`
Worker path: `workers/uk_aq_aqi_history_r2_api_worker/worker.mjs`
Deploy workflow: `.github/workflows/uk_aq_aqi_history_r2_api_worker_deploy.yml`

## Purpose

Cloudflare Worker for station AQI history reads, stitched from:

- recent window from `obs_aqidb` (`uk_aq_public.uk_aq_station_aqi_hourly`), and
- older window from R2 backups (`history/v1/aqilevels`).

This is intended for website DAQI/EAQI charts where recent AQI is not yet exported to R2.

## Routes

- `GET /v1/aqi-history`
- alias: `GET /`

## Query model

Required:

- `station_id` (positive integer)
  - aliases accepted: `entity`, `entity_id`

Optional:

- `scope` (must be `station`; default `station`)
- `grain` (must be `hourly`; default `hourly`)
- time range:
  - `from_utc` + `to_utc` (ISO timestamps)
  - aliases: `start_utc`/`end_utc`, `from`/`to`, `start`/`end`
  - or `days` lookback (default `1`)
- `since_utc` (ISO timestamp, exclusive lower bound)
  - alias: `since`
- `row_limit` (`1..20000`)
  - alias: `limit`
- `pollutant` (`pm25`, `pm10`, `no2`)
  - when present, the generic `daqi_index_level` / `eaqi_index_level` fields in each point are set from that pollutant only
  - pollutant-specific AQI columns are always returned in each point for explicit client-side mapping

Window split behavior:

- default recent source-of-truth window is last `168` hours (7 days) from now.
- requests are split into:
  - older segment -> R2 history, and
  - recent segment -> ObsAQIDB live table/view.
- overlapping timestamps are de-duplicated by hour, with ObsAQIDB rows winning.
- if the recent ObsAQIDB read fails, the worker falls back to R2 for that same recent window instead of failing the whole request.
- cache TTL is also dynamic by the requested end time:
  - requests ending within the last 24 hours use the short live TTL
  - requests ending more than 24 hours ago use the long immutable-history TTL

## Auth

- Requires header `x-uk-aq-upstream-auth`.
- Header value must equal Worker secret `UK_AQ_EDGE_UPSTREAM_SECRET`.

## R2 requirements

- Bucket binding: `UK_AQ_HISTORY_BUCKET`.
- Prefix default: `history/v1/aqilevels`.
- Reads day manifests first, then connector manifests/files under each day.

## Required GitHub env/secret targets

Variables:

- `UK_AQ_AQI_HISTORY_R2_API_WORKER_NAME` (optional; default `uk-aq-aqi-history-r2-api`)
- `UK_AQ_AQI_HISTORY_R2_API_CLOUDFLARE_ACCOUNT_ID` (or fallback `CLOUDFLARE_ACCOUNT_ID`)

Secrets:

- `UK_AQ_AQI_HISTORY_R2_API_CLOUDFLARE_API_TOKEN` (or fallback `CLOUDFLARE_API_TOKEN`)
- `UK_AQ_EDGE_UPSTREAM_SECRET`
- `OBS_AQIDB_SECRET_KEY`

Variables:

- `OBS_AQIDB_SUPABASE_URL`

## Runtime vars (wrangler defaults)

- `UK_AQ_R2_HISTORY_AQILEVELS_PREFIX=history/v1/aqilevels`
- `UK_AQ_AQI_HISTORY_R2_CACHE_MAX_AGE_SECONDS=300`
- `UK_AQ_AQI_HISTORY_R2_IMMUTABLE_CACHE_MAX_AGE_SECONDS=86400`
- `UK_AQ_AQI_HISTORY_SOURCE_OF_TRUTH_HOURS=168` (default)
- `UK_AQ_AQI_HISTORY_OBSAQIDB_TIMEOUT_MS=10000` (default)

## Cache proxy integration

Cache proxy route `/api/aq/aqi-history` should target this worker via:

- GitHub variable `UK_AQ_AQI_HISTORY_R2_API_URL=https://<worker-host>/v1/aqi-history`

Do not point `UK_AQ_AQI_HISTORY_R2_API_URL` back to `/api/aq/aqi-history` (would recurse).

## Response diagnostics

Coverage metadata includes the live/fallback status for the recent window:

- `coverage.obs_aqidb_status`: `not_requested`, `live`, or `history_fallback`
- `coverage.obs_aqidb_error`: recent live-read error message when fallback was needed
- `coverage.r2_recent_fallback_*`: best-effort R2 fallback window, counts, and missing-file diagnostics for the recent segment
- top-level `cache_scope`: `recent` or `immutable`

## Point payload

Each `points[]` row includes:

- `period_start_utc`
- `station_id`
- generic AQI fields:
  - `daqi_index_level`
  - `eaqi_index_level`
- pollutant-specific AQI fields:
  - `daqi_no2_index_level`
  - `daqi_pm25_rolling24h_index_level`
  - `daqi_pm10_rolling24h_index_level`
  - `eaqi_no2_index_level`
  - `eaqi_pm25_index_level`
  - `eaqi_pm10_index_level`

When `pollutant` is omitted, the generic pair remains the max-across-supported-pollutants summary for backward compatibility. New chart clients should read the pollutant-specific fields directly.
