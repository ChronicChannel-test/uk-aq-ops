# UK AQ AQI History R2 API Worker

Repo owner: `uk-aq-ops`
Worker path: `workers/uk_aq_aqi_history_r2_api_worker/worker.mjs`
Deploy workflow: `.github/workflows/uk_aq_aqi_history_r2_api_worker_deploy.yml`

## Purpose

Cloudflare Worker for station AQI history reads, stitched from:

- recent window from `obs_aqidb` (`uk_aq_public.uk_aq_timeseries_aqi_hourly`), and
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
- For the R2 segment, the worker first resolves `station_id -> connector_id` from `uk_aq_public.uk_aq_station_connector_lookup` and then reads only that connector manifest when the lookup succeeds.
- Optional AQI timeseries index fast-path:
  - prefix default: `history/_index/aqilevels_timeseries`
  - index key shape: `day_utc=YYYY-MM-DD/connector_id=<id>/manifest.json`
  - worker resolves station timeseries ids from `uk_aq_public.uk_aq_timeseries_aqi_hourly` within the requested window and narrows parquet scans by `min_timeseries_id/max_timeseries_id`.
  - if the resolved station timeseries id list is explicitly empty for the requested window, the worker now fast-returns an empty history segment and skips R2 parquet scans to avoid CPU-limit failures on stations with no AQI coverage.
  - missing/invalid index entries fall back to connector manifest scanning.
- AQI parquet reads use `station_id` row-group stats plus chunked column reads so the worker does not materialize whole parquet files for single-station requests.

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
- `UK_AQ_R2_HISTORY_INDEX_PREFIX=history/_index`
- `UK_AQ_AQI_HISTORY_R2_TIMESERIES_INDEX_PREFIX=history/_index/aqilevels_timeseries`
- `UK_AQ_AQI_HISTORY_R2_TIMESERIES_INDEX_ENABLED=true`
- `UK_AQ_AQI_HISTORY_R2_CACHE_MAX_AGE_SECONDS=300`
- `UK_AQ_AQI_HISTORY_R2_IMMUTABLE_CACHE_MAX_AGE_SECONDS=86400`
- `UK_AQ_AQI_HISTORY_SOURCE_OF_TRUTH_HOURS=168` (default)
- `UK_AQ_AQI_HISTORY_OBSAQIDB_TIMEOUT_MS=10000` (default)
- `UK_AQ_AQI_HISTORY_R2_PARQUET_ROW_CHUNK_SIZE=5000` (default)
- `UK_AQ_PUBLIC_SCHEMA=uk_aq_public`

## Cache proxy integration

Cache proxy route `/api/aq/aqi-history` should target this worker via:

- GitHub variable `UK_AQ_AQI_HISTORY_R2_API_URL=https://<worker-host>/v1/aqi-history`

Do not point `UK_AQ_AQI_HISTORY_R2_API_URL` back to `/api/aq/aqi-history` (would recurse).

## Response diagnostics

Coverage metadata includes the live/fallback status for the recent window:

- `coverage.obs_aqidb_status`: `not_requested`, `live`, or `history_fallback`
- `coverage.obs_aqidb_error`: recent live-read error message when fallback was needed
- `coverage.station_connector_id`: resolved connector for the requested station when lookup succeeds
- `coverage.station_connector_lookup_source_path`: PostgREST source used for the station connector lookup
- `coverage.station_connector_lookup_error`: lookup error when connector resolution fails and the worker falls back to scanning all connector manifests for the R2 segment
- `coverage.station_connector_lookup_cache_hit`: whether the in-worker station connector cache served the lookup
- `coverage.station_timeseries_lookup_source_path`: PostgREST source used for the station timeseries lookup
- `coverage.station_timeseries_lookup_error`: lookup error when station timeseries resolution fails and index filtering falls back to full connector manifest scans
- `coverage.station_timeseries_lookup_cache_hit`: whether the in-worker station timeseries cache served the lookup
- `coverage.station_timeseries_id_count`: number of station timeseries ids used for AQI index filtering
- `coverage.timeseries_index`: AQI index diagnostics for the main history segment (`enabled`, `prefix`, `hit_count`, `miss_count`, `skipped_days_by_file_range`, `skipped_files_by_pollutant`, and warnings)
- `coverage.r2_recent_fallback_*`: best-effort R2 fallback window, counts, and missing-file diagnostics for the recent segment
- `coverage.r2_recent_fallback_timeseries_index`: AQI index diagnostics for the recent fallback segment
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

Implementation note:

- R2 parquet reads are now based on normalized AQI rows (`pollutant_code`, `daqi_index_level`, `eaqi_index_level`).
- Pollutant-specific response fields remain available and are derived/aggregated at read time for compatibility.
