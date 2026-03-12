# UK AQ Backfill Cloud Run Setup

Cloud Run backfill worker for UK AQ ops.

Current status (Phase 9, incremental):

- `local_to_aqilevels`: implemented and production runnable.
- `obs_aqi_to_r2`: implemented with dry-run planning plus non-dry export/write.
- `source_to_r2`: Sensor.Community + OpenAQ source adapters implemented for archive-to-R2 writes.

## Runtime Components

- Service entrypoint: `workers/uk_aq_backfill_cloud_run/run_service.ts`
- Job logic: `workers/uk_aq_backfill_cloud_run/run_job.ts`
- Shared helpers: `workers/uk_aq_backfill_cloud_run/backfill_core.mjs`
- Docker image file: `workers/uk_aq_backfill_cloud_run/Dockerfile`
- Deploy workflow: `.github/workflows/uk_aq_backfill_cloud_run_deploy.yml`

## What The Worker Does

The service (`run_service.ts`) wraps `run_job.ts` over HTTP.

- `GET /` returns health and run modes.
- `POST /` and `POST /run` execute one backfill run.
- In-memory lock blocks overlapping runs per container (`409 run_in_flight`).
- Inputs are accepted from query, headers, or JSON body.

Input precedence per field:

1. query string
2. custom header
3. JSON body
4. environment fallback in `run_job.ts`

## Run Mode Behavior

### `local_to_aqilevels`

Main flow:

1. Resolve `from_day_utc` and `to_day_utc` (default yesterday UTC).
2. Build inclusive day range and process newest day first.
3. For each day, compute connector candidates from ingest/obs_aqidb fingerprint RPC counts.
4. For each connector/day choose source by priority:
   - ingest for likely in-retention days,
   - obs_aqidb for older retained days,
   - optional R2 fallback only when enabled.
5. Fetch source hourly rows and normalize helper shape.
6. Upsert hourly AQI rows and refresh daily/monthly rollups.
7. Write structured logs and optional ledger/checkpoints.

Idempotency:

- default skip if checkpoint status is complete/ok.
- `force_replace=true` bypasses skip.
- `dry_run=true` avoids write-path RPC calls.

R2 fallback note:

- `enable_r2_fallback=true` can route empty local-source connector/day to `r2`.
- pull-from-R2 read path is still not implemented in this worker, so those connector/days error with `r2_fallback_not_implemented_in_phase1`.

### `obs_aqi_to_r2`

Implemented as a real export/write path.

- Checks committed day manifests directly in R2 for both domains:
  - `history/v1/observations/day_utc=YYYY-MM-DD/manifest.json`
  - `history/v1/aqilevels/day_utc=YYYY-MM-DD/manifest.json`
- Reads source rows from `obs_aqidb` (`uk_aq_observs.observations`) via:
  - `uk_aq_public.uk_aq_rpc_observs_history_day_rows` when available, else
  - direct table fallback query.
- Reads AQI rows from `obs_aqidb` (`uk_aq_aqilevels.station_aqi_hourly`) via:
  - `uk_aq_public.uk_aq_rpc_aqilevels_history_day_connector_counts`
  - `uk_aq_public.uk_aq_rpc_aqilevels_history_day_rows`
- Writes in both domains:
  - connector parquet parts (`part-00000.parquet`, etc.),
  - connector manifests,
  - day manifest under:
    - `history/v1/observations/day_utc=.../manifest.json`
    - `history/v1/aqilevels/day_utc=.../manifest.json`
- Honors `force_replace=true` by re-exporting selected connector/day payloads.
- Safety guards for partial-day prevention:
  - when `connector_ids` filter is used, export is blocked if non-target connectors for that day do not already have connector manifests;
  - if a day export fails and that day was not previously committed (no existing day manifest), the worker deletes that day prefix so partial connector artifacts are not left in R2.

Outcomes:

- `dry_run=true`: returns planning summary with `backed_up_days` and `pending_backfill_days` where "backed up" means both observations + aqilevels day manifests exist.
- `dry_run=false`: executes writes for pending (or forced) days.
- non-dry returns `error` if connector/day failures leave pending days after run.

### `source_to_r2`

Implemented source adapters:

- Sensor.Community
  - source acquisition from `https://archive.sensor.community/YYYY-MM-DD/`.
  - archive files filtered by known core `station_ref` (Sensor.Community `sensor_id`).
  - unknown archive station refs are logged.
- OpenAQ (phase 1)
  - source acquisition from OpenAQ public AWS archive only:
    - `records/csv.gz/locationid=<LOCATION_ID>/year=<YYYY>/month=<MM>/location-<LOCATION_ID>-<YYYYMMDD>.csv.gz`
  - candidate UK location universe comes from existing OpenAQ station refs (no OpenAQ API fallback in phase 1).
  - source mapping follows project mapping:
    - `station_ref = OpenAQ location_id`
    - `timeseries_ref = OpenAQ sensor_id`
  - optional local raw mirror (local runs only) reuses/saves source `.csv.gz` files under:
    - `day_utc=YYYY-MM-DD/location-<location_id>-<YYYYMMDD>.csv.gz`

Shared behavior:

- Core metadata resolution:
  - reads connector/timeseries metadata from latest R2 core snapshot first (`UK_AQ_R2_HISTORY_CORE_PREFIX`),
  - falls back to ingest `uk_aq_core` tables when needed.
- Observations export:
  - parses archive CSV rows into canonical `(timeseries_id, observed_at, value)` observations.
- AQI export:
  - derives hourly means/sample counts from parsed observations,
  - computes rolling 24h PM means,
  - computes DAQI/EAQI index levels in-worker using project breakpoints,
  - writes canonical AQI hourly rows to R2.
- Output layout:
  - writes connector parquet parts + connector manifests for both domains,
  - then writes day manifests:
    - `history/v1/observations/day_utc=.../manifest.json`
    - `history/v1/aqilevels/day_utc=.../manifest.json`
- Supports optional local archive mirroring for replay/dev:
  - `UK_AQ_BACKFILL_SCOMM_RAW_MIRROR_ROOT`.
  - `UK_AQ_BACKFILL_OPENAQ_RAW_MIRROR_ROOT` (local runs only).

Status behavior:

- `dry_run=true`: `dry_run`.
- non-dry with pending unsupported connector acquisition days: `stubbed`.
- non-dry with no pending days and no connector/day errors: `ok`.
- connector/day processing errors: `error`.

## Run Status Values

- `ok`: completed for requested scope.
- `dry_run`: completed planning mode with no writes.
- `stubbed`: completed with intentionally unimplemented branch.
- `error`: failed.

## Required Runtime Variables and Secrets

### Required for `local_to_aqilevels` AQI DB writes

Variables:

- `SUPABASE_URL`
- `OBS_AQIDB_SUPABASE_URL`

Secrets:

- `SB_SECRET_KEY`
- `OBS_AQIDB_SECRET_KEY`

### Required for `obs_aqi_to_r2` and `source_to_r2` R2 export/write

Variables:

- `OBS_AQIDB_SUPABASE_URL`
- `CFLARE_R2_ENDPOINT` (or `R2_ENDPOINT`)
- `CFLARE_R2_REGION` (or `R2_REGION`, default `auto`)
- one bucket mapping:
  - `CFLARE_R2_BUCKET` / `R2_BUCKET`
  - or `R2_BUCKET_PROD|R2_BUCKET_STAGE|R2_BUCKET_DEV` with `UK_AQ_DEPLOY_ENV`

Secrets:

- `OBS_AQIDB_SECRET_KEY`
- `CFLARE_R2_ACCESS_KEY_ID` (or `R2_ACCESS_KEY_ID`)
- `CFLARE_R2_SECRET_ACCESS_KEY` (or `R2_SECRET_ACCESS_KEY`)

## Optional Runtime Controls

Core:

- `UK_AQ_BACKFILL_RUN_MODE=local_to_aqilevels|obs_aqi_to_r2|source_to_r2`
- `UK_AQ_BACKFILL_TRIGGER_MODE=manual|scheduler`
- `UK_AQ_BACKFILL_DRY_RUN=true|false`
- `UK_AQ_BACKFILL_FORCE_REPLACE=true|false`
- `UK_AQ_BACKFILL_FROM_DAY_UTC=YYYY-MM-DD`
- `UK_AQ_BACKFILL_TO_DAY_UTC=YYYY-MM-DD`
- `UK_AQ_BACKFILL_CONNECTOR_IDS=4,7,11`
- `UK_AQ_BACKFILL_ENABLE_R2_FALLBACK=true|false`
- `UK_AQ_BACKFILL_ALLOW_STUB_MODES=true|false` (default `false`)

Retention/window:

- `UK_AQ_BACKFILL_INGEST_RETENTION_DAYS` (default `7`)
- `UK_AQ_BACKFILL_OBS_AQI_LOCAL_RETENTION_DAYS` (default `31`)
- `UK_AQ_BACKFILL_LOCAL_TIMEZONE` (default `Europe/London`)

RPC tuning:

- `UK_AQ_BACKFILL_RPC_RETRIES` (default `3`)
- `UK_AQ_BACKFILL_HOURLY_UPSERT_CHUNK_SIZE` (default `2000`)
- `UK_AQ_BACKFILL_STATION_ID_PAGE_SIZE` (default `1000`)
- `UK_AQ_BACKFILL_SOURCE_RPC_PAGE_SIZE` (default `1000`)
- `UK_AQ_BACKFILL_SOURCE_RPC_MAX_PAGES` (default `200`)
- `UK_AQ_BACKFILL_OBS_R2_PAGE_SIZE` (default `20000`)
- `UK_AQ_BACKFILL_OBS_R2_MAX_PAGES` (default `50000`; safety ceiling for obs/aqi history export pagination)
- `UK_AQ_BACKFILL_R2_CORE_LOOKBACK_DAYS` (default `45`)
- `UK_AQ_BACKFILL_R2_CORE_SNAPSHOT_MAX_BYTES` (default `250000000`)
- `UK_AQ_BACKFILL_SCOMM_SOURCE_ENABLED` (default `true`)
- `UK_AQ_BACKFILL_SCOMM_CONNECTOR_CODE` (default `sensorcommunity`)
- `UK_AQ_BACKFILL_SCOMM_ARCHIVE_BASE_URL` (default `https://archive.sensor.community`)
- `UK_AQ_BACKFILL_SCOMM_INCLUDE_MET_FIELDS` (default `true`)
- `UK_AQ_BACKFILL_SCOMM_ARCHIVE_TIMEOUT_MS` (default `120000`)
- `UK_AQ_BACKFILL_SCOMM_ARCHIVE_FETCH_RETRIES` (default `3`; retries Sensor.Community index/CSV fetches on transient HTTP/network errors)
- `UK_AQ_BACKFILL_SCOMM_ARCHIVE_RETRY_BASE_MS` (default `1500`; linear retry backoff base delay in ms)
- `UK_AQ_BACKFILL_SCOMM_RAW_MIRROR_ROOT` (optional local replay mirror)
- `UK_AQ_BACKFILL_OPENAQ_SOURCE_ENABLED` (default `true`)
- `UK_AQ_BACKFILL_OPENAQ_CONNECTOR_CODE` (default `openaq`)
- `UK_AQ_BACKFILL_OPENAQ_CONNECTOR_ID_FALLBACK` (optional numeric fallback connector id)
- `UK_AQ_BACKFILL_OPENAQ_ARCHIVE_BASE_URL` (default `https://openaq-data-archive.s3.amazonaws.com`)
- `UK_AQ_BACKFILL_OPENAQ_INCLUDE_MET_FIELDS` (default `true`)
- `UK_AQ_BACKFILL_OPENAQ_ARCHIVE_TIMEOUT_MS` (default `120000`)
- `UK_AQ_BACKFILL_OPENAQ_ARCHIVE_FETCH_RETRIES` (default `3`)
- `UK_AQ_BACKFILL_OPENAQ_ARCHIVE_RETRY_BASE_MS` (default `1500`)
- `UK_AQ_BACKFILL_OPENAQ_RAW_MIRROR_ROOT` (optional local replay mirror; only used when running `run_job.ts` locally)
- `UK_AQ_R2_HISTORY_PART_MAX_ROWS` (default `1000000`)
- `UK_AQ_R2_HISTORY_ROW_GROUP_SIZE` (default `100000`)
- `UK_AQ_BACKFILL_OBS_R2_SOURCE_RPC` (default `uk_aq_rpc_observs_history_day_rows`)
- `UK_AQ_BACKFILL_AQI_R2_SOURCE_RPC` (default `uk_aq_rpc_aqilevels_history_day_rows`)
- `UK_AQ_BACKFILL_AQI_R2_CONNECTOR_COUNTS_RPC` (default `uk_aq_rpc_aqilevels_history_day_connector_counts`)
- `UK_AQ_R2_HISTORY_OBSERVATIONS_PREFIX` (default `history/v1/observations`)
- `UK_AQ_R2_HISTORY_AQILEVELS_PREFIX` (default `history/v1/aqilevels`)
- `UK_AQ_R2_HISTORY_CORE_PREFIX` (default `history/v1/core`)

Fallback note:

- if `UK_AQ_BACKFILL_OBS_R2_SOURCE_RPC` is unavailable, expose `uk_aq_observs` in PostgREST so table fallback can read `uk_aq_observs.observations`.
- AQI history export requires both AQI RPCs above (no table fallback path).
- `obs_aqi_to_r2` pagination is cursor-driven and runs until an empty page is returned; it does not stop early solely because a page is smaller than the requested page size.

Ledger:

- `UK_AQ_BACKFILL_LEDGER_ENABLED` (default `true`)
- `UK_AQ_BACKFILL_DRY_RUN_WRITE_LEDGER` (default `false`)
- `UK_AQ_BACKFILL_OPS_SCHEMA` (default `uk_aq_public`; compatible views over `uk_aq_ops` backfill tables)

## Ledger Schema

Canonical SQL lives in the schema repo:

- `../CIC-Test-UK-AQ-Schema/CIC-test-uk-aq-schema/schemas/obs_aqi_db/uk_aq_backfill_ops_obs_aqi.sql`

Included tables:

- `uk_aq_ops.backfill_runs`
- `uk_aq_ops.backfill_run_days`
- `uk_aq_ops.backfill_checkpoints`
- `uk_aq_ops.backfill_errors`

Data API compatibility views:

- `uk_aq_public.backfill_runs`
- `uk_aq_public.backfill_run_days`
- `uk_aq_public.backfill_checkpoints`
- `uk_aq_public.backfill_errors`

If these tables are unavailable, the worker still runs but skip/checkpoint persistence is limited.

## Command Reference

### 1) Local service run

```bash
deno run --allow-env --allow-net --allow-read --allow-write --allow-run \
  workers/uk_aq_backfill_cloud_run/run_service.ts
```

### 2) Local health check

```bash
curl -sS http://127.0.0.1:8000/ | jq .
```

### 3) Local dry-run (`local_to_aqilevels`)

```bash
curl -sS -X POST http://127.0.0.1:8000/run \
  -H 'content-type: application/json' \
  -d '{
    "trigger_mode": "manual",
    "run_mode": "local_to_aqilevels",
    "dry_run": true,
    "from_day_utc": "2026-02-01",
    "to_day_utc": "2026-02-05",
    "connector_ids": [4]
  }' | jq .
```

### 4) Local dry-run (`obs_aqi_to_r2` planning)

```bash
curl -sS -X POST http://127.0.0.1:8000/run \
  -H 'content-type: application/json' \
  -d '{
    "trigger_mode": "manual",
    "run_mode": "obs_aqi_to_r2",
    "dry_run": true,
    "from_day_utc": "2026-02-01",
    "to_day_utc": "2026-02-10"
  }' | jq .
```

### 5) Local dry-run (`source_to_r2` archive plan)

```bash
curl -sS -X POST http://127.0.0.1:8000/run \
  -H 'content-type: application/json' \
  -d '{
    "trigger_mode": "manual",
    "run_mode": "source_to_r2",
    "dry_run": true,
    "from_day_utc": "2026-02-11",
    "to_day_utc": "2026-02-15",
    "connector_ids": [7]
  }' | jq .
```

Use the Sensor.Community connector id (commonly `7`) or the OpenAQ connector id for OpenAQ AWS archive backfill.

### 6) Cloud Run call setup

```bash
PROJECT_ID="<gcp-project-id>"
REGION="europe-west2"
SERVICE_NAME="<uk-aq-backfill-service-name>"

SERVICE_URL="$(gcloud run services describe "${SERVICE_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --format='value(status.url)')"

ID_TOKEN="$(gcloud auth print-identity-token)"
```

### 7) Cloud Run non-dry `source_to_r2`

```bash
curl -sS -X POST "${SERVICE_URL}/run" \
  -H "authorization: Bearer ${ID_TOKEN}" \
  -H 'content-type: application/json' \
  -d '{
    "trigger_mode": "manual",
    "run_mode": "source_to_r2",
    "dry_run": false,
    "force_replace": true,
    "from_day_utc": "2026-02-11",
    "to_day_utc": "2026-02-15",
    "connector_ids": [7]
  }' | jq .
```

### 8) Trigger configured Cloud Scheduler job manually

```bash
gcloud scheduler jobs run "<backfill-scheduler-job-name>" \
  --location "europe-west2"
```
