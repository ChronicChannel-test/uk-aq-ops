# UK AQ Backfill Cloud Run Setup

Cloud Run backfill worker for UK AQ ops.

Current status (Phase 9, incremental):

- `local_to_aqilevels`: implemented and production runnable.
- `obs_aqi_to_r2`: planning/check mode implemented.
- `source_to_all`: partial execution implemented (local retained days only).

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

Implemented as a plan/check mode for now.

- Reads committed backup bounds using `uk_aq_public.uk_aq_rpc_r2_history_window`.
- Reads day completion evidence from `uk_aq_ops.prune_day_gates` in ingest DB.
- Treats a day as complete only when all are true:
  - `history_done = true`
  - `history_manifest_key is not null`
  - `history_completed_at is not null`

Outcomes:

- `dry_run=true`: returns planning summary with `backed_up_days` and `pending_backfill_days`.
- `dry_run=false` with no pending days: returns `ok` (no-op success).
- `dry_run=false` with pending days: throws by default.
- if `UK_AQ_BACKFILL_ALLOW_STUB_MODES=true`, pending-day non-dry runs return `stubbed` (planned only, no writes).

### `source_to_all`

Partially implemented:

- Computes rolling local retention window (`UK_AQ_BACKFILL_LOCAL_TIMEZONE`, `UK_AQ_BACKFILL_OBS_AQI_LOCAL_RETENTION_DAYS`).
- Splits requested days into:
  - `local_to_aqilevels_days` (inside local retention window),
  - `source_acquisition_pending_days` (outside local retention window).
- Executes `local_to_aqilevels` for retained days.
- Returns warnings for pending source acquisition days.

Status behavior:

- `dry_run=true`: `dry_run`.
- non-dry with pending acquisition days: `stubbed`.
- non-dry with no pending days and no local errors: `ok`.
- local processing errors: `error`.

## Run Status Values

- `ok`: completed for requested scope.
- `dry_run`: completed planning mode with no writes.
- `stubbed`: completed with intentionally unimplemented branch.
- `error`: failed.

## Required Runtime Variables and Secrets

### Required for `local_to_aqilevels` and `source_to_all` local writes

Variables:

- `SUPABASE_URL`
- `OBS_AQIDB_SUPABASE_URL`

Secrets:

- `SB_SECRET_KEY`
- `OBS_AQIDB_SECRET_KEY`

### Required for `obs_aqi_to_r2` planning/check

Variables:

- `SUPABASE_URL`

Secrets:

- `SB_SECRET_KEY`

## Optional Runtime Controls

Core:

- `UK_AQ_BACKFILL_RUN_MODE=local_to_aqilevels|obs_aqi_to_r2|source_to_all`
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

Ledger:

- `UK_AQ_BACKFILL_LEDGER_ENABLED` (default `true`)
- `UK_AQ_BACKFILL_DRY_RUN_WRITE_LEDGER` (default `false`)
- `UK_AQ_BACKFILL_OPS_SCHEMA` (default `uk_aq_ops`)

## Ledger Schema

Canonical SQL lives in the schema repo:

- `../CIC-Test-UK-AQ-Schema/CIC-test-uk-aq-schema/schemas/obs_aqi_db/uk_aq_backfill_ops_obs_aqi.sql`

Included tables:

- `uk_aq_ops.backfill_runs`
- `uk_aq_ops.backfill_run_days`
- `uk_aq_ops.backfill_checkpoints`
- `uk_aq_ops.backfill_errors`

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

### 5) Local dry-run (`source_to_all` split planning)

```bash
curl -sS -X POST http://127.0.0.1:8000/run \
  -H 'content-type: application/json' \
  -d '{
    "trigger_mode": "manual",
    "run_mode": "source_to_all",
    "dry_run": true,
    "from_day_utc": "2026-01-01",
    "to_day_utc": "2026-02-10"
  }' | jq .
```

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

### 7) Cloud Run non-dry `source_to_all` (partial mode visible)

```bash
curl -sS -X POST "${SERVICE_URL}/run" \
  -H "authorization: Bearer ${ID_TOKEN}" \
  -H 'content-type: application/json' \
  -d '{
    "trigger_mode": "manual",
    "run_mode": "source_to_all",
    "dry_run": false,
    "from_day_utc": "2026-01-01",
    "to_day_utc": "2026-02-10"
  }' | jq .
```

### 8) Trigger configured Cloud Scheduler job manually

```bash
gcloud scheduler jobs run "<backfill-scheduler-job-name>" \
  --location "europe-west2"
```
