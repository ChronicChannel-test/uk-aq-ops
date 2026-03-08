# UK AQ Backfill Cloud Run Setup

Cloud Run backfill worker for UK AQ ops.

Phase 1 status:

- `local_to_aggdaily`: implemented
- `obs_aqi_to_r2`: wired stub (no export pipeline yet)
- `source_to_all`: wired stub (retention classification only)

## Runtime Components

- Service entrypoint: `workers/uk_aq_backfill_cloud_run/run_service.ts`
- Job logic: `workers/uk_aq_backfill_cloud_run/run_job.ts`
- Shared helpers: `workers/uk_aq_backfill_cloud_run/backfill_core.mjs`
- Docker image file: `workers/uk_aq_backfill_cloud_run/Dockerfile`
- Deploy workflow: `.github/workflows/uk_aq_backfill_cloud_run_deploy.yml`

## What The Worker Does

The service process (`run_service.ts`) is an HTTP trigger wrapper around `run_job.ts`.

- `GET /` returns service health + available run modes.
- `POST /` and `POST /run` execute one backfill run.
- A single in-memory lock blocks overlapping runs in one container (`409 run_in_flight`).
- Request controls are accepted from query, headers, or JSON body.

Input precedence per field is:

1. query string
2. custom header
3. JSON body
4. environment fallback inside `run_job.ts`

## Run Mode Behavior

### `local_to_aggdaily` (implemented in Phase 1)

Main flow per run:

1. Resolve `from_day_utc`/`to_day_utc` (default is yesterday UTC for both).
2. Build inclusive day range and process newest day first.
3. For each day, compute connector candidates from ingest/history fingerprint RPC counts.
4. For each connector/day, choose source by priority:
   - Ingest first for days likely inside ingest retention.
   - History first for older retained days.
   - Optional R2 fallback only when explicitly enabled.
5. Fetch source hourly rows and normalize to helper-row shape.
6. Upsert AggDaily hourly rows.
7. Refresh AggDaily daily/monthly rollups for affected stations.
8. Write structured logs + optional ledger/checkpoints.

Idempotency and rerun safety:

- Default skip if checkpoint status is `complete`/`ok`.
- `force_replace=true` bypasses skip.
- `dry_run=true` avoids AggDaily writes and checkpoint completion writes.

R2 fallback note in Phase 1:

- `enable_r2_fallback=true` can route connector/day to `r2` when local sources are empty.
- Actual R2 pull logic is not implemented yet, so Phase 1 returns `r2_fallback_not_implemented_in_phase1` for those cases.

### `obs_aqi_to_r2` (stub in Phase 1)

- Fully routed and triggerable.
- Returns a stubbed summary only.
- No obs_aqidb->R2 History parquet/manifest writes yet.

### `source_to_all` (stub in Phase 1)

- Fully routed and triggerable.
- Computes retention boundary classification only:
  - `observs_write_eligible_days`
  - `observs_write_skipped_days`
- No source acquisition/write pipeline yet.

## Required Runtime Variables/Secrets (Phase 1)

Required variables:

- `SUPABASE_URL`
- `OBS_AQIDB_SUPABASE_URL`
- `AGGDAILY_SUPABASE_URL`

Required secrets:

- `SB_SECRET_KEY`
- `OBS_AQIDB_SECRET_KEY`
- `AGGDAILY_SECRET_KEY`

## Optional Runtime Controls

Core controls:

- `UK_AQ_BACKFILL_RUN_MODE=local_to_aggdaily|obs_aqi_to_r2|source_to_all`
- `UK_AQ_BACKFILL_TRIGGER_MODE=manual|scheduler`
- `UK_AQ_BACKFILL_DRY_RUN=true|false`
- `UK_AQ_BACKFILL_FORCE_REPLACE=true|false`
- `UK_AQ_BACKFILL_FROM_DAY_UTC=YYYY-MM-DD`
- `UK_AQ_BACKFILL_TO_DAY_UTC=YYYY-MM-DD`
- `UK_AQ_BACKFILL_CONNECTOR_IDS=4,7,11`
- `UK_AQ_BACKFILL_ENABLE_R2_FALLBACK=true|false`

Retention / window controls:

- `UK_AQ_BACKFILL_INGEST_RETENTION_DAYS` (default `7`)
- `UK_AQ_BACKFILL_OBS_AQI_LOCAL_RETENTION_DAYS` (default `31`)
- `UK_AQ_BACKFILL_LOCAL_TIMEZONE` (default `Europe/London`)

RPC tuning controls:

- `UK_AQ_BACKFILL_RPC_RETRIES` (default `3`)
- `UK_AQ_BACKFILL_HOURLY_UPSERT_CHUNK_SIZE` (default `2000`)
- `UK_AQ_BACKFILL_STATION_ID_PAGE_SIZE` (default `1000`)
- `UK_AQ_BACKFILL_SOURCE_RPC_PAGE_SIZE` (default `1000`)
- `UK_AQ_BACKFILL_SOURCE_RPC_MAX_PAGES` (default `200`)

Ledger controls:

- `UK_AQ_BACKFILL_LEDGER_ENABLED` (default `true`)
- `UK_AQ_BACKFILL_DRY_RUN_WRITE_LEDGER` (default `false`)
- `UK_AQ_BACKFILL_OPS_SCHEMA` (default `uk_aq_ops`)

## Retention Boundary Rule (source_to_all stub)

The helper computes a rolling local-day window in `UK_AQ_BACKFILL_LOCAL_TIMEZONE` and maps that to retained UTC days.

- Local retention default: 31 local days ending on last complete local day.
- Across UK DST transitions, retained UTC day count can be `31` or `32`.
- This is why the local obs_aqidb retention window is documented as `31/32` UTC days.

## Optional Ledger Schema

For persistent run/day/checkpoint/error tracking in AggDaily DB:

- Apply schema-repo SQL (canonical):
  - `../CIC-Test-UK-AQ-Schema/CIC-test-uk-aq-schema/schemas/aggdaily_db/uk_aq_backfill_ops_aggdaily.sql`
- The tables are also included in:
  - `../CIC-Test-UK-AQ-Schema/CIC-test-uk-aq-schema/schemas/aggdaily_db/uk_aq_aggdaily_schema.sql`

Tables:

- `uk_aq_ops.backfill_runs`
- `uk_aq_ops.backfill_run_days`
- `uk_aq_ops.backfill_checkpoints`
- `uk_aq_ops.backfill_errors`

If schema is not applied, worker still runs but cross-run checkpoint skip persistence is unavailable.

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

### 3) Local dry-run (`local_to_aggdaily`)

```bash
curl -sS -X POST http://127.0.0.1:8000/run \
  -H 'content-type: application/json' \
  -d '{
    "trigger_mode": "manual",
    "run_mode": "local_to_aggdaily",
    "dry_run": true,
    "from_day_utc": "2026-02-01",
    "to_day_utc": "2026-02-05",
    "connector_ids": [4]
  }' | jq .
```

### 4) Cloud Run authenticated call setup

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

### 5) Cloud Run dry-run (`local_to_aggdaily`)

```bash
curl -sS -X POST "${SERVICE_URL}/run" \
  -H "authorization: Bearer ${ID_TOKEN}" \
  -H 'content-type: application/json' \
  -d '{
    "trigger_mode": "manual",
    "run_mode": "local_to_aggdaily",
    "dry_run": true,
    "from_day_utc": "2026-02-20",
    "to_day_utc": "2026-02-20"
  }' | jq .
```

### 6) Cloud Run real run with connector filter

```bash
curl -sS -X POST "${SERVICE_URL}/run" \
  -H "authorization: Bearer ${ID_TOKEN}" \
  -H 'content-type: application/json' \
  -d '{
    "trigger_mode": "manual",
    "run_mode": "local_to_aggdaily",
    "dry_run": false,
    "from_day_utc": "2026-02-01",
    "to_day_utc": "2026-02-05",
    "connector_ids": [4,7]
  }' | jq .
```

### 7) Force reprocess (ignore complete checkpoints)

```bash
curl -sS -X POST "${SERVICE_URL}/run" \
  -H "authorization: Bearer ${ID_TOKEN}" \
  -H 'content-type: application/json' \
  -d '{
    "trigger_mode": "manual",
    "run_mode": "local_to_aggdaily",
    "dry_run": false,
    "force_replace": true,
    "from_day_utc": "2026-02-01",
    "to_day_utc": "2026-02-01",
    "connector_ids": [4]
  }' | jq .
```

### 8) Query-string trigger example

```bash
curl -sS -X POST \
  "${SERVICE_URL}/run?run_mode=local_to_aggdaily&dry_run=true&from_day_utc=2026-02-01&to_day_utc=2026-02-03&connector_ids=4,7" \
  -H "authorization: Bearer ${ID_TOKEN}" | jq .
```

### 9) Trigger stub modes (Phase 1 validation)

```bash
curl -sS -X POST "${SERVICE_URL}/run" \
  -H "authorization: Bearer ${ID_TOKEN}" \
  -H 'content-type: application/json' \
  -d '{"run_mode":"obs_aqi_to_r2","dry_run":true}' | jq .
```

```bash
curl -sS -X POST "${SERVICE_URL}/run" \
  -H "authorization: Bearer ${ID_TOKEN}" \
  -H 'content-type: application/json' \
  -d '{"run_mode":"source_to_all","dry_run":true}' | jq .
```

### 10) Direct job execution (without HTTP wrapper)

```bash
UK_AQ_BACKFILL_RUN_MODE=local_to_aggdaily \
UK_AQ_BACKFILL_TRIGGER_MODE=manual \
UK_AQ_BACKFILL_DRY_RUN=true \
UK_AQ_BACKFILL_FROM_DAY_UTC=2026-02-01 \
UK_AQ_BACKFILL_TO_DAY_UTC=2026-02-05 \
UK_AQ_BACKFILL_CONNECTOR_IDS=4,7 \
deno run --allow-env --allow-net --allow-read --allow-write --allow-run \
  workers/uk_aq_backfill_cloud_run/run_job.ts
```

### 11) Trigger configured Cloud Scheduler job manually

```bash
gcloud scheduler jobs run "<backfill-scheduler-job-name>" \
  --location "europe-west2"
```

The scheduler job payload defaults to `local_to_aggdaily` unless workflow vars override it.

### 12) Use ops helper script with required pre-set parameters

Script path:

- `scripts/gcp/uk_aq_backfill_cloud_run_call.sh`

The script enforces required parameters before making the call.

Required env vars:

- `UK_AQ_BACKFILL_SERVICE_URL`
- `UK_AQ_BACKFILL_TRIGGER_MODE` (`manual|scheduler`)
- `UK_AQ_BACKFILL_RUN_MODE` (`local_to_aggdaily|obs_aqi_to_r2|source_to_all`)
- `UK_AQ_BACKFILL_DRY_RUN` (`true|false`)
- `UK_AQ_BACKFILL_FORCE_REPLACE` (`true|false`)
- `UK_AQ_BACKFILL_FROM_DAY_UTC` (`YYYY-MM-DD`)
- `UK_AQ_BACKFILL_TO_DAY_UTC` (`YYYY-MM-DD`)

Optional:

- `UK_AQ_BACKFILL_CONNECTOR_IDS` (for example `4,7`)
- `UK_AQ_BACKFILL_ENABLE_R2_FALLBACK` (`true|false`, default `false`)
- `UK_AQ_BACKFILL_REQUEST_TIMEOUT_SECONDS` (default `300`)
- `UK_AQ_BACKFILL_ID_TOKEN` (if omitted, script first tries `gcloud auth print-identity-token --audiences "${UK_AQ_BACKFILL_SERVICE_URL}"`, then falls back to `gcloud auth print-identity-token`)

Example:

```bash
export UK_AQ_BACKFILL_SERVICE_URL="${SERVICE_URL}"
export UK_AQ_BACKFILL_TRIGGER_MODE="manual"
export UK_AQ_BACKFILL_RUN_MODE="local_to_aggdaily"
export UK_AQ_BACKFILL_DRY_RUN="false"
export UK_AQ_BACKFILL_FORCE_REPLACE="true"
export UK_AQ_BACKFILL_FROM_DAY_UTC="2026-02-01"
export UK_AQ_BACKFILL_TO_DAY_UTC="2026-02-01"
export UK_AQ_BACKFILL_CONNECTOR_IDS="4"

./scripts/gcp/uk_aq_backfill_cloud_run_call.sh
```
