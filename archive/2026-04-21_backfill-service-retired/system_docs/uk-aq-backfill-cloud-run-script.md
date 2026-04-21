# UK AQ Backfill Cloud Run (Quick Run)

Quick invocation guide for the ops helper script:

- `scripts/gcp/uk_aq_backfill_cloud_run_call.sh`
- `scripts/uk_aq_backfill_local_monthly.sh` (local month-by-month wrapper for `run_job.ts`)

Full setup/runbook remains here:

- `system_docs/uk-aq-backfill-cloud-run.md`

## Required env vars

- `UK_AQ_BACKFILL_SERVICE_URL`
- `UK_AQ_BACKFILL_TRIGGER_MODE` (`manual|scheduler`)
- `UK_AQ_BACKFILL_RUN_MODE` (`local_to_aqilevels|obs_aqi_to_r2|source_to_r2|r2_history_obs_to_aqilevels`)
- `UK_AQ_BACKFILL_DRY_RUN` (`true|false`)
- `UK_AQ_BACKFILL_FORCE_REPLACE` (`true|false`)
- `UK_AQ_BACKFILL_FROM_DAY_UTC` (`YYYY-MM-DD`)
- `UK_AQ_BACKFILL_TO_DAY_UTC` (`YYYY-MM-DD`)

## Optional env vars

- `UK_AQ_BACKFILL_CONNECTOR_IDS` (example `4,7`)
- `UK_AQ_BACKFILL_ENABLE_R2_FALLBACK` (`true|false`, default `false`)
- `UK_AQ_R2_HISTORY_CORE_PREFIX` (default `history/v1/core`; used for R2 core metadata lookup)
- `UK_AQ_BACKFILL_R2_CORE_LOOKBACK_DAYS` (default `45`)
- `UK_AQ_BACKFILL_R2_CORE_SNAPSHOT_MAX_BYTES` (default `250000000`)
- `UK_AQ_BACKFILL_BREATHELONDON_SOURCE_ENABLED` (`true|false`, default `true`)
- `UK_AQ_BACKFILL_BREATHELONDON_CONNECTOR_CODE` (default `breathelondon`)
- `UK_AQ_BACKFILL_BREATHELONDON_CONNECTOR_ID_FALLBACK` (default `3`)
- `UK_AQ_BACKFILL_BREATHELONDON_BASE_URL` (default `https://api.breathelondon-communities.org/api`)
- `BREATHELONDON_API_KEY` (required when Breathe London source backfill is enabled)
- `UK_AQ_BACKFILL_BREATHELONDON_TIMEOUT_MS` (default `60000`)
- `UK_AQ_BACKFILL_BREATHELONDON_FETCH_RETRIES` (default `3`)
- `UK_AQ_BACKFILL_BREATHELONDON_RETRY_BASE_MS` (default `1500`)
- `UK_AQ_BACKFILL_BREATHELONDON_RAW_MIRROR_ROOT` (optional local mirror root for Breathe London day/site/species JSON replay)
- `UK_AQ_BACKFILL_SCOMM_SOURCE_ENABLED` (`true|false`, default `true`)
- `UK_AQ_BACKFILL_SCOMM_CONNECTOR_CODE` (default `sensorcommunity`)
- `UK_AQ_BACKFILL_SCOMM_ARCHIVE_BASE_URL` (default `https://archive.sensor.community`)
- `UK_AQ_BACKFILL_SCOMM_INCLUDE_MET_FIELDS` (`true|false`, default `true`)
- `UK_AQ_BACKFILL_SCOMM_ARCHIVE_TIMEOUT_MS` (default `120000`)
- `UK_AQ_BACKFILL_SCOMM_ARCHIVE_FETCH_RETRIES` (default `3`)
- `UK_AQ_BACKFILL_SCOMM_ARCHIVE_RETRY_BASE_MS` (default `1500`)
- `UK_AQ_BACKFILL_SCOMM_RAW_MIRROR_ROOT` (optional local mirror root for archive CSV replay)
- `UK_AQ_BACKFILL_OPENAQ_SOURCE_ENABLED` (`true|false`, default `true`)
- `UK_AQ_BACKFILL_OPENAQ_CONNECTOR_CODE` (default `openaq`)
- `UK_AQ_BACKFILL_OPENAQ_CONNECTOR_ID_FALLBACK` (optional numeric fallback connector id)
- `UK_AQ_BACKFILL_OPENAQ_ARCHIVE_BASE_URL` (default `https://openaq-data-archive.s3.amazonaws.com`)
- `UK_AQ_BACKFILL_OPENAQ_INCLUDE_MET_FIELDS` (`true|false`, default `true`)
- `UK_AQ_BACKFILL_OPENAQ_ARCHIVE_TIMEOUT_MS` (default `120000`)
- `UK_AQ_BACKFILL_OPENAQ_ARCHIVE_FETCH_RETRIES` (default `3`)
- `UK_AQ_BACKFILL_OPENAQ_ARCHIVE_RETRY_BASE_MS` (default `1500`)
- `UK_AQ_BACKFILL_OPENAQ_RAW_MIRROR_ROOT` (optional local mirror root for OpenAQ `.csv.gz` replay; local `run_job.ts` only)
- `UK_AQ_BACKFILL_REQUEST_TIMEOUT_SECONDS` (default `300`)
- `UK_AQ_BACKFILL_ID_TOKEN` (if unset, script tries `gcloud auth print-identity-token --audiences "${UK_AQ_BACKFILL_SERVICE_URL}"`, then falls back to `gcloud auth print-identity-token`)

## How To Pass Parameters On The Command Line

The script reads parameters from environment variables (not `--flags`).
Use one of these patterns.

### 1) One-off run with inline vars (single command)

```bash
UK_AQ_BACKFILL_SERVICE_URL="${SERVICE_URL}" \
UK_AQ_BACKFILL_TRIGGER_MODE="manual" \
UK_AQ_BACKFILL_RUN_MODE="local_to_aqilevels" \
UK_AQ_BACKFILL_DRY_RUN="false" \
UK_AQ_BACKFILL_FORCE_REPLACE="true" \
UK_AQ_BACKFILL_FROM_DAY_UTC="2026-02-01" \
UK_AQ_BACKFILL_TO_DAY_UTC="2026-02-01" \
UK_AQ_BACKFILL_CONNECTOR_IDS="4" \
./scripts/gcp/uk_aq_backfill_cloud_run_call.sh
```

### 2) Connector-targeted run (`source_to_r2`)

```bash
UK_AQ_BACKFILL_SERVICE_URL="${SERVICE_URL}" \
UK_AQ_BACKFILL_TRIGGER_MODE="manual" \
UK_AQ_BACKFILL_RUN_MODE="source_to_r2" \
UK_AQ_BACKFILL_DRY_RUN="false" \
UK_AQ_BACKFILL_FORCE_REPLACE="true" \
UK_AQ_BACKFILL_FROM_DAY_UTC="2026-02-11" \
UK_AQ_BACKFILL_TO_DAY_UTC="2026-02-15" \
UK_AQ_BACKFILL_CONNECTOR_IDS="4" \
./scripts/gcp/uk_aq_backfill_cloud_run_call.sh
```

### 2a) AQI rebuild from committed R2 observation history

```bash
UK_AQ_BACKFILL_SERVICE_URL="${SERVICE_URL}" \
UK_AQ_BACKFILL_TRIGGER_MODE="manual" \
UK_AQ_BACKFILL_RUN_MODE="r2_history_obs_to_aqilevels" \
UK_AQ_BACKFILL_DRY_RUN="false" \
UK_AQ_BACKFILL_FORCE_REPLACE="true" \
UK_AQ_BACKFILL_FROM_DAY_UTC="2025-01-01" \
UK_AQ_BACKFILL_TO_DAY_UTC="2025-01-31" \
./scripts/gcp/uk_aq_backfill_cloud_run_call.sh
```

### 3) Use `env` explicitly

```bash
env \
  UK_AQ_BACKFILL_SERVICE_URL="${SERVICE_URL}" \
  UK_AQ_BACKFILL_TRIGGER_MODE="manual" \
  UK_AQ_BACKFILL_RUN_MODE="local_to_aqilevels" \
  UK_AQ_BACKFILL_DRY_RUN="true" \
  UK_AQ_BACKFILL_FORCE_REPLACE="false" \
  UK_AQ_BACKFILL_FROM_DAY_UTC="2026-02-01" \
  UK_AQ_BACKFILL_TO_DAY_UTC="2026-02-05" \
  ./scripts/gcp/uk_aq_backfill_cloud_run_call.sh
```

### 4) Export once, then run repeatedly

```bash
export UK_AQ_BACKFILL_SERVICE_URL="${SERVICE_URL}"
export UK_AQ_BACKFILL_TRIGGER_MODE="manual"
export UK_AQ_BACKFILL_RUN_MODE="local_to_aqilevels"
export UK_AQ_BACKFILL_DRY_RUN="false"
export UK_AQ_BACKFILL_FORCE_REPLACE="false"
export UK_AQ_BACKFILL_FROM_DAY_UTC="2026-02-01"
export UK_AQ_BACKFILL_TO_DAY_UTC="2026-02-05"
export UK_AQ_BACKFILL_CONNECTOR_IDS="4,7"

./scripts/gcp/uk_aq_backfill_cloud_run_call.sh
```

### 5) Override only one or two params for a single run

```bash
UK_AQ_BACKFILL_DRY_RUN="true" \
UK_AQ_BACKFILL_FORCE_REPLACE="true" \
./scripts/gcp/uk_aq_backfill_cloud_run_call.sh
```

## Notes

- Use actual values, not placeholders like `true|false`.
- `UK_AQ_BACKFILL_FROM_DAY_UTC`, `UK_AQ_BACKFILL_TO_DAY_UTC`, and `UK_AQ_BACKFILL_CONNECTOR_IDS` can all be passed inline in the command (examples above).
- All listed parameters can be passed in-command the same way.

## Local Monthly Wrapper (No Cloud Run)

Script:

- `scripts/uk_aq_backfill_local_monthly.sh`

Purpose:

- Split one local backfill window into month-sized runs.
- Execute `workers/uk_aq_backfill_cloud_run/run_job.ts` once per month.
- Write one log file per month (default `logs/backfill/monthly/`).
  - pattern: `<run_mode>_<run_started_at_utc>_<connector_ids_or_all>_<month_from>_to_<month_to>.log`
  - `run_started_at_utc` format: `YYYY-MM-DD_HH-MM-SS`
  - example: `source_to_r2_2026-03-14_14-05-12_3_2026-01-01_to_2026-01-31.log`

Example: all available source adapters/connectors for 2025

```bash
export UK_AQ_BACKFILL_TRIGGER_MODE="manual"
export UK_AQ_BACKFILL_RUN_MODE="r2_history_obs_to_aqilevels"
export UK_AQ_BACKFILL_DRY_RUN="false"
export UK_AQ_BACKFILL_FORCE_REPLACE="true"
export UK_AQ_BACKFILL_FROM_DAY_UTC="2025-01-01"
export UK_AQ_BACKFILL_TO_DAY_UTC="2025-01-31"
unset UK_AQ_BACKFILL_CONNECTOR_IDS

./scripts/uk_aq_backfill_local_monthly.sh
```

Optional wrapper env vars:

- `UK_AQ_BACKFILL_MONTHLY_LOG_DIR` (default `logs/backfill/monthly`)
- `UK_AQ_BACKFILL_MONTHLY_STOP_ON_ERROR` (`true|false`, default `true`)
- `UK_AQ_BACKFILL_MONTH_RUN_INTERVAL_SECONDS` (default `0`; pause between monthly runs, applies to all connectors)
- `UK_AQ_BACKFILL_MONTHLY_PAUSE_SECONDS` (legacy alias for `UK_AQ_BACKFILL_MONTH_RUN_INTERVAL_SECONDS`)

Behavior with future connectors and `UK_AQ_BACKFILL_FORCE_REPLACE=false`:

- Existing backed-up connector/day outputs are skipped.
- Newly available supported connectors are processed for missing connector/day outputs.
- Non-dry `source_to_r2` and `r2_history_obs_to_aqilevels` monthly runs rebuild:
  - `history/_index/observations_latest.json`
  - `history/_index/aqilevels_latest.json`
  - `history/_index/observations_timeseries_latest.json`
  - `history/_index/observations_timeseries/day_utc=YYYY-MM-DD/connector_id=<id>/manifest.json`
  after the monthly batch completes.

## Original Example (export style)

```bash
export UK_AQ_BACKFILL_SERVICE_URL="${SERVICE_URL}"
export UK_AQ_BACKFILL_TRIGGER_MODE="manual"
export UK_AQ_BACKFILL_RUN_MODE="local_to_aqilevels"
export UK_AQ_BACKFILL_DRY_RUN="false"
export UK_AQ_BACKFILL_FORCE_REPLACE="true"
export UK_AQ_BACKFILL_FROM_DAY_UTC="2026-02-01"
export UK_AQ_BACKFILL_TO_DAY_UTC="2026-02-01"
export UK_AQ_BACKFILL_CONNECTOR_IDS="4"

./scripts/gcp/uk_aq_backfill_cloud_run_call.sh
```
