# UK AQ Backfill Cloud Run (Quick Run)

Quick invocation guide for the ops helper script:

- `scripts/gcp/uk_aq_backfill_cloud_run_call.sh`

Full setup/runbook remains here:

- `system_docs/uk-aq-backfill-cloud-run.md`

## Required env vars

- `UK_AQ_BACKFILL_SERVICE_URL`
- `UK_AQ_BACKFILL_TRIGGER_MODE` (`manual|scheduler`)
- `UK_AQ_BACKFILL_RUN_MODE` (`local_to_aqilevels|obs_aqi_to_r2|source_to_r2`)
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
- `UK_AQ_BACKFILL_SCOMM_SOURCE_ENABLED` (`true|false`, default `true`)
- `UK_AQ_BACKFILL_SCOMM_CONNECTOR_CODE` (default `sensorcommunity`)
- `UK_AQ_BACKFILL_SCOMM_ARCHIVE_BASE_URL` (default `https://archive.sensor.community`)
- `UK_AQ_BACKFILL_SCOMM_INCLUDE_MET_FIELDS` (`true|false`, default `true`)
- `UK_AQ_BACKFILL_SCOMM_ARCHIVE_TIMEOUT_MS` (default `120000`)
- `UK_AQ_BACKFILL_SCOMM_RAW_MIRROR_ROOT` (optional local mirror root for archive CSV replay)
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

### 2) Use `env` explicitly

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

### 3) Export once, then run repeatedly

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

### 4) Override only one or two params for a single run

```bash
UK_AQ_BACKFILL_DRY_RUN="true" \
UK_AQ_BACKFILL_FORCE_REPLACE="true" \
./scripts/gcp/uk_aq_backfill_cloud_run_call.sh
```

## Notes

- Use actual values, not placeholders like `true|false`.
- `UK_AQ_BACKFILL_FROM_DAY_UTC`, `UK_AQ_BACKFILL_TO_DAY_UTC`, and `UK_AQ_BACKFILL_CONNECTOR_IDS` can all be passed inline in the command (examples above).
- All listed parameters can be passed in-command the same way.

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
