# uk-aq-ingestdb-prune behavior

This document describes what the prune function does at runtime.

## Purpose

`POST /run` verifies that ingest observations older than 7 days are present in history with identical content, then deletes only verified ingest buckets.

Bucket key is:

- `connector_id`
- `hour_start` (`date_trunc('hour', observed_at)` in UTC)

## Core flow

1. Build UTC window:
- `window_end = UTC midnight today - INGESTDB_RETENTION_DAYS`
- `window_start = window_end - MAX_HOURS_PER_RUN`
- If `MAX_HOURS_PER_RUN > 24`, split into sequential 24-hour internal batches and process each batch in order.

0. Phase B pre-prune backup gate:
- Before fingerprint compare/delete work, the service runs Phase B backup for closed UTC days (`day_utc <= utc_today - 8 days`).
- Source rows are streamed from `uk_aq_core.observations` by `(day_utc, connector_id)` and written to R2 Parquet with ZSTD compression.
- Part rollover defaults to `1,000,000` rows per file.
- Backup writes to staging first (`backup/staging/run_id=...`) then copies to committed prefix (`backup/observations/...`), writes manifests, verifies object existence, and updates:
  - `uk_aq_ops.backup_candidates`
  - `uk_aq_ops.prune_day_gates.backup_done`
- Prune deletion for an hour bucket is allowed only when that bucket day has `backup_done=true`.

2. Fetch hourly summaries via RPC from both DBs:
- ingest: `uk_aq_public.uk_aq_rpc_observations_hourly_fingerprint`
- history: `uk_aq_public.uk_aq_rpc_observations_hourly_fingerprint`
- Hash inputs include `connector_id`, `timeseries_id`, `observed_at`, and `value` (status is excluded in both DBs).

3. Compare buckets by `(connector_id, hour_start)`:
- missing in history -> mismatch
- count differs -> mismatch
- fingerprint differs -> mismatch
- count and fingerprint equal -> deletable

Comparison scope rule:

- A bucket must exist in ingest to be checked for parity and considered for deletion.
- Buckets that exist only in history are not treated as mismatches for delete gating.

4. Log structured results:
- mismatches at `ERROR`
- deletable plan at `INFO`
- history-only buckets at `INFO` with event `history_extra_buckets`

## Dry-run behavior

When `INGESTDB_PRUNE_DRY_RUN=true`, no delete RPC is called.

If `REPAIR_ONE_MISMATCH_BUCKET=true`, dry-run also runs a repair pilot for one mismatch bucket:

1. Enqueue that bucket’s rows to ingest outbox via:
- `uk_aq_public.uk_aq_rpc_history_outbox_enqueue_hour_bucket`

2. Flush outbox immediately, inside the prune run itself:
- claim: `uk_aq_public.uk_aq_rpc_history_outbox_claim`
- upsert to history: `uk_aq_public.uk_aq_rpc_history_observations_upsert`
- receipts upsert: `uk_aq_public.uk_aq_rpc_history_sync_receipt_daily_upsert`
- resolve: `uk_aq_public.uk_aq_rpc_history_outbox_resolve`

3. Recheck that same bucket with hourly fingerprint RPCs.

Important:

- The prune function does its own outbox flush logic in-process.
- It does not call the separate `uk-aq-history-outbox-flush-service` endpoint.

## Live delete behavior

When `INGESTDB_PRUNE_DRY_RUN=false`, the flow is:

1. First compare pass and first delete pass:
- delete all buckets that already match
- log mismatches

2. Repair phase:
- enqueue all repairable mismatch buckets into history outbox
- flush outbox in-process (same RPC chain as dry-run pilot)

3. Recheck phase:
- re-run compare for the original mismatch buckets
- delete buckets that are now verified
- log buckets that still mismatch after repair

Only one repair/recheck cycle is executed per run.

Delete RPC:

- `uk_aq_public.uk_aq_rpc_observations_delete_hour_bucket`

Each bucket is deleted in bounded batches until:

- delete RPC returns `0` (drained), or
- `MAX_DELETE_BATCHES_PER_HOUR` is reached (warning + alert condition)

## Guardrails

- All date/hour logic is UTC.
- No raw row comparison is moved out of DBs for verification; only bucket aggregates are compared.
- Buckets with any mismatch are skipped from delete in each pass.
- `history_count > ingest_count` is logged as a specific error condition and not deleted.

## Logging details

- Logs are structured JSON on Cloud Run stdout/stderr and appear in Cloud Logging.
- On fatal run errors (`ingestdb_prune_run_error`, HTTP `500`), the service also attempts a Dropbox error upload when Dropbox env/secrets are configured.
- Dropbox error path format: `<UK_AQ_DROPBOX_ROOT>/error_log/YYYY-MM-DD/uk_aq_error_cloud_run_ingestdb_prune_<timestamp>_<uuid>.json`.
- Dropbox upload uses:
  - `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN`
  - optional `UK_AQ_DROPBOX_ROOT`
  - optional `UK_AIR_ERROR_DROPBOX_FOLDER` (default `/error_log`)
  - optional allowlist gate `UK_AIR_ERROR_DROPBOX_ALLOWED_SUPABASE_URL` (must match `SUPABASE_URL`/`SB_URL` when set)
- For batched runs (`MAX_HOURS_PER_RUN > 24`), the service logs:
  - `ingestdb_prune_batch_plan` at run start
  - per-batch `ingestdb_prune_run_start`
  - one final aggregate summary event (`ingestdb_prune_dry_run_batched_summary` or `ingestdb_prune_delete_batched_summary`)
- History-only buckets (present in history, missing in ingest) are logged once per run as:
  - `severity=INFO`
  - `event=history_extra_buckets`
  - fields: `count` and `sample` (sample bucket list)
- They are informational only and do not block deletes.
- This is expected after successful prune runs, because deleted ingest buckets will remain in history.

## Runtime inputs

Required:

- `SUPABASE_URL`
- `HISTORY_SUPABASE_URL`
- `SB_SECRET_KEY`
- `HISTORY_SECRET_KEY`

Key optional controls:

- `INGESTDB_PRUNE_DRY_RUN` (default `true`)
- `INGESTDB_RETENTION_DAYS` (default `7`)
- `MAX_HOURS_PER_RUN` (default `48`)
- `DELETE_BATCH_SIZE` (default `50000`)
- `MAX_DELETE_BATCHES_PER_HOUR` (default `10`)
- `REPAIR_ONE_MISMATCH_BUCKET` (default `true`)
- `REPAIR_BUCKET_OUTBOX_CHUNK_SIZE` (default `1000`)
- `FLUSH_CLAIM_BATCH_LIMIT` (default `20`)
- `MAX_FLUSH_BATCHES` (default `30`)
- `BACKUP_PHASE_B_ENABLED` (default `true`)
- `BACKUP_PART_MAX_ROWS` (default `1000000`)
- `BACKUP_CURSOR_FETCH_ROWS` (default `20000`)
- `BACKUP_ROW_GROUP_SIZE` (default `100000`)
- `BACKUP_MAX_CANDIDATES_PER_RUN` (default `500`)
- `BACKUP_STAGING_RETENTION_DAYS` (default `7`)
- `BACKUP_STAGING_PREFIX` (default `backup/staging`)
- `BACKUP_COMMITTED_PREFIX` (default `backup/observations`)
- `BACKUP_RUNS_PREFIX` (default `backup/runs`)
- `UK_AQ_DEPLOY_ENV` (`dev|stage|prod`; default `dev`)

Phase B required env/secrets:

- `SUPABASE_DB_URL` (direct Postgres URL for streaming cursor reads)
- `CFLARE_R2_ENDPOINT`
- `CFLARE_R2_REGION` (default `auto`)
- bucket mapping: `R2_BUCKET_PROD` / `R2_BUCKET_STAGE` / `R2_BUCKET_DEV` (or fallback `CFLARE_R2_BUCKET`)
- `CFLARE_R2_ACCESS_KEY_ID`
- `CFLARE_R2_SECRET_ACCESS_KEY`

## Related SQL scripts

- `sql/ingest_db_ops_rpcs.sql`
- `sql/history_db_ops_rpcs.sql`
- Fingerprint RPCs depend on `pgcrypto.digest`; keep `pgcrypto` installed and accessible via function `search_path` (for example `extensions` and/or `public`).

For deployment and scheduler wiring, use:

- `README.md`
- `.github/workflows/uk_aq_prune_daily_cloud_run_deploy.yml`
