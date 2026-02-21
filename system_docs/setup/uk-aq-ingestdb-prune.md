# uk-aq-ingestdb-prune behavior

This document describes what the prune function does at runtime.

## Purpose

`POST /run` verifies that ingest observations older than 7 days are present in history with identical content, then deletes only verified ingest buckets.

Bucket key is:

- `connector_id`
- `hour_start` (`date_trunc('hour', observed_at)` in UTC)

## Core flow

1. Build UTC window:
- `window_end = UTC midnight today - 7 days`
- `window_start = window_end - MAX_HOURS_PER_RUN`

2. Fetch hourly summaries via RPC from both DBs:
- ingest: `uk_aq_public.uk_aq_rpc_observations_hourly_fingerprint`
- history: `uk_aq_public.uk_aq_rpc_observations_hourly_fingerprint`

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

When `DRY_RUN=true`, no delete RPC is called.

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

When `DRY_RUN=false`, the flow is:

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

- `DRY_RUN` (default `true`)
- `MAX_HOURS_PER_RUN` (default `48`)
- `DELETE_BATCH_SIZE` (default `50000`)
- `MAX_DELETE_BATCHES_PER_HOUR` (default `10`)
- `REPAIR_ONE_MISMATCH_BUCKET` (default `true`)
- `REPAIR_BUCKET_OUTBOX_CHUNK_SIZE` (default `1000`)
- `FLUSH_CLAIM_BATCH_LIMIT` (default `20`)
- `MAX_FLUSH_BATCHES` (default `30`)

## Related SQL scripts

- `sql/ingest_db_ops_rpcs.sql`
- `sql/history_db_ops_rpcs.sql`

For deployment and scheduler wiring, use:

- `README.md`
- `.github/workflows/uk_aq_ingestdb_prune_cloud_run_deploy.yml`
