# UK AQ Backfill Cloud Run Setup

Phase 1 scope:

- `local_to_aggdaily` implemented
- `history_to_r2` stubbed
- `source_to_all` stubbed

## Runtime

- Service: `workers/uk_aq_backfill_cloud_run/run_service.ts`
- Job: `workers/uk_aq_backfill_cloud_run/run_job.ts`
- Dockerfile: `workers/uk_aq_backfill_cloud_run/Dockerfile`

## Required Secrets/Variables (Phase 1)

Required variables:

- `SUPABASE_URL`
- `HISTORY_SUPABASE_URL`
- `AGGDAILY_SUPABASE_URL`

Required secrets:

- `SB_SECRET_KEY`
- `HISTORY_SECRET_KEY`
- `AGGDAILY_SECRET_KEY`

## Optional Runtime Controls

- `UK_AQ_BACKFILL_RUN_MODE=local_to_aggdaily|history_to_r2|source_to_all`
- `UK_AQ_BACKFILL_TRIGGER_MODE=manual|scheduler`
- `UK_AQ_BACKFILL_DRY_RUN=true|false`
- `UK_AQ_BACKFILL_FORCE_REPLACE=true|false`
- `UK_AQ_BACKFILL_FROM_DAY_UTC=YYYY-MM-DD`
- `UK_AQ_BACKFILL_TO_DAY_UTC=YYYY-MM-DD`
- `UK_AQ_BACKFILL_CONNECTOR_IDS_CSV=4,7,11`
- `UK_AQ_BACKFILL_ENABLE_R2_FALLBACK=true|false`

Retention helper settings:

- `UK_AQ_BACKFILL_INGEST_RETENTION_DAYS` (default `7`)
- `UK_AQ_BACKFILL_HISTORY_LOCAL_RETENTION_DAYS` (default `31`)
- `UK_AQ_BACKFILL_LOCAL_TIMEZONE` (default `Europe/London`)

## Manual Trigger

```json
{
  "trigger_mode": "manual",
  "run_mode": "local_to_aggdaily",
  "dry_run": true,
  "from_day_utc": "2026-02-01",
  "to_day_utc": "2026-02-05",
  "connector_ids": [4]
}
```

## Processing Rules (Phase 1)

- Window direction: newest selected UTC day first, then older days.
- Chunk unit: one UTC day, per connector.
- Skip behavior: complete checkpoints are skipped by default.
- Force behavior: `force_replace=true` bypasses skip.
- Retry safety: checkpoints are updated only after non-dry-run completion.

## Retention Boundary Rule (used by `source_to_all` stub)

The helper computes retained UTC days from a rolling local-day window in `UK_AQ_BACKFILL_LOCAL_TIMEZONE`.
Because UK local days can be 23/24/25 hours across DST transitions, the retained UTC day count can be `31` or `32` for a 31 local-day window.

## Optional Ledger Schema

To enable persistent run/day/checkpoint tracking in AggDaily DB:

- Apply `workers/uk_aq_backfill_cloud_run/sql/uk_aq_backfill_ops_aggdaily.sql`

When not applied, the worker still runs, but cross-run checkpoint skip logic is unavailable.
