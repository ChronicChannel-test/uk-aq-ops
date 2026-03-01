# uk-aq-history-partition-maintenance setup (Cloud Run + Scheduler)

This deploys a dedicated Cloud Run service that maintains `uk_aq_history.observations` partitions in history DB.

## Runtime behavior

`POST /run` performs:
- create/ensure UTC-day partitions through `today + 3 days`
- enforce hot/cold index policy
  - hot: previous 2 UTC days + today + next 3 UTC days -> unique btree on `(connector_id, timeseries_id, observed_at)` + BRIN(observed_at)
  - cold: BRIN(observed_at) only
- default partition diagnostics (`count`, min/max observed_at, top offenders)
- retention drops based on DST-aware cutoff from Europe/London local days (`31 complete local days + today`)
- backup gate before each drop:
  - HEAD `_SUCCESS` marker in Cloudflare R2
  - fallback list-prefix requiring at least one `.parquet`
  - if not confirmed, skip drop and log `SKIP DROP — backup not confirmed`

## Required environment variables

- `HISTORY_SUPABASE_URL` (or `HISTORY_URL`)
- `HISTORY_SECRET_KEY`

## Optional environment variables

Partition policy controls:
- `HISTORY_PARTITIONS_FUTURE_DAYS` (policy-fixed to `3`)
- `HISTORY_PARTITIONS_HOT_DAYS` (default `3`)
- `HISTORY_COMPLETE_LOCAL_DAYS` (default `31`)
- `HISTORY_DEFAULT_TOP_N` (default `20`)
- `HISTORY_PARTITION_DROP_DRY_RUN` (default `false`)

Dropbox logging:
- `UK_AQ_DROPBOX_ROOT` (optional)
- `UK_AQ_HISTORY_PARTITION_DROPBOX_FOLDER` (default `/history_partition_maintenance`)
- `UK_AIR_ERROR_DROPBOX_ALLOWED_SUPABASE_URL` (optional allowlist)
- `DROPBOX_APP_KEY`
- `DROPBOX_APP_SECRET`
- `DROPBOX_REFRESH_TOKEN`

Cloudflare R2 backup-check placeholders (S3-compatible API):
- `CFLARE_R2_ENDPOINT` (or `R2_ENDPOINT`)
- `CFLARE_R2_BUCKET` (or `R2_BUCKET`)
- `CFLARE_R2_ACCESS_KEY_ID` (or `R2_ACCESS_KEY_ID`)
- `CFLARE_R2_SECRET_ACCESS_KEY` (or `R2_SECRET_ACCESS_KEY`)
- `CFLARE_R2_REGION` (or `R2_REGION`, default `auto`)
- `CFLARE_R2_OBSERVATIONS_PREFIX` (or `R2_OBSERVATIONS_PREFIX`, default `uk_aq_history/observations`)

## Local run

```bash
npm install
npm run start:history-partitions
```

Run once:

```bash
curl -X POST "http://localhost:8080/run"
```

Dry-run partition drop gate:

```bash
curl -X POST "http://localhost:8080/run?dropDryRun=true"
```

## Scheduler

Recommended schedule: `0 3 * * *` with timezone `UTC`.

Target service endpoint:
- `POST /run`

## SQL prerequisites

Apply in history DB:
- `sql/history_db_ops_rpcs.sql`

The service expects these RPCs to exist:
- `uk_aq_public.uk_aq_rpc_history_ensure_daily_partitions`
- `uk_aq_public.uk_aq_rpc_history_enforce_hot_cold_indexes`
- `uk_aq_public.uk_aq_rpc_history_observations_default_diagnostics`
- `uk_aq_public.uk_aq_rpc_history_drop_candidates`
- `uk_aq_public.uk_aq_rpc_history_drop_partition`
