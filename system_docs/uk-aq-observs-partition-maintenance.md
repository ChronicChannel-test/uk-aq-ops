# uk-aq-observs-partition-maintenance setup (Cloud Run + Scheduler)

This deploys a dedicated Cloud Run service that maintains `uk_aq_observs.observations` partitions in `obs_aqidb`.

## Runtime behavior

`POST /run` performs:
- create/ensure UTC-day partitions through `today + 3 days`
- enforce hot/cold index policy
  - hot: previous 2 UTC days + today + next 3 UTC days -> unique btree on `(connector_id, timeseries_id, observed_at)` + BRIN(observed_at)
  - cold: BRIN(observed_at) only
- default partition diagnostics (`count`, min/max observed_at, top offenders)
- retention drops based on DST-aware cutoff from Europe/London local days (`31 complete local days + today`)
- R2 History manifest gate before each drop:
  - HEAD `history/v1/observations/day_utc=YYYY-MM-DD/manifest.json` in Cloudflare R2
  - if not confirmed, skip drop and log `SKIP DROP — history manifest not confirmed`

## Required environment variables

- `OBS_AQIDB_SUPABASE_URL`
- `OBS_AQIDB_SECRET_KEY`

## Optional environment variables

Partition policy controls:
- `OBSERVS_PARTITIONS_FUTURE_DAYS` (policy-fixed to `3`)
- `OBSERVS_PARTITIONS_HOT_DAYS` (default `3`)
- `OBSERVS_COMPLETE_LOCAL_DAYS` (default `31`)
- `OBSERVS_DEFAULT_TOP_N` (default `20`)
- `OBSERVS_PARTITION_DROP_DRY_RUN` (default `false`)

Dropbox logging:
- `UK_AQ_DROPBOX_ROOT` (optional)
- `UK_AQ_OBSERVS_PARTITION_DROPBOX_FOLDER` (default `/observs_partition_maintenance`)
- `UK_AIR_ERROR_DROPBOX_ALLOWED_SUPABASE_URL` (optional allowlist)
- `DROPBOX_APP_KEY`
- `DROPBOX_APP_SECRET`
- `DROPBOX_REFRESH_TOKEN`

Cloudflare R2 history-check placeholders (S3-compatible API):
- `CFLARE_R2_ENDPOINT` (or `R2_ENDPOINT`)
- `CFLARE_R2_BUCKET` (or `R2_BUCKET`)
- `CFLARE_R2_ACCESS_KEY_ID` (or `R2_ACCESS_KEY_ID`)
- `CFLARE_R2_SECRET_ACCESS_KEY` (or `R2_SECRET_ACCESS_KEY`)
- `CFLARE_R2_REGION` (or `R2_REGION`, default `auto`)
- `UK_AQ_R2_HISTORY_OBSERVATIONS_PREFIX` (default `history/v1/observations`)

## Local run

```bash
npm install
npm run start:observs-partitions
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

Apply in obs_aqidb:
- `../CIC-Test-UK-AQ-Schema/CIC-test-uk-aq-schema/schemas/observs_db/observs_db_ops_rpcs.sql`

The service expects these RPCs to exist:
- `uk_aq_public.uk_aq_rpc_observs_ensure_daily_partitions`
- `uk_aq_public.uk_aq_rpc_observs_enforce_hot_cold_indexes`
- `uk_aq_public.uk_aq_rpc_observs_observations_default_diagnostics`
- `uk_aq_public.uk_aq_rpc_observs_drop_candidates`
- `uk_aq_public.uk_aq_rpc_observs_drop_partition`
