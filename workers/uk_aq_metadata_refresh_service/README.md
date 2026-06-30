# UK AQ metadata refresh service

Scheduled Cloud Run job for coalescing Supabase metadata-refresh outbox rows into one safe R2 refresh batch.

## Flow

Every five minutes Cloud Scheduler runs this job. The job:

1. Checks `uk_aq_ops.metadata_refresh_requests` for `pending` or `retryable` `core_metadata_refresh` rows.
2. Exits immediately when there is no pending work.
3. Exits without processing when the oldest pending row is younger than `UK_AQ_METADATA_REFRESH_QUIET_PERIOD_SECONDS` (default `120`).
4. Claims all eligible rows in one transaction using `FOR UPDATE SKIP LOCKED` and marks them `processing` with a shared `batch_id`.
5. Runs the R2 core metadata snapshot command.
6. Runs the latest-snapshot builder with `UK_AQ_LATEST_SNAPSHOT_FORCE_METADATA_REFRESH=1`.
7. Logs `metadata_refresh_cache_purge_skipped`; no cache purge is performed by default because the public cache proxy uses the realtime latest-snapshot profile and should refresh naturally after its short TTL.
8. Marks the batch `completed`, or `retryable`/`failed` with `last_error` if anything fails.

## Required setup

Apply `db/migrations/20260629_metadata_refresh_requests.sql` to the Obs AQI Supabase database before deploying the job.

Required runtime environment/secrets:

- `OBS_AQIDB_DB_URL` (or `OBS_AQIDB_DATABASE_URL`, `SUPABASE_DB_URL`, `DATABASE_URL`)
- R2 config used by `scripts/backup_r2/uk_aq_core_snapshot_to_r2.mjs`
- R2 config used by `workers/uk_aq_latest_snapshot_cloud_run/run_job.ts`

Optional command overrides:

- `UK_AQ_METADATA_REFRESH_CORE_SNAPSHOT_COMMAND`
- `UK_AQ_METADATA_REFRESH_LATEST_SNAPSHOT_COMMAND`
