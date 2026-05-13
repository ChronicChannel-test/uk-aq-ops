# Daily Task Health

Daily task health tracks whether expected daily scheduled jobs reported for a UTC day. It is deliberately narrow: it stores status metadata in Supabase, does not integrate R2 in v1, does not track manual backfills, and does not duplicate existing ingest run dashboard data.

## V1 Tasks

| Task key | Display name | Platform | Source |
| --- | --- | --- | --- |
| `ops.prune_daily` | Prune daily | gcp | `uk-aq-ops` |
| `ops.observs_partition_maintenance` | Observations partition maintenance | gcp | `uk-aq-ops` |
| `ingest.stations_daily` | Stations daily | github | `uk-aq-ingest` |
| `ops.r2_history_dropbox_backup` | R2 history Dropbox backup | github | `uk-aq-ops` |
| `ops.supabase_db_dump_backup` | Supabase DB dump backup | gcp | `uk-aq-ops` |

## Tables

`uk_aq_ops.daily_task_definitions` defines the daily tasks that are expected. New tasks can be added by inserting rows. `scheduled_time_utc` is informational; `due_time_utc` is the threshold used to decide whether a missing report is still not due or has failed the day.

`uk_aq_ops.daily_task_runs` stores factual reports from scheduled task runs. Valid run statuses are `Started`, `Finished`, and `Failed`. Retries use a higher `attempt`; dashboard and report views use the latest attempt for each task/date.

`uk_aq_ops.daily_task_status` stores the computed calendar/report status for a UTC day. Valid computed/final statuses are `Unknown`, `Pending`, `Finished`, and `Failed`. Manual overrides are day-level only and never alter factual task run rows.

## Due Times

Daily workflows that report through GitHub can start later than intended schedule times, so `due_time_utc` is the reliable missing-run threshold. The seed data gives GitHub-origin tasks a larger grace period, including extra runtime allowance for the R2 Dropbox backup, while GCP jobs use about 30 minutes. Check seeded times against deployed scheduler timings before relying on them operationally.

## Phase 2 GCP Reporting

The ops GCP daily workers now report Started, Finished, and Failed through `workers/shared/daily_task_health.mjs`.

Instrumented workers:

| Task key | Worker |
| --- | --- |
| `ops.prune_daily` | `workers/uk_aq_prune_daily/server.mjs` |
| `ops.observs_partition_maintenance` | `workers/uk_aq_observs_partition_maintenance_service/server.mjs` |
| `ops.supabase_db_dump_backup` | `workers/uk_aq_supabase_db_dump_backup_service/server.mjs` |

Required Supabase configuration:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

The helper also accepts existing ops names where present:

- `OBS_AQIDB_SUPABASE_URL`
- `OBS_AQIDB_SECRET_KEY`
- `SB_URL`
- `SB_SECRET_KEY`

Optional controls:

- `DAILY_TASK_HEALTH_DISABLED=true` skips all health RPC calls and lets the worker continue normally.
- `DAILY_TASK_HEALTH_STRICT=true` makes health reporting errors fail the worker.
- `DAILY_TASK_HEALTH_RECOMPUTE_DISABLED=true` skips the recompute RPC after Finished/Failed.

Health reporting is best effort by default. Missing Supabase env vars or RPC failures are logged as warnings and do not make the scheduled task fail unless strict mode is enabled.

Local validation with reporting disabled:

```bash
DAILY_TASK_HEALTH_DISABLED=true npm run start:prune
DAILY_TASK_HEALTH_DISABLED=true npm run start:observs-partitions
DAILY_TASK_HEALTH_DISABLED=true npm run start:supabase-db-dump-backup
```

Check today's factual runs:

```sql
select *
from uk_aq_ops.daily_task_runs
where scheduled_for_date = current_date
order by created_at desc;
```

Check today's daily status:

```sql
select *
from uk_aq_ops.daily_task_status
where date_utc = current_date;
```

GitHub tasks (`ingest.stations_daily` and `ops.r2_history_dropbox_backup`) are still Phase 3 and are externally scheduled via Cloudflare Worker cron calling `workflow_dispatch`.

## Phase 3 GitHub Reporting

The GitHub daily workflows report final-only status with `uk_aq_rpc_daily_task_report_final`. There is no Started row for these jobs in v1 because GitHub scheduled workflows can start late, be retried, or fail before a start-reporting step is useful.

Instrumented GitHub tasks:

| Task key | Workflow |
| --- | --- |
| `ingest.stations_daily` | `uk-aq-ingest/.github/workflows/uk_aq_stations_daily.yml` |
| `ops.r2_history_dropbox_backup` | `uk-aq-ops/.github/workflows/uk_aq_r2_history_dropbox_backup.yml` |

Required GitHub repo/org configuration:

- `SUPABASE_URL` as a repository or organization variable
- `SUPABASE_SERVICE_ROLE_KEY` as a repository or organization secret

Optional controls:

- `DAILY_TASK_HEALTH_DISABLED=true` skips reporting.
- `DAILY_TASK_HEALTH_STRICT=true` makes reporting/recompute failures fail the final step.
- `DAILY_TASK_SCHEDULED_FOR_DATE=YYYY-MM-DD` overrides the UTC date for testing.

The reporting step runs with `if: always()` and maps GitHub job status to daily task status:

- `success` -> `Finished`
- `failure`, `cancelled`, or any other non-success status -> `Failed`

The script records GitHub metadata in `summary`, including repository, workflow, run id, run attempt, SHA, ref, event name, actor, and a GitHub Actions run URL. The table attempt is left for the RPC to allocate so repeated `workflow_dispatch` tests on the same UTC day do not collide on attempt `1`.

GitHub due times are still controlled by `uk_aq_ops.daily_task_definitions.due_time_utc`. A GitHub workflow is only Missing after the configured due time passes with no final report.

Manual validation:

1. Run `uk_aq_stations_daily.yml` manually via `workflow_dispatch` to test `ingest.stations_daily`.
2. Run `uk_aq_r2_history_dropbox_backup.yml` manually via `workflow_dispatch` to test `ops.r2_history_dropbox_backup`.
3. Query the resulting runs and day status:

```sql
select *
from uk_aq_ops.daily_task_runs
where task_key in ('ingest.stations_daily', 'ops.r2_history_dropbox_backup')
order by created_at desc
limit 20;

select *
from uk_aq_ops.daily_task_status
where date_utc = current_date;
```

Expected checks:

- Successful workflows write `Finished`.
- Failed or cancelled workflows write `Failed`.
- `log_url` points to the GitHub Actions run.
- `uk_aq_ops.daily_task_status` is recomputed.
- Reporting failures warn but do not break a successful workflow unless `DAILY_TASK_HEALTH_STRICT=true`.

## RPC Examples

GCP worker start and finish:

```sql
select uk_aq_public.uk_aq_rpc_daily_task_started(
  jsonb_build_object(
    'task_key', 'ops.prune_daily',
    'scheduled_for_date', '2026-05-06',
    'source_repo', 'uk-aq-ops',
    'source_worker', 'uk_aq_prune_daily'
  )
) as run_id;

select uk_aq_public.uk_aq_rpc_daily_task_finished(
  '<run-id>'::uuid,
  jsonb_build_object('summary', jsonb_build_object('phase', 'complete'))
);
```

GCP worker failure:

```sql
select uk_aq_public.uk_aq_rpc_daily_task_failed(
  '<run-id>'::uuid,
  jsonb_build_object(
    'error_message', 'worker exited non-zero',
    'error', jsonb_build_object('exit_code', 1)
  )
);
```

GitHub final reporting call:

```sql
select uk_aq_public.uk_aq_rpc_daily_task_report_final(
  jsonb_build_object(
    'task_key', 'ingest.stations_daily',
    'status', 'Finished',
    'scheduled_for_date', '2026-05-06',
    'source_repo', 'uk-aq-ingest',
    'platform_run_id', 'github-run-id',
    'log_url', 'https://github.com/.../actions/runs/...'
  )
);
```

Recompute a day:

```sql
select uk_aq_public.uk_aq_rpc_recompute_daily_task_status('2026-05-06'::date);
select * from uk_aq_ops.daily_task_status_calendar where date_utc = '2026-05-06';
```

Set and clear a manual override:

```sql
select uk_aq_public.uk_aq_rpc_set_daily_task_status_override(
  '2026-05-06'::date,
  'Finished',
  'Verified manually from scheduler logs',
  'ops'
);

select uk_aq_public.uk_aq_rpc_clear_daily_task_status_override('2026-05-06'::date);
```

## Future Phases

- Add a dashboard calendar overlay.
- Add a daily 10:00 UTC email/report.
