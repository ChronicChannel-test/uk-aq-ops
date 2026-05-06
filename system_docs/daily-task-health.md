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

GitHub Actions scheduled workflows can start much later than their cron time, so `due_time_utc` is the reliable missing-run threshold. The seed data gives GitHub jobs a larger grace period, including extra runtime allowance for the R2 Dropbox backup, while GCP jobs use about 30 minutes. Check the seeded times against deployed GitHub/GCP schedules before relying on them operationally.

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

GitHub tasks (`ingest.stations_daily` and `ops.r2_history_dropbox_backup`) are still Phase 3.

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

- Instrument GitHub Actions.
- Add a dashboard calendar overlay.
- Add a daily 10:00 UTC email/report.
