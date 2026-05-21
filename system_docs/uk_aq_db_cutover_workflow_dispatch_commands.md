# UK-AQ DB Cutover Workflow (Restore + GH/GCP Redeploy)

Purpose:
- Restore `ingestdb` and `obs_aqidb` from backup files in the correct order.
- Re-dispatch GitHub deploy workflows so updated DB connection config is propagated to GitHub/Supabase/GCP runtimes.

Assumptions:
- You are in a shell where your `.env` values are already exported.
- `psql`, `gh`, and `gunzip` are installed.
- Backup files are present locally (Dropbox synced).

## 0) Preflight and env normalization

Run from `CIC-test-uk-aq-ingest`:

```bash
set -euo pipefail

export UK_AQ_INGEST_REPO_DIR="/Users/mikehinford/Dropbox/Projects/CIC Website/CIC Air Quality Networks/CIC-test-uk-aq-ingest"
export UK_AQ_OPS_REPO_DIR="/Users/mikehinford/Dropbox/Projects/CIC Website/CIC Air Quality Networks/CIC-test-uk-aq Operations/CIC-test-uk-aq-ops"

# URL aliases so commands work with either env naming scheme
export UK_AQ_INGESTDB_DB_URL="${UK_AQ_INGESTDB_DB_URL:-${SUPABASE_DB_URL:-}}"
export UK_AQ_OBS_AQIDB_DB_URL="${UK_AQ_OBS_AQIDB_DB_URL:-${OBS_AQIDB_SUPABASE_DB_URL:-}}"

test -n "${UK_AQ_INGESTDB_DB_URL}" || { echo "Missing ingest DB URL env var"; exit 1; }
test -n "${UK_AQ_OBS_AQIDB_DB_URL}" || { echo "Missing obs_aqidb DB URL env var"; exit 1; }

# Choose backup root + date
export UK_AQ_DB_DUMP_ROOT="${UK_AQ_DB_DUMP_ROOT:-/Users/mikehinford/Dropbox/Apps/github-uk-air-quality-networks/CIC-Test/Supabase_Backup_db_dump}"
export UK_AQ_DB_DUMP_DATE="${UK_AQ_DB_DUMP_DATE:-2026-05-21}"
```

## 1) Verify backup files exist

```bash
for db in ingestdb obs_aqidb; do
  base="$UK_AQ_DB_DUMP_ROOT/$db/$UK_AQ_DB_DUMP_DATE"
  echo "Checking $base"
  ls "$base/roles.sql.gz" "$base/schema.sql.gz" "$base/data.sql.gz" "$base/cron_jobs.sql.gz"
done
```

## 2) Restore `ingestdb` (exact order)

```bash
export UK_AQ_DB="ingestdb"
export UK_AQ_DB_URL="$UK_AQ_INGESTDB_DB_URL"
export UK_AQ_DB_DIR="$UK_AQ_DB_DUMP_ROOT/$UK_AQ_DB/$UK_AQ_DB_DUMP_DATE"

gunzip -c "$UK_AQ_DB_DIR/roles.sql.gz" | psql "$UK_AQ_DB_URL" -v ON_ERROR_STOP=1
gunzip -c "$UK_AQ_DB_DIR/schema.sql.gz" | psql "$UK_AQ_DB_URL" -v ON_ERROR_STOP=1
gunzip -c "$UK_AQ_DB_DIR/data.sql.gz" | psql "$UK_AQ_DB_URL" -v ON_ERROR_STOP=1
gunzip -c "$UK_AQ_DB_DIR/cron_jobs.sql.gz" | psql "$UK_AQ_DB_URL" -v ON_ERROR_STOP=1
```

## 3) Restore `obs_aqidb` (exact order)

```bash
export UK_AQ_DB="obs_aqidb"
export UK_AQ_DB_URL="$UK_AQ_OBS_AQIDB_DB_URL"
export UK_AQ_DB_DIR="$UK_AQ_DB_DUMP_ROOT/$UK_AQ_DB/$UK_AQ_DB_DUMP_DATE"

gunzip -c "$UK_AQ_DB_DIR/roles.sql.gz" | psql "$UK_AQ_DB_URL" -v ON_ERROR_STOP=1
gunzip -c "$UK_AQ_DB_DIR/schema.sql.gz" | psql "$UK_AQ_DB_URL" -v ON_ERROR_STOP=1
gunzip -c "$UK_AQ_DB_DIR/data.sql.gz" | psql "$UK_AQ_DB_URL" -v ON_ERROR_STOP=1
gunzip -c "$UK_AQ_DB_DIR/cron_jobs.sql.gz" | psql "$UK_AQ_DB_URL" -v ON_ERROR_STOP=1
```

## 4) Quick DB checks after restore

```bash
psql "$UK_AQ_INGESTDB_DB_URL" -v ON_ERROR_STOP=1 -c "select count(*) as cron_jobs from cron.job;"
psql "$UK_AQ_OBS_AQIDB_DB_URL" -v ON_ERROR_STOP=1 -c "select count(*) as cron_jobs from cron.job;"
```

## 5) Sync GitHub vars/secrets from local env files

From ingest repo:

```bash
cd "$UK_AQ_INGEST_REPO_DIR"
./scripts/uk_aq_sync_github_secrets.sh --env-file .env --targets-file config/uk_aq_github_env_targets.csv
```

From ops repo:

```bash
cd "$UK_AQ_OPS_REPO_DIR"
./scripts/uk_aq_sync_github_secrets.sh --env-file .env --targets-file config/uk_aq_github_env_targets.csv
```

## 6) Dispatch ingest workflows (DB-dependent runtimes)

Run from ingest repo:

```bash
cd "$UK_AQ_INGEST_REPO_DIR"
for wf in \
  uk_aq_validate_github_env_targets.yml \
  supabase_edge_deploy.yml \
  uk_aq_observs_edge_deploy.yml \
  uk_aq_dispatcher_deploy.yml \
  uk_aq_observs_pubsub_cloud_run_deploy.yml \
  uk_aq_breathelondon_cloud_run_deploy.yml \
  uk_aq_uk_air_sos_cloud_run_deploy.yml \
  uk_aq_openaq_cloud_run_deploy.yml \
  uk_aq_scomm_cloud_run_deploy.yml
do
  echo "Running $wf"
  gh workflow run "$wf" --ref main || echo "FAILED: $wf"
done
```

## 7) Dispatch ops workflows (DB-dependent runtimes)

Run from ops repo:

```bash
cd "$UK_AQ_OPS_REPO_DIR"
for wf in \
  uk_aq_cache_proxy_deploy.yml \
  uk_aq_db_r2_metrics_api_worker_deploy.yml \
  uk_aq_aqi_history_r2_api_worker_deploy.yml \
  uk_aq_latest_snapshot_cloud_run_deploy.yml \
  uk_aq_db_size_logger_cloud_run_deploy.yml \
  uk_aq_prune_daily_cloud_run_deploy.yml \
  uk_aq_timeseries_aqi_hourly_cloud_run_deploy.yml \
  uk_aq_aqilevels_retention_cloud_run_deploy.yml \
  uk_aq_observs_partition_maintenance_cloud_run_deploy.yml \
  uk_aq_supabase_db_dump_backup_service_deploy.yml
do
  echo "Running $wf"
  gh workflow run "$wf" --ref main || echo "FAILED: $wf"
done
```

## 8) Monitor workflow run status

```bash
gh run list --limit 30
```

Notes:
- These workflow deploys include GCP Secret Manager update paths where configured, so this sequence covers GH + GCP propagation.
- `cron_jobs.sql.gz` must be restored after `data.sql.gz` for each DB.
- If a workflow is intentionally disabled in your environment, skip it.
