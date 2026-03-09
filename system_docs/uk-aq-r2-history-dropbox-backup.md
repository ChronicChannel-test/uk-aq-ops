# UK AQ R2 History Dropbox Backup

This document describes the Phase 7 daily incremental backup from Cloudflare R2 History to Dropbox.

## Purpose

- Source of truth for completed days: committed day manifest in R2 History.
- Copy only new completed UTC days since previous successful copies.
- Preserve exact R2 key layout in Dropbox.

## Layout

Dropbox root (example):

- `/CIC-Test/R2_history_backup`

Mirrored domain paths:

- `history/v1/observations/day_utc=YYYY-MM-DD/...`
- `history/v1/aqilevels/day_utc=YYYY-MM-DD/...`

Checkpoint path (default):

- `_ops/checkpoints/r2_history_backup_state_v1.json`

Final checkpoint object location example:

- `/CIC-Test/R2_history_backup/_ops/checkpoints/r2_history_backup_state_v1.json`

## Script

Script:

- `scripts/backup_r2/sync_history_to_dropbox.mjs`

The script:

1. Lists day folders for `observations` and `aqilevels` from R2 prefixes.
2. Uses checkpoint state to identify days not yet copied.
3. Verifies source day completeness via day manifest existence (`manifest.json`).
4. Uses `rclone copy` for day-folder copy operations.
5. Verifies copied manifest hash at destination.
6. Updates checkpoint state after each successful day.

## Workflow

GitHub workflow:

- `.github/workflows/uk_aq_r2_history_dropbox_backup.yml`

Default schedule:

- `35 4 * * *` (UTC)

Supports manual dispatch with:

- `dry_run`
- `max_days_per_run`

## Required GitHub values

Secrets:

- `CFLARE_R2_ACCESS_KEY_ID`
- `CFLARE_R2_SECRET_ACCESS_KEY`
- `DROPBOX_APP_KEY`
- `DROPBOX_APP_SECRET`
- `DROPBOX_REFRESH_TOKEN`

Variables:

- `CFLARE_R2_ENDPOINT`
- `CFLARE_R2_BUCKET`
- `CFLARE_R2_REGION` (default `auto`)
- `UK_AQ_R2_HISTORY_OBSERVATIONS_PREFIX` (default `history/v1/observations`)
- `UK_AQ_R2_HISTORY_AQILEVELS_PREFIX` (default `history/v1/aqilevels`)
- `UK_AQ_DROPBOX_ROOT` (default `/CIC-Test`)
- `UK_AQ_R2_HISTORY_DROPBOX_DIR` (default `R2_history_backup`)
- `UK_AQ_R2_HISTORY_BACKUP_STATE_REL_PATH` (default `_ops/checkpoints/r2_history_backup_state_v1.json`)
- `UK_AQ_R2_HISTORY_BACKUP_MAX_DAYS_PER_RUN` (default `0` = unlimited)

Effective backup root:

- `{UK_AQ_DROPBOX_ROOT}/{UK_AQ_R2_HISTORY_DROPBOX_DIR}`

## Local run

```bash
node scripts/backup_r2/sync_history_to_dropbox.mjs \
  --source-root "uk_aq_r2:${CFLARE_R2_BUCKET}" \
  --dest-root "uk_aq_dropbox:/CIC-Test/R2_history_backup" \
  --report-out ./tmp/r2_history_dropbox_backup_report.json
```

Dry-run:

```bash
node scripts/backup_r2/sync_history_to_dropbox.mjs \
  --source-root "uk_aq_r2:${CFLARE_R2_BUCKET}" \
  --dest-root "uk_aq_dropbox:/CIC-Test/R2_history_backup" \
  --dry-run
```
