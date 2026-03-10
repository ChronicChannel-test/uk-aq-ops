# UK AQ R2 History Dropbox Backup

This document describes the Phase 7 daily incremental backup from Cloudflare R2 History to Dropbox.

## Purpose

- Source of truth for completed days: committed day manifest in R2 History.
- Copy only new completed UTC days since previous successful copies.
- Preserve exact R2 key layout in Dropbox.

## Layout

Dropbox root (example):

- `CIC-Test/R2_history_backup`

Mirrored domain paths:

- `history/v1/observations/day_utc=YYYY-MM-DD/...`
- `history/v1/aqilevels/day_utc=YYYY-MM-DD/...`

Checkpoint path (default):

- `_ops/checkpoints/r2_history_backup_state_v1.json`

Final checkpoint object location example:

- `CIC-Test/R2_history_backup/_ops/checkpoints/r2_history_backup_state_v1.json`

## Script

Script:

- `scripts/backup_r2/sync_history_to_dropbox.mjs`

The script:

1. Lists day folders for `observations` and `aqilevels` from R2 prefixes.
2. Uses checkpoint state plus source day-manifest hash to identify days that are new or changed since last copy.
3. Verifies source day completeness via day manifest existence (`manifest.json`).
4. Uses `rclone copy` for day-folder copy operations.
5. Verifies copied manifest hash at destination.
6. Updates checkpoint state after each successful day.

## Workflow

GitHub workflow:

- `.github/workflows/uk_aq_r2_history_dropbox_backup.yml`

Restore workflow (manual):

- `.github/workflows/uk_aq_r2_history_restore_from_dropbox.yml`

Default schedule:

- `35 4 * * *` (UTC)

Supports manual dispatch with:

- `dry_run`
- `max_days_per_run`

### How dispatch inputs behave

- `Run without writing copy/checkpoint changes` (`dry_run=true`):
  - Performs listing + manifest checks + copy planning only.
  - Does not write copied files.
  - Does not update checkpoint state.
- `Override max new days copied per domain (0 = unlimited)` (`max_days_per_run`):
  - This is a per-domain copy cap, not a "days from now" lookback.
  - `1` means: copy at most one uncopied complete day for `observations`, and at most one uncopied complete day for `aqilevels`, in that run.
  - `0` means no cap (all uncopied complete days can be copied).

Selection rule used by the script for each domain:

1. List available `day_utc=YYYY-MM-DD` folders under the R2 History domain prefix.
2. Sort days ascending (oldest to newest).
3. For days already checkpointed with a stored `manifest_hash`, compare the current source `manifest.json` hash:
   - If hash matches, skip as existing.
   - If hash changed, re-queue the day for copy.
4. For days missing in checkpoint (or legacy checkpoint rows without `manifest_hash`), queue copy.
5. Apply `max_days_per_run` cap.
6. Copy only days with a source `manifest.json` (incomplete days are skipped).

Practical effect:

- Running with `max_days_per_run=1` repeatedly will only copy a day when a new uncopied complete day exists.
- If yesterday was already copied (or not yet complete), the next run can show `copied_days=0`.

### Interpreting a "no new copy" run

If a run succeeds but copies nothing, read these fields in the JSON report:

- `listed_days`: complete day folders currently visible in source prefix.
- `candidate_days`: days queued for copy after checkpoint + manifest-hash comparison, after cap.
- `copied_days`: days actually copied this run.
- `skipped_existing`: days skipped because checkpoint hash still matches source (plus any days deferred by `max_days_per_run`).

Example:

- `listed_days=1`, `candidate_days=0`, `copied_days=0`, `skipped_existing=1`
  means the only available complete day is already backed up, so this is expected.

### Recommended manual-dispatch values

Use this rollout sequence:

1. First validation run:
   - `dry_run=true`
   - `max_days_per_run=3`
2. First write run:
   - `dry_run=false`
   - `max_days_per_run=1`
3. Controlled catch-up runs:
   - `dry_run=false`
   - `max_days_per_run=7` (or `14` if runtime is comfortably below timeout)
4. Steady-state daily runs:
   - `dry_run=false`
   - `max_days_per_run=0` (unlimited) once backlog is cleared.

Notes:
- `max_days_per_run` is per domain (`observations`, `aqilevels`).
- `0` means unlimited and can be slower on first catch-up if many days are pending.

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
- `UK_AQ_DROPBOX_ROOT` (default `CIC-Test`)
- `UK_AQ_R2_HISTORY_DROPBOX_DIR` (default `R2_history_backup`)
- `UK_AQ_R2_HISTORY_BACKUP_STATE_REL_PATH` (default `_ops/checkpoints/r2_history_backup_state_v1.json`)
- `UK_AQ_R2_HISTORY_BACKUP_MAX_DAYS_PER_RUN` (default `0` = unlimited)

Effective backup root:

- `{UK_AQ_DROPBOX_ROOT}/{UK_AQ_R2_HISTORY_DROPBOX_DIR}`

Dropbox app-folder note:

- For sandbox/app-folder tokens, keep `UK_AQ_DROPBOX_ROOT` without a leading slash.
- Example: `CIC-Test` (not `/CIC-Test`).

## Local run

```bash
node scripts/backup_r2/sync_history_to_dropbox.mjs \
  --source-root "uk_aq_r2:${CFLARE_R2_BUCKET}" \
  --dest-root "uk_aq_dropbox:CIC-Test/R2_history_backup" \
  --report-out ./tmp/r2_history_dropbox_backup_report.json
```

Dry-run:

```bash
node scripts/backup_r2/sync_history_to_dropbox.mjs \
  --source-root "uk_aq_r2:${CFLARE_R2_BUCKET}" \
  --dest-root "uk_aq_dropbox:CIC-Test/R2_history_backup" \
  --dry-run
```

## Restore (Dropbox -> R2 History)

Script:

- `scripts/backup_r2/restore_history_from_dropbox.mjs`

Workflow dispatch inputs:

- `dry_run`:
  - `true` = validate/list/copy-plan only.
  - `false` = write to R2.
- `day_utc`:
  - Optional `YYYY-MM-DD`.
  - If set, restore only that day folder under selected domains.
  - If blank, restore selected full domain prefixes.
- `restore_observations`:
  - Include `history/v1/observations`.
- `restore_aqilevels`:
  - Include `history/v1/aqilevels`.

Recommended first restore run:

1. `dry_run=true`
2. Set the required domain flags.
3. Set `day_utc` if you want a targeted restore first.
