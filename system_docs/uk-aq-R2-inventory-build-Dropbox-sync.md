# UK-AQ R2 Inventory Build and Dropbox Sync

## Purpose

The R2 backup inventory workflow is designed to make Dropbox backup runs faster while preserving an important correctness rule:

> Any existing historical day in R2 may be updated later, so changed old days must still be detected and refreshed in Dropbox.

The fix splits the work into two separate responsibilities:

```text
build_backup_inventory.mjs
  = checks R2 and updates history/_index/backup_inventory_v1.json

sync_history_to_dropbox.mjs
  = compares that inventory against the Dropbox checkpoint and copies changed/missing backup units
```

## Why the first inventory build is slower

On the first run, there is no previous inventory to compare against.

So the builder has to:

```text
1. List all relevant manifest/index files in R2.
2. Read every manifest/index JSON file with rclone cat.
3. Compute a SHA-256 hash of the exact JSON bytes.
4. Build history/_index/backup_inventory_v1.json.
5. Upload the inventory back to R2.
```

This first run is expected to be relatively slow because it must read all existing manifests once.

For UK-AQ history, that can include:

```text
- observations day manifests
- aqilevels day manifests
- core day manifests
- latest index JSON files
- observations_timeseries index manifest units
- aqilevels_timeseries index manifest units
```

## Why the second inventory build is quicker

The second run is faster because the builder can reuse most of the previous inventory.

It does **not** simply check whether an inventory file exists and then stop. It still needs to check R2, because old days can change.

However, instead of re-reading every manifest body, it can use R2 object metadata from `rclone lsjson --hash`.

The steady-state builder process is:

```text
1. Read previous history/_index/backup_inventory_v1.json.
2. List current R2 manifest/index files using rclone lsjson --hash.
3. For each listed object, compare:
   - Size
   - Hashes.md5 / R2 ETag
4. If size + md5 are unchanged:
   - reuse the previous inventory entry
   - do not rclone cat that manifest
5. If the object is new or changed:
   - rclone cat that manifest
   - compute SHA-256 of the exact JSON bytes
   - update that inventory entry
6. Upload the refreshed inventory to R2.
```

So the second and later runs still perform a R2 listing/check, but they avoid thousands of individual manifest reads when most objects are unchanged.

## Why `rclone lsjson --hash` matters

Plain `rclone lsjson` or `rclone lsjson -M` may show size and modtime, but not the R2 ETag/MD5.

For Cloudflare R2, the stronger metadata signal is exposed when using:

```bash
rclone lsjson --hash --hash-type MD5 <r2-path>
```

Example output:

```json
{
  "Size": 4398,
  "ModTime": "2026-03-23T19:02:50.720000000Z",
  "Hashes": {
    "md5": "71350ccf4912edae37da099acd8b0672"
  }
}
```

That `Hashes.md5` value matches the R2 S3 `head-object` ETag for small JSON manifest files.

The builder should therefore prefer:

```text
Size + Hashes.md5
```

for unchanged detection, and only fall back to:

```text
Size + ModTime
```

if the hash is unavailable.

## What the Dropbox sync step does

The Dropbox sync step should no longer read every day manifest directly from R2.

Instead, it should:

```text
1. Read R2 history/_index/backup_inventory_v1.json.
2. Read Dropbox _ops/checkpoints/r2_history_backup_state_v1.json.
3. Compare inventory hashes against checkpoint hashes.
4. Queue only missing or changed backup units.
5. Copy those units from R2 to Dropbox.
6. Update the Dropbox checkpoint after successful copies.
```

So yes: the Dropbox sync step does the backup decision against the inventory.

But the inventory builder has already done the R2-side change detection needed to keep the inventory current.

## Simple mental model

```text
Builder:
  "What does R2 currently contain, and which units changed since the previous inventory?"

Sync:
  "Which of those current R2 units are missing or stale in Dropbox?"
```

## Why this preserves correctness

The old slow behaviour was effectively:

```text
for every old day:
    rclone cat history/v1/.../day_utc=.../manifest.json
    hash the file
    compare with Dropbox checkpoint
```

The new behaviour is:

```text
builder:
    use rclone lsjson --hash to cheaply detect unchanged R2 objects
    only rclone cat changed/new manifests
    maintain backup_inventory_v1.json

sync:
    compare backup_inventory_v1.json with the Dropbox checkpoint
    copy only changed/missing units
```

Changed old days are still detected because their manifest/index hash in the inventory changes, and the sync step sees that the inventory hash no longer matches the Dropbox checkpoint hash.

## Expected performance pattern

### First run

```text
Inventory builder: slower
Dropbox sync: inventory-driven
```

The builder must read all existing manifest/index JSON files once.

### Second and later runs

```text
Inventory builder: much quicker
Dropbox sync: much quicker
```

Most unchanged manifest/index files are skipped by comparing size + MD5/ETag from the R2 listing against the previous inventory.

Only new or changed files are re-read and re-hashed.

## Recommended reporting fields

The builder report should include enough telemetry to prove the optimisation is working:

```json
{
  "metadata_strategy": "size_md5",
  "md5_available_count": 0,
  "md5_missing_count": 0,
  "reuse_by_md5_size": 0,
  "reuse_by_size_modtime": 0,
  "manifests_listed": 0,
  "manifests_reread": 0,
  "etag_skip_hits": 0,
  "etag_skip_rate": 0,
  "first_build": false,
  "metadata_warnings": []
}
```

The sync report should show that the inventory path was used:

```json
{
  "inventory_used": true,
  "inventory_generated_at": "2026-05-15T12:00:00.000Z",
  "inventory_hash": "sha256hex...",
  "copied_days": {},
  "skipped_unchanged": {},
  "changed_existing_days": {},
  "new_days": {},
  "index_files_copied": 0,
  "index_tree_units_copied": 0
}
```

## Key point

The second inventory build is quicker because it does not re-read every old manifest.

It still checks R2, but it does so through a cheap metadata listing:

```text
same size + same MD5/ETag = previous SHA-256 inventory entry is still valid
```

Only changed or new objects need to be opened, hashed, and written into the refreshed inventory.
