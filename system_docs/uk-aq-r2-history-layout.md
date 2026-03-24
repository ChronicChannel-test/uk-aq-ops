# UK AQ R2 History Layout

This document is the canonical object-layout reference for the UK AQ R2 history bucket.

The SQL schema repo only tracks R2 telemetry such as domain sizes. The actual R2 object tree, manifest shapes, and derived index payloads are defined by the ops writers and readers in this repo.

## Bucket and Top-Level Prefixes

Bucket selection is deployment-specific:

- explicit bucket: `R2_BUCKET` or `CFLARE_R2_BUCKET`
- otherwise deploy mapping:
  - `R2_BUCKET_PROD`
  - `R2_BUCKET_STAGE`
  - `R2_BUCKET_DEV`

Stable top-level prefixes:

- `history/v1/observations`
- `history/v1/aqilevels`
- `history/v1/core`
- `history/_index`

Operational prefixes:

- `history/v1/_ops/observations/runs`
- `history/v1/_ops/observations/staging`

## Object Tree

```text
history/
  _index/
    observations_latest.json
    aqilevels_latest.json
    observations_timeseries_latest.json
    observations_timeseries/
      day_utc=YYYY-MM-DD/
        connector_id=<id>/
          manifest.json
  v1/
    observations/
      day_utc=YYYY-MM-DD/
        manifest.json
        connector_id=<id>/
          manifest.json
          part-00000.parquet
          part-00001.parquet
          ...
    aqilevels/
      day_utc=YYYY-MM-DD/
        manifest.json
        connector_id=<id>/
          manifest.json
          part-00000.parquet
          part-00001.parquet
          ...
    core/
      day_utc=YYYY-MM-DD/
        manifest.json
        checksums.sha256
        table=<table>/
          rows.ndjson.gz
    _ops/
      observations/
        runs/
          run_id=<uuid>/
            run_manifest.json
        staging/
          run_id=<uuid>/
            ...
```

## Observations Domain

Committed observations objects live under:

- `history/v1/observations/day_utc=YYYY-MM-DD/connector_id=<id>/part-xxxxx.parquet`
- `history/v1/observations/day_utc=YYYY-MM-DD/connector_id=<id>/manifest.json`
- `history/v1/observations/day_utc=YYYY-MM-DD/manifest.json`

Current observations parquet schema:

- `history_schema_name`: `observations`
- `history_schema_version`: `2`
- `writer_version`: `parquet-wasm-zstd-v2`
- columns:
  - `connector_id`
  - `timeseries_id`
  - `observed_at`
  - `value`

Connector manifest fields:

- identity:
  - `day_utc`
  - `connector_id`
  - `run_id`
- coverage:
  - `source_row_count`
  - `min_observed_at`
  - `max_observed_at`
- object listing:
  - `parquet_object_keys`
  - `file_count`
  - `total_bytes`
  - `files`
    - each observations `files[]` entry may also include:
      - `min_timeseries_id`
      - `max_timeseries_id`
      - `min_observed_at`
      - `max_observed_at`
- schema metadata:
  - `history_schema_name`
  - `history_schema_version`
  - `columns`
  - `writer_version`
  - `writer_git_sha`
- file-size stats:
  - `bytes_per_row_estimate`
  - `avg_file_bytes`
  - `min_file_bytes`
  - `max_file_bytes`
- manifest metadata:
  - `backed_up_at_utc`
  - `manifest_hash`

Day manifest fields:

- day-level identity and coverage:
  - `day_utc`
  - `connector_ids`
  - `run_id`
  - `source_row_count`
  - `min_observed_at`
  - `max_observed_at`
- day-level object listing:
  - `parquet_object_keys`
  - `file_count`
  - `total_bytes`
  - `files`
  - `connector_manifests`
- schema metadata:
  - `history_schema_name`
  - `history_schema_version`
  - `columns`
  - `writer_version`
  - `writer_git_sha`
- file-size stats:
  - `bytes_per_row_estimate`
  - `avg_file_bytes`
  - `min_file_bytes`
  - `max_file_bytes`
- manifest metadata:
  - `backed_up_at_utc`
  - `manifest_hash`

Notes:

- Parts are written directly to the committed prefix.
- Connector manifests are written after all parts for that connector/day exist.
- Day manifests are written only after all connector manifests for that day are available.

## AQI Levels Domain

Committed AQI objects live under:

- `history/v1/aqilevels/day_utc=YYYY-MM-DD/connector_id=<id>/part-xxxxx.parquet`
- `history/v1/aqilevels/day_utc=YYYY-MM-DD/connector_id=<id>/manifest.json`
- `history/v1/aqilevels/day_utc=YYYY-MM-DD/manifest.json`

Current AQI parquet schema:

- `history_schema_name`: `aqilevels`
- `history_schema_version`: `2`
- `writer_version`: `parquet-wasm-zstd-v2`
- columns:
  - `connector_id`
  - `timeseries_id`
  - `station_id`
  - `pollutant_code`
  - `timestamp_hour_utc`
  - `hourly_mean_ugm3`
  - `rolling24h_mean_ugm3`
  - `hourly_sample_count`
  - `daqi_index_level`
  - `eaqi_index_level`

AQI connector/day manifests follow the same overall pattern as observations, but use:

- `min_timestamp_hour_utc`
- `max_timestamp_hour_utc`

instead of:

- `min_observed_at`
- `max_observed_at`

## Derived Index Objects

Derived index objects live under:

- `history/_index/observations_latest.json`
- `history/_index/aqilevels_latest.json`
- `history/_index/observations_timeseries_latest.json`
- `history/_index/observations_timeseries/day_utc=YYYY-MM-DD/connector_id=<id>/manifest.json`
- `history/_index/aqilevels_timeseries_latest.json`
- `history/_index/aqilevels_timeseries/day_utc=YYYY-MM-DD/connector_id=<id>/manifest.json`

These are rebuilt from committed day manifests and connector manifests only.

Index payload fields:

- `schema_version`
- `generated_at`
- `source`
- `domain`
- `bucket`
- `prefix`
- `min_day_utc`
- `max_day_utc`
- `day_count`
- `total_rows`
- `days`
- `day_summaries`

Each `day_summaries` entry includes:

- `day_utc`
- `total_rows`
- `connector_count`
- `file_count`
- `total_bytes`
- `connectors`
- `run_id`
- `backed_up_at_utc`
- `manifest_hash`

Observations summaries also include:

- `min_observed_at`
- `max_observed_at`

AQI summaries also include:

- `min_timestamp_hour_utc`
- `max_timestamp_hour_utc`

Each `connectors` entry includes:

- `connector_id`
- `row_count`
- `file_count`
- `total_bytes`
- `manifest_key`

### Observations timeseries index (lightweight file-range index)

This index family accelerates historical reads for one `timeseries_id` by narrowing
which parquet files are scanned for each `day_utc + connector_id`, using file
`min_timeseries_id/max_timeseries_id` and (when present) `min_observed_at/max_observed_at`.

Latest descriptor key:

- `history/_index/observations_timeseries_latest.json`

Per-day per-connector index key:

- `history/_index/observations_timeseries/day_utc=YYYY-MM-DD/connector_id=<id>/manifest.json`

`observations_timeseries_latest.json` fields:

- `schema_version`
- `generated_at`
- `source` (`r2_connector_manifests`)
- `domain` (`observations`)
- `index_kind` (`timeseries_file_ranges`)
- `bucket`
- `observations_prefix`
- `index_prefix`
- `min_day_utc`
- `max_day_utc`
- `day_count`
- `connector_index_count`
- `file_count`
- `indexed_file_count`
- `days`
- `key_layout.connector_index_manifest_key_template`
- `key_layout.latest_key`
- `day_summaries[]` with:
  - `day_utc`
  - `connector_count`
  - `connector_ids`

Per-connector index manifest fields:

- `schema_version`
- `generated_at`
- `source` (`r2_connector_manifest`)
- `domain` (`observations`)
- `index_kind` (`timeseries_file_ranges`)
- `bucket`
- `observations_prefix`
- `day_utc`
- `connector_id`
- `connector_manifest_key`
- `connector_manifest_hash`
- `source_row_count`
- `file_count`
- `indexed_file_count`
- `index_coverage` (`complete|partial`)
- `min_timeseries_id`
- `max_timeseries_id`
- `files[]`, each with:
  - `key`
  - `row_count`
  - `bytes`
  - `etag_or_hash`
  - `min_timeseries_id`
  - `max_timeseries_id`
  - `min_observed_at`
  - `max_observed_at`
- `backed_up_at_utc`

### AQI timeseries index (lightweight file-range index)

This index family accelerates AQI history reads by narrowing parquet scans for
`day_utc + connector_id`, then pruning by timeseries-id range and optional
pollutant availability.

Latest descriptor key:

- `history/_index/aqilevels_timeseries_latest.json`

Per-day per-connector index key:

- `history/_index/aqilevels_timeseries/day_utc=YYYY-MM-DD/connector_id=<id>/manifest.json`

Per-connector AQI index manifest fields:

- `schema_version`
- `generated_at`
- `source` (`r2_connector_manifest`)
- `domain` (`aqilevels`)
- `index_kind` (`timeseries_file_ranges`)
- `bucket`
- `aqilevels_prefix`
- `day_utc`
- `connector_id`
- `connector_manifest_key`
- `connector_manifest_hash`
- `source_row_count`
- `file_count`
- `indexed_file_count`
- `index_coverage` (`complete|partial`)
- `available_pollutants`
- `min_timeseries_id`
- `max_timeseries_id`
- `files[]`, each with:
  - `key`
  - `row_count`
  - `bytes`
  - `etag_or_hash`
  - `pollutant_codes`
  - `min_timeseries_id`
  - `max_timeseries_id`
  - `min_timestamp_hour_utc`
  - `max_timestamp_hour_utc`
- `backed_up_at_utc`

## Core Snapshot Objects

Core snapshot objects live under:

- `history/v1/core/day_utc=YYYY-MM-DD/manifest.json`
- `history/v1/core/day_utc=YYYY-MM-DD/checksums.sha256`
- `history/v1/core/day_utc=YYYY-MM-DD/table=<table>/rows.ndjson.gz`

Core manifest fields:

- `schema_name`: `uk_aq_core_snapshot`
- `schema_version`: `1`
- `generated_at_utc`
- `day_utc`
- `source_schema`
- `prefix`
- `file_format`
- `tables`
- `totals`
- `checksums`
- `manifest_hash`

Each `tables` entry includes:

- `table`
- `order_by`
- `key`
- `relative_path`
- `row_count`
- `uncompressed_bytes`
- `compressed_bytes`
- `sha256`
- `sha256_uncompressed`

## Operational Objects

Run manifests live under:

- `history/v1/_ops/observations/runs/run_id=<uuid>/run_manifest.json`

Run manifest fields:

- `run_id`
- `backed_up_at_utc`
- `summary`
- `manifest_hash`

The `summary` payload is the Phase B run summary returned by the prune worker, including:

- eligible window metadata
- candidate counts
- completed and failed previews
- byte and row totals
- AQI export summary
- staging cleanup summary

Staging objects live under:

- `history/v1/_ops/observations/staging/run_id=<uuid>/...`

Notes:

- staging is legacy/operational only
- current observation export writes committed parts directly under `history/v1/observations/...`
- staging cleanup is still retained to drain older run artifacts

## What Readers Treat As Committed

Committed public history is defined by day manifests under:

- `history/v1/observations/day_utc=YYYY-MM-DD/manifest.json`
- `history/v1/aqilevels/day_utc=YYYY-MM-DD/manifest.json`

Readers and maintenance jobs should treat these as the source of truth for completed days.

Examples:

- prune day gating updates `uk_aq_ops.prune_day_gates.history_done` from committed day manifests
- R2 API workers read day manifests first, then connector manifests and parquet files
- observs R2 API cache keys are canonicalized to `/v1/observations` with normalized query params (`timeseries_id`, `connector_id`, `start_utc`, `end_utc`, optional `since_utc`, optional `limit`)
- observs R2 API responses use dynamic cache TTL (`recent` vs `immutable`) based on `end_utc` and expose that via `cache_scope`
- Dropbox backup mirrors committed day folders and derived index files exactly

## Non-Goals

This layout doc does not replace database schema docs.

The schema repo still owns:

- DB tables
- views
- RPCs
- R2 telemetry tables such as domain-size metrics

This page only describes the R2 object tree and manifest/index payload shapes.
