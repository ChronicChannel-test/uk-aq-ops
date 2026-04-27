# UK AQ Scripts Notes

## Postcode lookup scripts

- `scripts/postcodes/build_postcode_lookup_from_onspd.mjs`
  - Reads ONSPD CSV and writes postcode shard JSON files plus `manifest.json`.
  - Shards are grouped by leading postcode area and keyed by normalized postcode.

- `scripts/postcodes/upload_postcode_lookup_to_r2.mjs`
  - Uploads shard files and `manifest.json` to R2 using S3-compatible API.
  - Supports postcode-specific env vars and existing `CFLARE_R2_*` conventions.
