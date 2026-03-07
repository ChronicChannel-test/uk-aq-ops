# UK AQ AQI History R2 + Cache Test

This is a test harness to measure browser fetch performance for AQI history served from Cloudflare R2 through a Cloudflare Worker cache layer.

## What Was Added

Ops repo:

- Seed script:
  - `scripts/backup_r2/uk_aq_aqi_history_r2_test_seed.mjs`
- Worker:
  - `workers/uk_aq_aqi_history_r2_test_worker/worker.mjs`
  - `workers/uk_aq_aqi_history_r2_test_worker/wrangler.toml`
- Deploy workflow:
  - `.github/workflows/uk_aq_aqi_history_r2_test_worker_deploy.yml`

Website repo:

- Test page:
  - `uk_aq_history_r2_cache_test.html`

## R2 Test Prefix

- Prefix: `aqi-r2-test/v1`
- Object layout:
  - `aqi-r2-test/v1/{scope}/{grain}/{entity}.parquet`
  - `aqi-r2-test/v1/manifest.json`
  - `aqi-r2-test/v1/{scope}/{grain}/manifest.json`

Scopes:

- `station`
- `pcon`
- `la`
- `region`

Grains:

- `hourly` (2 days)
- `daily` (14 days)
- `monthly` (6 months)

## Worker API Routes

- `GET /v1/aqi-history/manifest`
- `GET /v1/aqi-history/data`

Aliases:

- `GET /manifest`
- `GET /data`

Query params:

- `scope` (`station|pcon|la|region`)
- `grain` (`hourly|daily|monthly`)
- `entity` (or `entity_id`)
- `v` (optional cache-buster)
- `prefix` (optional R2 prefix override, defaults to `AQI_R2_TEST_PREFIX`)
- `row_limit` (optional)

## Generate/Regenerate Test Data

From `CIC-test-uk-aq-ops`, export only required vars:

```bash
export $(grep -E '^(AGGDAILY_SUPABASE_URL|AGGDAILY_SECRET_KEY|CFLARE_R2_ENDPOINT|CFLARE_R2_REGION|CFLARE_R2_BUCKET|CFLARE_R2_ACCESS_KEY_ID|CFLARE_R2_SECRET_ACCESS_KEY)=' .env | sed 's/[[:space:]]*#.*$//' | xargs)

node scripts/backup_r2/uk_aq_aqi_history_r2_test_seed.mjs \
  --prefix aqi-r2-test/v1
```

Dry run:

```bash
export $(grep -E '^(AGGDAILY_SUPABASE_URL|AGGDAILY_SECRET_KEY|CFLARE_R2_ENDPOINT|CFLARE_R2_REGION|CFLARE_R2_BUCKET|CFLARE_R2_ACCESS_KEY_ID|CFLARE_R2_SECRET_ACCESS_KEY)=' .env | sed 's/[[:space:]]*#.*$//' | xargs)

node scripts/backup_r2/uk_aq_aqi_history_r2_test_seed.mjs \
  --prefix aqi-r2-test/v1 \
  --dry-run
```

## Deploy Worker

Manual deploy:

```bash
cd workers/uk_aq_aqi_history_r2_test_worker
wrangler deploy
```

Or push to `main` to trigger:

- `.github/workflows/uk_aq_aqi_history_r2_test_worker_deploy.yml`

## Use The Test Page

Open:

- `uk_aq_history_r2_cache_test.html`

Set `api_base` query param to your deployed worker URL. Example:

```text
uk_aq_history_r2_cache_test.html?api_base=https://uk-aq-aqi-history-r2-test.<workers-subdomain>.workers.dev/v1/aqi-history
```

Then:

1. Pick `scope`, `grain`, `entity`.
2. Click `Load cached` (stable `v` token).
3. Click again to compare warm cache.
4. Click `Load bypass-cache` (uses unique `v` token).
5. Click `Clear cache` to rotate the stable token.

Page shows:

- fetch duration (client-side ms)
- payload size
- endpoint URL
- cache header (`x-ukaq-cache` / `cf-cache-status`)
- row count and object bytes
- source parquet path
- DAQI + EAQI history chart
