# uk-aq-aqilevels-retention setup (Cloud Run + Scheduler)

This deploys a dedicated Cloud Run service that enforces rolling retention for `uk_aq_aqilevels` in `obs_aqidb`.

## Runtime behavior

`POST /run` performs:
- compute DST-aware cutoff from Europe/London local days
- fetch day-level cleanup candidates older than cutoff
- check committed R2 History manifest before each drop:
  - HEAD `history/v1/aqilevels/day_utc=YYYY-MM-DD/manifest.json`
- delete only confirmed complete days from:
  - `uk_aq_aqilevels.station_aqi_hourly`
  - `uk_aq_aqilevels.station_aqi_daily`

If manifest is missing/unconfirmed for a day, that day is skipped.

## Required environment variables

- `OBS_AQIDB_SUPABASE_URL`
- `OBS_AQIDB_SECRET_KEY`

## Optional environment variables

Retention controls:
- `OBS_AQIDB_AQILEVELS_RETENTION_DAYS` (default `14`)
- `AQILEVELS_RETENTION_DROP_DRY_RUN` (default `false`)

Cloudflare R2 history-check configuration:
- `CFLARE_R2_ENDPOINT` (or `R2_ENDPOINT`)
- `CFLARE_R2_BUCKET` (or `R2_BUCKET`)
- `CFLARE_R2_ACCESS_KEY_ID` (or `R2_ACCESS_KEY_ID`)
- `CFLARE_R2_SECRET_ACCESS_KEY` (or `R2_SECRET_ACCESS_KEY`)
- `CFLARE_R2_REGION` (or `R2_REGION`, default `auto`)
- `UK_AQ_R2_HISTORY_AQILEVELS_PREFIX` (default `history/v1/aqilevels`)

## Local run

```bash
npm install
npm run start:aqilevels-retention
```

Run once:

```bash
curl -X POST "http://localhost:8080/run"
```

Dry-run retention:

```bash
curl -X POST "http://localhost:8080/run?dropDryRun=true"
```

## Scheduler

Recommended schedule: `15 3 * * *` with timezone `UTC`.
