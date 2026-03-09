# UK AQ Cache Proxy Worker

Repo owner: `uk-aq-ops`
Worker path: `workers/uk_aq_cache_proxy/src/index.ts`
Deploy workflow: `.github/workflows/uk_aq_cache_proxy_deploy.yml`

## Purpose

Cloudflare edge cache + session/auth proxy for website AQ read routes.

- Provides browser session start/end endpoints.
- Applies origin checks, Turnstile-gated session minting, and cache policy.
- Proxies AQ read routes to Supabase edge functions.
- Injects upstream shared-secret header for edge-function allowlist.

## Routes

Session endpoints:

- `POST /api/aq/session/start`
- `POST /api/aq/session/end`

Read endpoints:

- `/api/aq/latest` -> `uk_aq_latest`
- `/api/aq/timeseries` -> `uk_aq_timeseries`
- `/api/aq/stations-chart` -> `uk_aq_stations_chart`
- `/api/aq/stations` -> `uk_aq_stations`
- `/api/aq/la-hex` -> `uk_aq_la_hex`
- `/api/aq/pcon-hex` -> `uk_aq_pcon_hex`

## Required GitHub env/secret targets

Variables:

- `SUPABASE_URL`
- `UK_AQ_CACHE_ALLOWED_ORIGINS`
- `UK_AQ_EDGE_SESSION_MAX_AGE_SECONDS` (optional)

Secrets:

- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`
- `SB_PUBLISHABLE_DEFAULT_KEY`
- `UK_AQ_EDGE_ACCESS_TOKEN_SECRET`
- `UK_AQ_EDGE_UPSTREAM_SECRET`
- `UK_AQ_CACHE_BYPASS_SECRET`
- `UK_AQ_TURNSTILE_SECRET_KEY`

## Deployment

Workflow `uk_aq_cache_proxy_deploy.yml`:

1. Deploys worker code.
2. Applies worker secrets/vars with `wrangler secret bulk`.
3. Deploys worker code again so latest config is active.

## Notes

- Route binding is managed in Cloudflare (dashboard or Wrangler route config).
- Keep test/prod hostnames and origin allowlists separated by environment.
- Upstream edge functions must validate `X-UK-AQ-Upstream-Auth` with the same `UK_AQ_EDGE_UPSTREAM_SECRET`.
