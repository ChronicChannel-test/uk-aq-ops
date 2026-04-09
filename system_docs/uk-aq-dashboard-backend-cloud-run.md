# UK AQ Dashboard Backend Cloud Run

Repo owner: `uk-aq-ops`  
Service source: `local/dashboard/server/uk_aq_dashboard_api.py`  
Deploy workflow: `.github/workflows/uk_aq_dashboard_backend_cloud_run_deploy.yml`

## Purpose

Hosts the migrated Python dashboard backend API so the Cloudflare dashboard Worker can proxy `/api/*` routes without exposing Supabase service-role keys in the browser.

## Routes served

- `GET /api/dashboard`
- `GET /api/config`
- `GET /api/snapshot`
- `GET /api/storage_coverage`
- `GET /api/r2_metrics`
- `GET /api/r2_connector_counts`
- `POST /api/connectors`
- `POST /api/dispatcher_settings`

## Runtime requirements

Required env/secrets:

- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `OBS_AQIDB_SUPABASE_URL`
- `OBS_AQIDB_SECRET_KEY`

Common optional settings:

- `UK_AQ_DB_SIZE_API_URL`
- `UK_AQ_DB_SIZE_API_TOKEN`
- `UK_AQ_R2_HISTORY_DAYS_API_URL`
- `UK_AQ_R2_HISTORY_COUNTS_API_URL`
- `DASHBOARD_UPSTREAM_BEARER_TOKEN`
- `CLEANAIRSURB_ST_ID` (default station id for `/api/config` and snapshot page load)
- `DROPBOX_APP_KEY`
- `DROPBOX_APP_SECRET`
- `DROPBOX_REFRESH_TOKEN`

Dropbox status note:

- If local checkpoint file discovery fails (for example in Cloud Run), the backend now fetches
  the checkpoint JSON directly from Dropbox using `DROPBOX_APP_KEY/SECRET/REFRESH_TOKEN`.

## Auth model

- Cloud Run is deployed with `allow-unauthenticated` to permit Cloudflare Worker access.
- If `DASHBOARD_UPSTREAM_BEARER_TOKEN` is set, all `/api/*` requests require:
  - `Authorization: Bearer <token>`
- The same token should be configured in dashboard Worker secret `DASHBOARD_UPSTREAM_BEARER_TOKEN`.

## Deployment flow

1. Workflow builds and pushes `local/dashboard/server/Dockerfile`.
2. Workflow deploys service to Cloud Run.
3. Workflow logs show the deployed Cloud Run URL.
4. Set repo variable `DASHBOARD_UPSTREAM_BASE_URL` to that URL.
5. Re-deploy dashboard Worker (`uk_aq_ops_dashboard_api_worker_deploy.yml`).
