# uk-aq-ingestdb-prune setup (Cloud Run + Scheduler)

This deploys the daily ingest-prune verification job as a Cloud Run service and invokes it from Cloud Scheduler at `02:00 UTC`.

Dry-run includes a repair pilot: it can enqueue one mismatch bucket to the ingest DB history outbox, flush all due outbox entries, and then recheck that bucket.

## 1) Set variables

```bash
export PROJECT_ID="your-project-id"
export PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
export REGION="europe-west2"

export SERVICE_NAME="uk-aq-ingestdb-prune-service"
export JOB_NAME="uk-aq-ingestdb-prune-daily"

export OPS_SA_NAME="uk-aq-ops-job"
export OPS_SA_EMAIL="${OPS_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Required DB URLs
export SUPABASE_URL="https://YOUR_INGEST_PROJECT.supabase.co"
export HISTORY_SUPABASE_URL="https://YOUR_HISTORY_PROJECT.supabase.co"
```

## 2) Create the dedicated service account

```bash
gcloud iam service-accounts create "$OPS_SA_NAME" \
  --project "$PROJECT_ID" \
  --display-name "UK AQ Ops Job"
```

## 3) Grant least-privilege secret access (runtime)

```bash
gcloud secrets add-iam-policy-binding SB_SECRET_KEY \
  --project "$PROJECT_ID" \
  --member "serviceAccount:${OPS_SA_EMAIL}" \
  --role "roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding HISTORY_SECRET_KEY \
  --project "$PROJECT_ID" \
  --member "serviceAccount:${OPS_SA_EMAIL}" \
  --role "roles/secretmanager.secretAccessor"
```

## 4) Deploy Cloud Run service

From the `uk-aq-ops` repo root:

```bash
gcloud run deploy "$SERVICE_NAME" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --source . \
  --service-account "$OPS_SA_EMAIL" \
  --no-allow-unauthenticated \
  --cpu 1 \
  --memory 256Mi \
  --min-instances 0 \
  --max-instances 1 \
  --set-env-vars "SUPABASE_URL=${SUPABASE_URL},HISTORY_SUPABASE_URL=${HISTORY_SUPABASE_URL},DRY_RUN=true,MAX_HOURS_PER_RUN=48,DELETE_BATCH_SIZE=50000,MAX_DELETE_BATCHES_PER_HOUR=10,REPAIR_ONE_MISMATCH_BUCKET=true,REPAIR_BUCKET_OUTBOX_CHUNK_SIZE=1000,FLUSH_CLAIM_BATCH_LIMIT=20,MAX_FLUSH_BATCHES=30" \
  --set-secrets "SB_SECRET_KEY=SB_SECRET_KEY:latest,HISTORY_SECRET_KEY=HISTORY_SECRET_KEY:latest" \
  --labels "job=${JOB_NAME},service=${SERVICE_NAME}"
```

## 5) Allow the service account to invoke Cloud Run

```bash
gcloud run services add-iam-policy-binding "$SERVICE_NAME" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --member "serviceAccount:${OPS_SA_EMAIL}" \
  --role "roles/run.invoker"
```

## 6) Allow Cloud Scheduler service agent to mint OIDC token for the ops SA

```bash
gcloud iam service-accounts add-iam-policy-binding "$OPS_SA_EMAIL" \
  --project "$PROJECT_ID" \
  --member "serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-cloudscheduler.iam.gserviceaccount.com" \
  --role "roles/iam.serviceAccountTokenCreator"
```

## 7) Create the scheduler job (daily 02:00 UTC)

```bash
export SERVICE_URL="$(gcloud run services describe "$SERVICE_NAME" --project "$PROJECT_ID" --region "$REGION" --format='value(status.url)')"

gcloud scheduler jobs create http "$JOB_NAME" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --schedule "0 2 * * *" \
  --time-zone "UTC" \
  --uri "${SERVICE_URL}/run" \
  --http-method POST \
  --oidc-service-account-email "$OPS_SA_EMAIL" \
  --oidc-token-audience "$SERVICE_URL" \
  --headers "Content-Type=application/json" \
  --message-body '{}'
```

If the job already exists, use:

```bash
gcloud scheduler jobs update http "$JOB_NAME" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --schedule "0 2 * * *" \
  --time-zone "UTC" \
  --uri "${SERVICE_URL}/run" \
  --http-method POST \
  --oidc-service-account-email "$OPS_SA_EMAIL" \
  --oidc-token-audience "$SERVICE_URL" \
  --headers "Content-Type=application/json" \
  --message-body '{}'
```

## 8) Ad-hoc run

Run scheduler immediately:

```bash
gcloud scheduler jobs run "$JOB_NAME" \
  --project "$PROJECT_ID" \
  --location "$REGION"
```

Direct dry-run call:

```bash
curl -X POST "${SERVICE_URL}/run?dryRun=true" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token --audiences=${SERVICE_URL})"
```

Direct live delete call:

```bash
curl -X POST "${SERVICE_URL}/run?dryRun=false" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token --audiences=${SERVICE_URL})"
```

## 9) Apply SQL RPC scripts

- In ingest DB SQL editor: run `sql/ingest_db_ops_rpcs.sql`
- In history DB SQL editor: run `sql/history_db_ops_rpcs.sql`

## 10) GitHub Actions deploy workflow

Workflow path:

- `.github/workflows/uk_aq_ingestdb_prune_cloud_run_deploy.yml`

Set these GitHub Actions repository variables:

- `GCP_PROJECT_ID`
- `GCP_REGION` (optional, default `europe-west2`)
- `GCP_ARTIFACT_REPO` (optional, default `uk-aq`)
- `SUPABASE_URL`
- `HISTORY_SUPABASE_URL`
- `GCP_WORKLOAD_IDENTITY_PROVIDER` (recommended auth mode)
- `GCP_SERVICE_ACCOUNT` (existing deploy SA, recommended auth mode)
- `GCP_OPS_PRUNE_RUNTIME_SERVICE_ACCOUNT` (optional; default `uk-aq-ops-job@<project>.iam.gserviceaccount.com`)
- `GCP_OPS_PRUNE_SCHEDULER_SERVICE_ACCOUNT` (optional; defaults to runtime SA)
- `GCP_OPS_PRUNE_SERVICE_NAME` (optional, default `uk-aq-ingestdb-prune-service`)
- `GCP_OPS_PRUNE_SCHEDULER_JOB_NAME` (optional, default `uk-aq-ingestdb-prune-daily`)
- `GCP_OPS_PRUNE_SCHEDULER_CRON` (optional, default `0 2 * * *`)
- `GCP_OPS_PRUNE_SCHEDULER_TIMEZONE` (optional, default `Etc/UTC`)
- `GCP_OPS_PRUNE_DRY_RUN` (optional, default `true`)
- `GCP_OPS_PRUNE_MAX_HOURS_PER_RUN` (optional, default `48`)
- `GCP_OPS_PRUNE_DELETE_BATCH_SIZE` (optional, default `50000`)
- `GCP_OPS_PRUNE_MAX_DELETE_BATCHES_PER_HOUR` (optional, default `10`)
- `GCP_OPS_PRUNE_REPAIR_ONE_MISMATCH_BUCKET` (optional, default `true`)
- `GCP_OPS_PRUNE_REPAIR_BUCKET_OUTBOX_CHUNK_SIZE` (optional, default `1000`)
- `GCP_OPS_PRUNE_FLUSH_CLAIM_BATCH_LIMIT` (optional, default `20`)
- `GCP_OPS_PRUNE_MAX_FLUSH_BATCHES` (optional, default `30`)
- `SB_SECRET_KEY_SECRET_NAME` (optional, default `SB_SECRET_KEY`)
- `HISTORY_SECRET_KEY_SECRET_NAME` (optional, default `HISTORY_SECRET_KEY`)

Optional GitHub Actions secret for fallback auth mode:

- `GCP_SA_KEY` (only needed when not using Workload Identity Federation)
