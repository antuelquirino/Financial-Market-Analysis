# Deploy

The API runs on **Cloud Run** and the dashboard on **Vercel**. Both deploy
themselves on every push to `main`: GitHub Actions builds and deploys the API
when `api/` changes (after its tests pass), and Vercel rebuilds the dashboard.
Monthly cost is close to zero: Cloud Run scales to zero, and the free tiers of
Cloud Run, Artifact Registry and BigQuery cover a portfolio's traffic.

This recipe is written to be reused by other projects (InsightFlow next):
change the variables at the top and the names that depend on them.

```
GitHub push to main
  ├─ api/ changed → .github/workflows/deploy-api.yml
  │     tests → docker build → Artifact Registry → Cloud Run (market-api)
  └─ web/ changed → Vercel builds web/ → https://<project>.vercel.app
                                   │  server-side fetch (API_URL)
                                   ▼
                        Cloud Run market-api ── reads ──▶ BigQuery marts
```

The browser never calls the API: Next.js server components do. So the API's
URL is a server-side variable in Vercel (`API_URL`), and CORS is a second
fence rather than the main one.

## One-time Google Cloud setup

All commands are for **PowerShell** and pass `--project` explicitly, so the
default `gcloud` configuration is never touched. They assume the Workload
Identity Federation pool from [setup-gcp.md](setup-gcp.md) already exists.

```powershell
$PROJECT_ID     = "financial-market-analysis"
$PROJECT_NUMBER = "921931290234"
$REGION         = "us-central1"        # Cloud Run's free tier region
$REPO           = "antuelquirino/Financial-Market-Analysis"
$BILLING        = "01E4E1-E90F93-79C453"
$RUNTIME_SA     = "market-api@$PROJECT_ID.iam.gserviceaccount.com"
$DEPLOY_SA      = "api-deployer@$PROJECT_ID.iam.gserviceaccount.com"
```

### 1. APIs

Cloud Run runs the container, Artifact Registry stores its images, and the
Budgets API lets the budget below be created from the command line.

```powershell
gcloud services enable run.googleapis.com artifactregistry.googleapis.com billingbudgets.googleapis.com `
  --project $PROJECT_ID
```

### 2. Artifact Registry with a cleanup policy

Every deploy pushes a new image. The cleanup policy keeps the 5 most recent
and deletes the rest, so storage never grows. Keep rules win over delete
rules, which is how "delete everything except the last 5" is written.

```powershell
gcloud artifacts repositories create market --repository-format docker `
  --location $REGION --description "Financial Market container images" --project $PROJECT_ID

$policy = '[{"name":"keep-last-5","action":{"type":"Keep"},"mostRecentVersions":{"keepCount":5}},' +
          '{"name":"delete-the-rest","action":{"type":"Delete"},"condition":{"tagState":"ANY"}}]'
$file = Join-Path $env:TEMP "cleanup-policy.json"; [IO.File]::WriteAllText($file, $policy)
gcloud artifacts repositories set-cleanup-policies market --location $REGION `
  --policy $file --no-dry-run --project $PROJECT_ID
```

### 3. The API's runtime account: read-only

Cloud Run runs the container as `market-api`. It can run queries and read the
marts dataset, nothing else: no writes, no raw data, no other datasets. The
image contains no credentials; the BigQuery client gets this account's
short-lived token from Cloud Run's metadata server.

```powershell
gcloud iam service-accounts create market-api `
  --display-name "Financial Market API (Cloud Run, read-only)" --project $PROJECT_ID

gcloud projects add-iam-policy-binding $PROJECT_ID `
  --member "serviceAccount:$RUNTIME_SA" --role roles/bigquery.jobUser --condition None

bq query --project_id=$PROJECT_ID --use_legacy_sql=false `
  "GRANT ``roles/bigquery.dataViewer`` ON SCHEMA ``$PROJECT_ID.analytics_finance`` TO 'serviceAccount:$RUNTIME_SA'"
```

### 4. The deploy account, for GitHub Actions

`api-deployer` is what the deploy workflow runs as, through Workload Identity
Federation (no key). It can push images to this one repository, deploy Cloud
Run services, and attach `market-api` to them (`serviceAccountUser` on that
account only). It cannot read data or change IAM.

```powershell
gcloud iam service-accounts create api-deployer `
  --display-name "Deploys the API from GitHub Actions" --project $PROJECT_ID

gcloud projects add-iam-policy-binding $PROJECT_ID `
  --member "serviceAccount:$DEPLOY_SA" --role roles/run.developer --condition None

gcloud artifacts repositories add-iam-policy-binding market --location $REGION `
  --member "serviceAccount:$DEPLOY_SA" --role roles/artifactregistry.writer --project $PROJECT_ID

gcloud iam service-accounts add-iam-policy-binding $RUNTIME_SA `
  --member "serviceAccount:$DEPLOY_SA" --role roles/iam.serviceAccountUser --project $PROJECT_ID

gcloud iam service-accounts add-iam-policy-binding $DEPLOY_SA `
  --role roles/iam.workloadIdentityUser `
  --member "principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/github/attribute.repository/$REPO" `
  --project $PROJECT_ID
```

### 5. A budget with alerts

A budget does not cap spending; it emails the billing admins at 50%, 90% and
100% of US$5 for this project, so a surprise is caught within a day.

```powershell
gcloud billing budgets create --billing-account $BILLING `
  --display-name "Financial Market - 5 USD" --budget-amount 5USD `
  --filter-projects "projects/$PROJECT_ID" `
  --threshold-rule percent=0.5 --threshold-rule percent=0.9 --threshold-rule percent=1.0
```

The real cost caps are in the service itself: at most 2 instances, 512 MiB
and 1 vCPU each, scale to zero, and `maximum_bytes_billed` on every query.

### 6. GitHub repository variables

**Settings → Secrets and variables → Actions → Variables.** Identifiers, not
secrets:

| Name | Value |
|---|---|
| `GCP_WIF_PROVIDER` | `projects/921931290234/locations/global/workloadIdentityPools/github/providers/github-repo` (from setup-gcp.md) |
| `GCP_DEPLOY_SERVICE_ACCOUNT` | `api-deployer@financial-market-analysis.iam.gserviceaccount.com` |
| `API_ALLOWED_ORIGINS` | The dashboard's URL, e.g. `https://financial-market.vercel.app` (comma-separated if several) |

## The API on Cloud Run

[deploy-api.yml](../.github/workflows/deploy-api.yml) runs on pushes to
`main` that touch `api/`, the `Dockerfile` or the workflow, and by hand from
the Actions tab:

1. **test:** the API's pytest suite. If it fails, nothing is deployed.
2. **deploy:** builds the two-stage image (dependencies built in one stage,
   copied into a clean `python:3.11-slim` in the next), pushes it tagged with
   the commit SHA, and deploys `market-api` with 0–2 instances, 512 MiB,
   1 vCPU, CPU boost at startup, and `GCP_PROJECT` / `ALLOWED_ORIGINS` as
   environment variables. Then it calls `/health`.

There are no secrets to store: the API needs no keys. If one is ever needed,
put it in Secret Manager and mount it with `--set-secrets`, never as a plain
environment variable.

**After the first deploy only**, make the service public. The deploy account
cannot change IAM on purpose, so this is a one-time step by a project owner:

```powershell
gcloud run services add-iam-policy-binding market-api --region $REGION `
  --member allUsers --role roles/run.invoker --project $PROJECT_ID
```

## The dashboard on Vercel

1. In [vercel.com/new](https://vercel.com/new), import the GitHub repository.
2. **Root Directory:** `web`. The framework (Next.js) is detected.
3. **Environment Variables:** `API_URL` = the Cloud Run URL (from the deploy
   workflow's summary, or
   `gcloud run services describe market-api --region us-central1 --project financial-market-analysis --format "value(status.url)"`).
4. **Deploy.** Every later push to `main` redeploys; pull requests get preview
   URLs.
5. Put the production URL in the `API_ALLOWED_ORIGINS` repository variable
   and re-run **Deploy API** so CORS allows it.

`API_URL` is read on the server at request time, so changing it needs a
redeploy in Vercel but no code change.

## Checking a deploy

```powershell
$API = gcloud run services describe market-api --region $REGION --project $PROJECT_ID --format "value(status.url)"
curl.exe -s "$API/health"
curl.exe -s "$API/status"
# CORS: an allowed origin gets the header back, any other origin does not
curl.exe -s -D - -o NUL -H "Origin: https://financial-market.vercel.app" "$API/health" | Select-String access-control
curl.exe -s -D - -o NUL -H "Origin: https://evil.example" "$API/health" | Select-String access-control
```
