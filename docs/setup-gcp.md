# Google Cloud setup

One-time configuration the daily pipeline needs. Commands are for **PowerShell**
and always pass `--project` explicitly, so your default `gcloud` configuration
is never touched.

```powershell
$PROJECT_ID     = "financial-market-analysis"
$PROJECT_NUMBER = "921931290234"
$REPO           = "antuelquirino/Financial-Market-Analysis"
$SA             = "pipeline-runner@$PROJECT_ID.iam.gserviceaccount.com"
```

## 1. Billing and table expiration

BigQuery's free sandbox deletes tables after 60 days. This project accumulates
history day by day, so it runs with billing enabled (the free tier — 10 GB
storage and 1 TB of queries per month — covers it) and with no expiration on
datasets or tables.

Check that nothing expires:

```powershell
foreach ($d in "raw_finance","analytics_finance") {
  bq --project_id=$PROJECT_ID show --format=prettyjson "${PROJECT_ID}:$d" | Select-String Expiration
  bq --project_id=$PROJECT_ID ls --format=prettyjson "${PROJECT_ID}:$d" | Select-String expirationTime
}
```

No output means no expiration. To remove one:
`bq update --default_table_expiration 0 --default_partition_expiration 0 <project>:<dataset>`
for a dataset, `bq update --expiration 0 <project>:<dataset>.<table>` for a table.

## 2. Service account for the pipeline

`pipeline-runner` is the identity the GitHub Actions workflow runs as. It gets
the minimum it needs: run query jobs in the project, and read/write only the two
datasets of this pipeline. No project-wide data access, no admin roles.

```powershell
gcloud iam service-accounts create pipeline-runner `
  --display-name "Daily market pipeline (GitHub Actions)" --project $PROJECT_ID

# Run BigQuery jobs (queries, loads) in this project
gcloud projects add-iam-policy-binding $PROJECT_ID `
  --member "serviceAccount:$SA" --role roles/bigquery.jobUser --condition None

# Read/write tables, only in the pipeline's datasets
foreach ($d in "raw_finance","analytics_finance") {
  bq query --project_id=$PROJECT_ID --use_legacy_sql=false `
    "GRANT ``roles/bigquery.dataEditor`` ON SCHEMA ``$PROJECT_ID.$d`` TO 'serviceAccount:$SA'"
}
```

## 3. Workload Identity Federation

Instead of storing a service-account JSON key in GitHub (a long-lived secret
that works from anywhere if it leaks), GitHub Actions presents a short-lived
OIDC token that says "I am a workflow of repository X". Google Cloud trusts
tokens from that repository only and exchanges them for credentials of
`pipeline-runner` that expire within an hour. There is nothing to leak or rotate.

```powershell
# APIs used by the token exchange
gcloud services enable iam.googleapis.com iamcredentials.googleapis.com sts.googleapis.com `
  --project $PROJECT_ID

# A pool groups external identities; the provider says "trust GitHub's OIDC issuer"
gcloud iam workload-identity-pools create github `
  --location global --display-name "GitHub Actions" --project $PROJECT_ID

gcloud iam workload-identity-pools providers create-oidc github-repo `
  --location global --workload-identity-pool github `
  --display-name "GitHub repository" `
  --issuer-uri "https://token.actions.githubusercontent.com" `
  --attribute-mapping "google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.ref=assertion.ref" `
  --attribute-condition "assertion.repository == '$REPO'" `
  --project $PROJECT_ID

# Let workflows of this repository (and only this one) act as pipeline-runner
gcloud iam service-accounts add-iam-policy-binding $SA `
  --role roles/iam.workloadIdentityUser `
  --member "principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/github/attribute.repository/$REPO" `
  --project $PROJECT_ID
```

The `--attribute-condition` is the important line: without it, any GitHub
repository could request tokens from this provider.

## 4. GitHub repository variables

In GitHub: **Settings → Secrets and variables → Actions → Variables → New
repository variable**. These are identifiers, not secrets, so they go in
*Variables*:

| Name | Value |
|---|---|
| `GCP_WIF_PROVIDER` | `projects/921931290234/locations/global/workloadIdentityPools/github/providers/github-repo` |
| `GCP_SERVICE_ACCOUNT` | `pipeline-runner@financial-market-analysis.iam.gserviceaccount.com` |

Then run the workflow by hand (**Actions → Daily market pipeline → Run
workflow**) and check that it succeeds.

## 5. Remove the old key-based access

Only after the workflow has succeeded with Workload Identity Federation:

```powershell
# The old loader account had bigquery.admin and two keys that never expire
gcloud iam service-accounts keys list `
  --iam-account "bq-loader@$PROJECT_ID.iam.gserviceaccount.com" --managed-by user --project $PROJECT_ID
gcloud iam service-accounts delete "bq-loader@$PROJECT_ID.iam.gserviceaccount.com" --project $PROJECT_ID
```

Deleting the account invalidates all its keys. Then delete the `GCP_SA_KEY`
secret in GitHub (**Settings → Secrets and variables → Actions → Secrets**).

`streamlit-permit` (read-only, used by the Streamlit app) stays until the new
frontend replaces Streamlit.
