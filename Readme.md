# file: README.md
# Venue City/Country Enrichment (Cloud Run)

Enrich BigQuery rows with `city` and `country` using OpenAI, exposed via an HTTP endpoint on Cloud Run and auto-deployed with Cloud Build.

## What you get
- **BigQuery migration**: adds `city` and `country` columns (if missing).
- **Service**: `/enrich?limit=30000` endpoint runs the enrichment job.
- **Efficiency**: strict JSON extraction, caching in SQLite, de-dup within batch.
- **CI/CD**: `cloudbuild.yaml` builds image, migrates columns, deploys Cloud Run.

---

## One-time Google Cloud setup (no code edits)

1. **Enable APIs**  
   In the GCP project you’ll use:
   - Cloud Run, Cloud Build, Artifact Registry, Secret Manager, BigQuery.

2. **Create an Artifact Registry repo** (once)
   - Name: `app` (or change `_REPOSITORY` in `cloudbuild.yaml`).
   - Format: Docker, Region: your choice (e.g., `us-central1`).

3. **Secret Manager: OpenAI key**
   - Create a secret named **`OPENAI_API_KEY`** with your key value.
   - Grant **Secret Manager Secret Accessor** to:
     - Cloud Run runtime SA (e.g., `PROJECT_NUMBER-compute@developer.gserviceaccount.com` or your custom SA).
     - Cloud Build SA: `PROJECT_NUMBER@cloudbuild.gserviceaccount.com`.

4. **IAM roles**
   - Cloud Build SA needs:
     - Cloud Run Admin
     - Service Account User (on the Cloud Run runtime SA)
     - Artifact Registry Writer
     - Secret Manager Secret Accessor
     - BigQuery Data Editor + BigQuery Job User
   - Cloud Run runtime SA needs:
     - Secret Manager Secret Accessor
     - BigQuery Data Editor + Job User

5. **Connect GitHub to Cloud Build**
   - Create a **trigger** on your repo:
     - Event: push to `main` (or your branch).
     - Config: `cloudbuild.yaml`.
     - Set **Substitutions** (in trigger UI) for:
       - `_REGION` (e.g., `us-central1`)
       - `_SERVICE` (e.g., `venue-enricher`)
       - `_DATASET` (e.g., `my_dataset`)
       - `_TABLE` (e.g., `venues`)
       - `_REPOSITORY` (your Artifact Registry repo, e.g., `app`)
       - Optional: `_OPENAI_MODEL`, `_BATCH_SIZE`, `_CONCURRENCY`

> After the trigger runs, you’ll have a Cloud Run URL like:
> `https://venue-enricher-xxxxx-uc.a.run.app`

---

## How to run enrichment

- **From a browser** (or Cloud Scheduler):
