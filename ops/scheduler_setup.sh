# path: ops/scheduler_setup.sh
#!/usr/bin/env bash
set -euo pipefail

# --- config ---
PROJECT_ID="${PROJECT_ID:-rfp-database-464609}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-venue-enricher}"
JOB_ID="${JOB_ID:-venue-enricher-batches}"
SCHED_SA="${SCHED_SA:-enricher-scheduler@${PROJECT_ID}.iam.gserviceaccount.com}"
CRON="${CRON:-*/2 * * * *}"            # every 2 minutes to start
LIMIT_QS="${LIMIT_QS:-limit=1000&overwrite=false&verbose=false}"
# ---------------

# 0) Preflight
ACTIVE="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' || true)"
if [[ -z "${ACTIVE}" ]]; then
  echo "No active gcloud account. Run:  gcloud auth login" >&2
  exit 1
fi

gcloud config set project "${PROJECT_ID}" >/dev/null
gcloud config set run/region "${REGION}" >/dev/null

URL="$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)' || true)"
if [[ -z "${URL}" ]]; then
  echo "Cloud Run URL empty. Check SERVICE/REGION/project." >&2
  exit 1
fi
echo "Cloud Run URL: ${URL}"

# 1) Verify scheduler SA exists
if ! gcloud iam service-accounts list --format='value(email)' \
  | grep -qx "${SCHED_SA}"; then
  echo "Service account not found: ${SCHED_SA}" >&2
  echo "Pick an existing one from your list or create it, then re-run." >&2
  exit 1
fi

# 2) Grant run.invoker (idempotent)
echo "Granting roles/run.invoker to ${SCHED_SA} on ${SERVICE}…"
gcloud run services add-iam-policy-binding "${SERVICE}" \
  --region "${REGION}" \
  --member="serviceAccount:${SCHED_SA}" \
  --role="roles/run.invoker" >/dev/null || true

# 3) Create/update Scheduler job (OIDC)
URI="${URL}/enrich?${LIMIT_QS}"
echo "Ensuring Scheduler job ${JOB_ID} -> ${URI}"
gcloud scheduler jobs create http "${JOB_ID}" \
  --location="${REGION}" \
  --http-method=POST \
  --uri="${URI}" \
  --headers="Content-Type=application/json" \
  --oidc-service-account-email="${SCHED_SA}" \
  --oidc-token-audience="${URL}" \
  --schedule="${CRON}" \
  --time-zone="UTC" \
  --attempt-deadline="1800s" >/dev/null || \
gcloud scheduler jobs update http "${JOB_ID}" \
  --location="${REGION}" \
  --uri="${URI}" \
  --headers="Content-Type=application/json" \
  --oidc-service-account-email="${SCHED_SA}" \
  --oidc-token-audience="${URL}" \
  --schedule="${CRON}" \
  --time-zone="UTC" \
  --attempt-deadline="1800s" >/dev/null

echo "Done. Job is scheduled: ${CRON}"
