# path: ops/scheduler_run_once_and_check.sh
#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-rfp-database-464609}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-venue-enricher}"
JOB_ID="${JOB_ID:-venue-enricher-batches}"

URL="$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)')"

# 1) Kick one run (non-blocking)
gcloud scheduler jobs run "${JOB_ID}" --location "${REGION}" >/dev/null || true
echo "Triggered ${JOB_ID}"

# 2) Show last few /enrich requests (NO streaming)
gcloud logging read \
  'logName:"run.googleapis.com%2Frequests"
   resource.type="cloud_run_revision"
   resource.labels.service_name="'"${SERVICE}"'"
   jsonPayload.request_method="POST"
   jsonPayload.request_url=~"/enrich.*"' \
  --project "${PROJECT_ID}" --limit 10 \
  --format='table(timestamp, jsonPayload.status, jsonPayload.latency, jsonPayload.request_url)'

# 3) App summary (NO follow)
gcloud run services logs read "${SERVICE}" --region "${REGION}" --limit 50 \
  --format='value(textPayload)' | tail -n 20

# 4) Pending from API (if /stats exists) else BQ (no cache)
if curl -sf "${URL}/stats" >/dev/null 2>&1; then
  echo "# /stats"
  curl -s "${URL}/stats"
else
  echo "# BigQuery pending"
  bq query --use_legacy_sql=false --nouse_cache \
  "SELECT COUNT(*) AS pending
   FROM \`${PROJECT_ID}.rfpdata.OUTPUT\`
   WHERE enrichment_status='OK' AND (city IS NULL OR country IS NULL);"
fi
