# path: ops/verify.sh
#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-rfp-database-464609}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-venue-enricher}"
URL="$(gcloud run services describe ${SERVICE} --region ${REGION} --format='value(status.url)')"

echo "# health"
curl -s "${URL}/health" | jq .

echo "# stats (no cache)"
curl -s "${URL}/stats" | jq .

echo "# run small verbose batch"
curl -s -X POST "${URL}/enrich?limit=10&overwrite=false&verbose=true" | jq .

echo "# stats again"
curl -s "${URL}/stats" | jq .

echo "# request logs"
gcloud logging read \
  'logName:"run.googleapis.com%2Frequests"
   resource.type="cloud_run_revision"
   resource.labels.service_name="'"${SERVICE}"'"
   jsonPayload.request_method="POST"
   jsonPayload.request_url=~"/enrich.*"' \
  --project "${PROJECT_ID}" --limit 10 \
  --format='table(timestamp, jsonPayload.status, jsonPayload.latency, jsonPayload.request_url)'

echo "# service logs (follow CTRL+C to stop)"
gcloud run services logs read "${SERVICE}" --region "${REGION}" --follow \
  | grep '"event": "enrich_row"' || true
