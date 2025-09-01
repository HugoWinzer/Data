# path: ops/deploy.sh
#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-rfp-database-464609}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-venue-enricher}"
RUN_SA="${RUN_SA:-rfp-enricher-sa@${PROJECT_ID}.iam.gserviceaccount.com}"

gcloud run deploy "${SERVICE}" \
  --source . \
  --region "${REGION}" \
  --allow-unauthenticated \
  --service-account "${RUN_SA}" \
  --cpu "1" \
  --memory "512Mi" \
  --timeout "3600" \
  --set-env-vars "PROJECT_ID=${PROJECT_ID},DATASET_ID=rfpdata,TABLE_ID=OUTPUT,OPENAI_MODEL=gpt-4o-mini,BATCH_SIZE=200,CONCURRENCY=8" \
  --set-secrets "OPENAI_API_KEY=OPENAI_API_KEY:latest"
