#!/usr/bin/env bash
# Stop the always-on services so they cost nothing (keeps data, topic and tables).
#   bash infra/teardown.sh
set -euo pipefail
REGION="${REGION:-europe-west3}"
for s in wiki-producer wiki-processor; do
  gcloud run services delete "$s" --region "$REGION" --quiet || true
done
echo "Cloud Run services removed. Redeploy any time with: bash infra/deploy.sh"
