#!/usr/bin/env bash
# Build the Docker images with Cloud Build and deploy them to Cloud Run.
# No local Docker needed. Run after infra/setup.sh:
#
#   bash infra/deploy.sh             # producer + processor
#   bash infra/deploy.sh producer    # only one of them
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-europe-west3}"
REPO="wiki"
TAG="$(date +%Y%m%d-%H%M%S)"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_BASE="$REGION-docker.pkg.dev/$PROJECT_ID/$REPO"
if [[ $# -gt 0 ]]; then TARGETS=("$@"); else TARGETS=(producer processor); fi

# Both are long-running stream workers, not request handlers:
#   --no-cpu-throttling  keeps the CPU on between requests
#   --min-instances 1    keeps one instance always running
#   --max-instances 1    one stream reader is enough (and avoids duplicate producers)
deploy() {
  local name="$1" sa="$2" env="$3"
  local image="$IMAGE_BASE/$name:$TAG"
  echo "== Building $image"
  gcloud builds submit "$ROOT/$name" --tag "$image"
  echo "== Deploying $name"
  gcloud run deploy "wiki-$name" \
    --image "$image" --region "$REGION" \
    --service-account "$sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --set-env-vars "$env" \
    --no-cpu-throttling --min-instances 1 --max-instances 1 \
    --cpu 1 --memory 512Mi \
    --no-allow-unauthenticated
}

for t in "${TARGETS[@]}"; do
  case "$t" in
    producer)  deploy producer  wiki-producer  "PROJECT_ID=$PROJECT_ID,TOPIC_ID=wiki-edits" ;;
    processor) deploy processor wiki-processor "PROJECT_ID=$PROJECT_ID,SUBSCRIPTION_ID=wiki-edits-processor" ;;
    *) echo "Unknown target: $t (use producer or processor)" >&2; exit 1 ;;
  esac
done

echo
echo "Deployed. Check:"
echo "  gcloud run services list --region $REGION"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM wiki.silver_edits'"
