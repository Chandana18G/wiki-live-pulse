#!/usr/bin/env bash
# One-time GCP setup for Wikipedia Live Pulse.
# Easiest place to run it: Google Cloud Shell (gcloud, bq and docker are preinstalled).
#
#   gcloud config set project <YOUR_PROJECT_ID>
#   bash infra/setup.sh
#
# Safe to run again: existing resources are kept.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-europe-west3}"          # Frankfurt
TOPIC="wiki-edits"
SUBSCRIPTION="wiki-edits-processor"
REPO="wiki"

if [[ -z "$PROJECT_ID" ]]; then
  echo "No project set. Run: gcloud config set project <YOUR_PROJECT_ID>" >&2
  exit 1
fi
echo "== Project: $PROJECT_ID   Region: $REGION"

echo "== 1/6 Enabling APIs"
gcloud services enable \
  pubsub.googleapis.com run.googleapis.com bigquery.googleapis.com \
  artifactregistry.googleapis.com cloudbuild.googleapis.com

echo "== 2/6 Pub/Sub topic (7-day retention, so we can replay) and subscription"
gcloud pubsub topics describe "$TOPIC" >/dev/null 2>&1 || \
  gcloud pubsub topics create "$TOPIC" --message-retention-duration=7d
gcloud pubsub subscriptions describe "$SUBSCRIPTION" >/dev/null 2>&1 || \
  gcloud pubsub subscriptions create "$SUBSCRIPTION" --topic="$TOPIC" \
    --ack-deadline=60 --message-retention-duration=7d

echo "== 3/6 BigQuery dataset, tables and gold views"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bq --location="$REGION" query --use_legacy_sql=false < "$SCRIPT_DIR/../sql/01_create_tables.sql"
bq --location="$REGION" query --use_legacy_sql=false < "$SCRIPT_DIR/../sql/02_gold_views.sql"

echo "== 4/6 Artifact Registry repository for our Docker images"
gcloud artifacts repositories describe "$REPO" --location="$REGION" >/dev/null 2>&1 || \
  gcloud artifacts repositories create "$REPO" --repository-format=docker --location="$REGION"

echo "== 5/6 Service accounts (one per component, least privilege)"
for sa in wiki-producer wiki-processor; do
  gcloud iam service-accounts describe "$sa@$PROJECT_ID.iam.gserviceaccount.com" >/dev/null 2>&1 || \
    gcloud iam service-accounts create "$sa" --display-name="$sa"
done
PRODUCER_SA="wiki-producer@$PROJECT_ID.iam.gserviceaccount.com"
PROCESSOR_SA="wiki-processor@$PROJECT_ID.iam.gserviceaccount.com"

echo "== 6/6 Permissions"
# producer may only publish to the topic
gcloud pubsub topics add-iam-policy-binding "$TOPIC" \
  --member="serviceAccount:$PRODUCER_SA" --role=roles/pubsub.publisher >/dev/null
# processor may only read its subscription ...
gcloud pubsub subscriptions add-iam-policy-binding "$SUBSCRIPTION" \
  --member="serviceAccount:$PROCESSOR_SA" --role=roles/pubsub.subscriber >/dev/null
# ... and write BigQuery tables
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$PROCESSOR_SA" --role=roles/bigquery.dataEditor --condition=None >/dev/null

echo
echo "Setup done. Next: bash infra/deploy.sh"
echo "Reminder: set a budget alert in the console (Billing > Budgets & alerts)."
