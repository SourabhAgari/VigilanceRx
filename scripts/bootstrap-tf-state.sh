#!/usr/bin/env bash

#
# One-time bootstrap: GCS bucket for Terraform remote state (both stacks).
# Idempotent — safe to re-run; existing bucket is left untouched.
#

set -euo pipefail

PROJECT_ID="vigilancerx-502702"
BUCKET="gs://${PROJECT_ID}-tf-state"
LOCATION="us-central1"

if gcloud storage buckets describe "$BUCKET" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "State bucket already exists: $BUCKET"
else
    echo "Creating state bucket: $BUCKET"

    gcloud storage buckets create "$BUCKET" \
        --project="$PROJECT_ID" \
        --location="$LOCATION" \
        # permission will be granted at bucket level not at the object level (will be disabled)
        --uniform-bucket-level-access \
        # never allow this bucket to be accessed publicly even though admin grants allusers access
        --public-access-prevention
fi

echo "Ensuring object versioning is enabled (state undo button)"

gcloud storage buckets update "$BUCKET" --versioning

echo "Done. Terraform backends can now use bucket: ${PROJECT_ID}-tf-state"