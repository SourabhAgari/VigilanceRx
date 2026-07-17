#!/usr/bin/env bash

set -euo pipefail

# Configuration
BUCKET="gs://vigilancerx-502702-tf-state"
LOCATION="us-central1"

# Check if the Terraform state bucket already exists
if gcloud storage buckets describe "$BUCKET" >/dev/null 2>&1; then
    echo "State bucket already exists: $BUCKET"
else
    # Create the Terraform state bucket
    gcloud storage buckets create "$BUCKET" \
        --location="$LOCATION" \
        --uniform-bucket-level-access \
        --public-access-prevention=enforced
fi

gcloud storage buckets update "$BUCKET" --versioning