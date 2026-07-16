#!/usr/bin/env bash

###############################################################################
# Bootstrap local Redpanda environment
#
# Responsibilities
#   1. Wait until Redpanda is healthy
#   2. Create Kafka topics (idempotent)
#   3. Register Avro schemas in Schema Registry
#   4. Configure schema compatibility (FULL_TRANSITIVE)
#   5. Print created topics and registered subjects
###############################################################################

set -euo pipefail

###############################################################################
# Configuration
###############################################################################

REGISTRY="http://localhost:8081"

TOPICS=(
    rx-fill-events
    ndc-drug-class-ref
    alert-lead-time-ref
    gap-risk-alerts
    lapsed-alerts
    pdc-snapshots
    dead-letter
)

###############################################################################
# Wait for Redpanda
###############################################################################

echo "Waiting for Redpanda to become healthy..."

docker exec redpanda \
    rpk cluster health \
    --watch \
    --exit-when-healthy \
    >/dev/null

echo "Redpanda is healthy."

###############################################################################
# Topic configuration
#
# Local development uses three partitions for the primary event stream so that
# Flink watermark idleness and partition assignment can be tested.
###############################################################################

partitions_for() {
    case "$1" in
        rx-fill-events)
            echo 3
            ;;
        *)
            echo 1
            ;;
    esac
}

###############################################################################
# Create topics if they do not already exist
###############################################################################

echo
echo "Creating Kafka topics..."

for topic in "${TOPICS[@]}"; do

    if docker exec redpanda rpk topic describe "$topic" >/dev/null 2>&1; then
        echo "✔ Topic already exists: $topic"
    else
        docker exec redpanda \
            rpk topic create \
            "$topic" \
            -p "$(partitions_for "$topic")" \
            -r 1

        echo "✔ Created topic: $topic"
    fi

done

###############################################################################
# Register Avro schema
#
# Subject naming follows TopicNameStrategy:
#
#     <topic>-value
#
# Schema Registry expects the Avro schema itself to be embedded as a JSON
# string:
#
# {
#     "schema": "{ ...escaped avsc... }"
# }
###############################################################################

register_schema() {

    local subject="$1"
    local schema_file="$2"

    #
    # Convert .avsc file into the payload expected by Schema Registry.
    #
    python3 -c '
import json
import sys

print(json.dumps({
    "schema": open(sys.argv[1]).read()
}))
' "$schema_file" |
        curl -sf \
            -X POST \
            -H "Content-Type: application/vnd.schemaregistry.v1+json" \
            --data @- \
            "$REGISTRY/subjects/$subject/versions" \
            >/dev/null

    #
    # Configure compatibility mode.
    #
    curl -sf \
        -X PUT \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d '{"compatibility":"FULL_TRANSITIVE"}' \
        "$REGISTRY/config/$subject" \
        >/dev/null

    echo "✔ Registered schema: $subject (FULL_TRANSITIVE)"
}

###############################################################################
# Register schemas
###############################################################################

echo
echo "Registering Avro schemas..."

register_schema \
    rx-fill-events-value \
    src/main/resources/rx-fill-event.avsc

register_schema \
    gap-risk-alerts-value \
    src/main/resources/gap-risk-alert.avsc

register_schema \
    lapsed-alerts-value \
    src/main/resources/lapsed-alert.avsc

###############################################################################
# Summary
###############################################################################

echo
echo "=============================="
echo "Kafka Topics"
echo "=============================="

docker exec redpanda rpk topic list

echo
echo "=============================="
echo "Schema Registry Subjects"
echo "=============================="

curl -s "$REGISTRY/subjects"
echo

echo
echo "Bootstrap completed successfully."