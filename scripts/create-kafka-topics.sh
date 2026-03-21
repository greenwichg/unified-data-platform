#!/bin/bash
###############################################################################
# Create Kafka Topics for all 4 Pipelines
# Run against the self-hosted Kafka cluster
###############################################################################

set -euo pipefail

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
PARTITIONS="${PARTITIONS:-64}"
REPLICATION="${REPLICATION:-3}"

echo "Creating Kafka topics on: $KAFKA_BOOTSTRAP"
echo "Partitions: $PARTITIONS, Replication: $REPLICATION"

# Core data topics (used by Pipeline 2 CDC and Pipeline 4 Real-time)
TOPICS=(
    "orders"
    "users"
    "menu"
    "promo"
    "topics"
)

for topic in "${TOPICS[@]}"; do
    echo "Creating topic: $topic"
    kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION" \
        --config retention.ms=604800000 \
        --config compression.type=lz4 \
        --config min.insync.replicas=2 \
        --config cleanup.policy=delete
done

# Debezium internal topics
DEBEZIUM_TOPICS=(
    "debezium-configs"
    "debezium-offsets"
    "debezium-status"
    "schema-changes.orders"
    "schema-changes.users"
    "schema-changes.menu"
    "schema-changes.promo"
)

for topic in "${DEBEZIUM_TOPICS[@]}"; do
    echo "Creating Debezium topic: $topic"
    kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 1 \
        --replication-factor "$REPLICATION" \
        --config cleanup.policy=compact
done

# Druid ingestion topic (Pipeline 4 → Druid)
echo "Creating Druid ingestion topic"
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic "druid-ingestion-events" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION" \
    --config retention.ms=172800000 \
    --config compression.type=lz4

# Alert/feedback topics (Complex Event Processing)
echo "Creating alert topics"
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic "order-spike-alerts" \
    --partitions 8 \
    --replication-factor "$REPLICATION" \
    --config retention.ms=86400000

echo ""
echo "All Kafka topics created successfully!"
echo ""
echo "Listing all topics:"
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --list
