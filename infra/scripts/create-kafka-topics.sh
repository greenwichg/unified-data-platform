#!/bin/bash
###############################################################################
# Create Kafka Topics for all 4 Pipelines
#
# MIGRATION NOTE: In production, this runs against Amazon MSK with IAM
# authentication. Set MSK_ENABLED=true and MSK_BOOTSTRAP to use MSK.
# When using MSK with IAM auth, ensure the AWS CLI/SDK credentials have
# the kafka-cluster:CreateTopic permission and that the
# --command-config flag points to a properties file with:
#   security.protocol=SASL_SSL
#   sasl.mechanism=AWS_MSK_IAM
#   sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
#   sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
###############################################################################

set -euo pipefail

MSK_ENABLED="${MSK_ENABLED:-false}"
MSK_BOOTSTRAP="${MSK_BOOTSTRAP:-}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
PARTITIONS="${PARTITIONS:-64}"
REPLICATION="${REPLICATION:-3}"

# Use MSK bootstrap servers if enabled
if [[ "$MSK_ENABLED" == "true" && -n "$MSK_BOOTSTRAP" ]]; then
    KAFKA_BOOTSTRAP="$MSK_BOOTSTRAP"
    echo "Using Amazon MSK cluster: $KAFKA_BOOTSTRAP"
    COMMAND_CONFIG_FLAG="--command-config /etc/kafka/msk-iam.properties"
else
    COMMAND_CONFIG_FLAG=""
fi

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
    kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" $COMMAND_CONFIG_FLAG \
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
    kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" $COMMAND_CONFIG_FLAG \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 1 \
        --replication-factor "$REPLICATION" \
        --config cleanup.policy=compact
done

# Druid ingestion topic (Pipeline 4 → Druid)
echo "Creating Druid ingestion topic"
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" $COMMAND_CONFIG_FLAG \
    --create --if-not-exists \
    --topic "druid-ingestion-events" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION" \
    --config retention.ms=172800000 \
    --config compression.type=lz4

# Alert/feedback topics (Complex Event Processing)
echo "Creating alert topics"
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" $COMMAND_CONFIG_FLAG \
    --create --if-not-exists \
    --topic "order-spike-alerts" \
    --partitions 8 \
    --replication-factor "$REPLICATION" \
    --config retention.ms=86400000

echo ""
echo "All Kafka topics created successfully!"
echo ""
echo "Listing all topics:"
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" $COMMAND_CONFIG_FLAG --list
