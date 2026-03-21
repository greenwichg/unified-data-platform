#!/usr/bin/env bash
# ============================================================================
# Zomato Data Platform - Kafka Topic Creation Script (MSK)
# Creates all required topics with appropriate partition counts and configs
# Designed for 450M messages/min throughput across the cluster
#
# Usage:
#   ./create_topics.sh [--bootstrap-server b-1.msk-cluster:9094] [--dry-run]
#
# Prerequisites:
#   - kafka-topics.sh available in PATH (or set KAFKA_HOME)
#   - AWS MSK cluster must be running
#   - IAM auth configured (aws-msk-iam-auth library on classpath)
# ============================================================================

set -euo pipefail

# ---- Configuration ----
BOOTSTRAP_SERVER="${1:-${MSK_BOOTSTRAP_TLS:-b-1.zomato-data-platform-msk.kafka.ap-south-1.amazonaws.com:9094}}"
KAFKA_BIN="${KAFKA_HOME:-/opt/kafka}/bin/kafka-topics.sh"
REPLICATION_FACTOR=3
DRY_RUN=false

# ---- IAM Auth Command Config ----
COMMAND_CONFIG_FILE=$(mktemp /tmp/msk-client-XXXXXX.properties)
cat > "$COMMAND_CONFIG_FILE" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF
trap "rm -f $COMMAND_CONFIG_FILE" EXIT

# Parse arguments
for arg in "$@"; do
    case $arg in
        --bootstrap-server=*)
            BOOTSTRAP_SERVER="${arg#*=}"
            ;;
        --dry-run)
            DRY_RUN=true
            ;;
        --help|-h)
            echo "Usage: $0 [--bootstrap-server=host:port] [--dry-run]"
            exit 0
            ;;
    esac
done

# ---- Helper Functions ----
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

create_topic() {
    local topic_name="$1"
    local partitions="$2"
    local retention_ms="${3:-604800000}"    # default 7 days
    local cleanup_policy="${4:-delete}"
    local compression="${5:-lz4}"
    local extra_configs="${6:-}"

    log "Creating topic: ${topic_name} (partitions=${partitions}, retention=${retention_ms}ms, cleanup=${cleanup_policy})"

    if [ "$DRY_RUN" = true ]; then
        log "[DRY RUN] Would create topic: ${topic_name}"
        return 0
    fi

    # Check if topic already exists
    if $KAFKA_BIN --bootstrap-server "$BOOTSTRAP_SERVER" --command-config "$COMMAND_CONFIG_FILE" --list 2>/dev/null | grep -qx "$topic_name"; then
        log "Topic ${topic_name} already exists. Updating configs..."
        $KAFKA_BIN --bootstrap-server "$BOOTSTRAP_SERVER" --command-config "$COMMAND_CONFIG_FILE" \
            --alter --topic "$topic_name" \
            --partitions "$partitions" 2>/dev/null || true
    else
        local cmd="$KAFKA_BIN --bootstrap-server $BOOTSTRAP_SERVER --command-config $COMMAND_CONFIG_FILE \
            --create \
            --topic $topic_name \
            --partitions $partitions \
            --replication-factor $REPLICATION_FACTOR \
            --config retention.ms=$retention_ms \
            --config cleanup.policy=$cleanup_policy \
            --config compression.type=$compression \
            --config min.insync.replicas=2 \
            --config segment.bytes=1073741824"

        if [ -n "$extra_configs" ]; then
            cmd="$cmd $extra_configs"
        fi

        eval "$cmd"
        log "Created topic: ${topic_name}"
    fi
}

# ---- Main ----
log "============================================"
log "Zomato Kafka Topic Creation (MSK)"
log "Bootstrap: ${BOOTSTRAP_SERVER}"
log "Auth: IAM (SASL_SSL)"
log "Replication Factor: ${REPLICATION_FACTOR}"
log "Dry Run: ${DRY_RUN}"
log "============================================"

# ================================================================
# Core Business Event Topics
# High-throughput topics for real-time order processing pipeline
# ================================================================

# Orders - highest throughput topic, ~200M msgs/min
# 256 partitions for maximum parallelism across Flink consumers
create_topic "orders" 256 \
    604800000 \
    "delete" \
    "lz4" \
    "--config max.message.bytes=5242880 --config message.timestamp.type=LogAppendTime"

# Orders - dead letter queue for failed processing
create_topic "orders.dlq" 32 \
    2592000000 \
    "delete" \
    "lz4"

# Menu updates from restaurant partners
create_topic "menu" 64 \
    604800000 \
    "delete" \
    "lz4"

# Promotions and coupon events
create_topic "promo" 32 \
    604800000 \
    "delete" \
    "lz4"

# User profile events
create_topic "users" 64 \
    604800000 \
    "delete" \
    "lz4"

# ================================================================
# Clickstream & App Events
# High-volume user interaction data for analytics and ML
# ================================================================

# Clickstream - ~150M msgs/min from mobile and web
create_topic "clickstream" 128 \
    259200000 \
    "delete" \
    "lz4" \
    "--config max.message.bytes=2097152"

# Search events for search analytics and ranking
create_topic "search.events" 64 \
    259200000 \
    "delete" \
    "lz4"

# ================================================================
# CDC Topics (Debezium - Aurora PostgreSQL)
# Compact topics for Change Data Capture
# ================================================================

# Aurora CDC - Orders database
create_topic "cdc.aurora.orders.orders" 64 \
    -1 \
    "compact" \
    "lz4" \
    "--config min.compaction.lag.ms=3600000 --config delete.retention.ms=86400000"

create_topic "cdc.aurora.orders.order_items" 64 \
    -1 \
    "compact" \
    "lz4" \
    "--config min.compaction.lag.ms=3600000 --config delete.retention.ms=86400000"

# Aurora CDC - Users database
create_topic "cdc.aurora.users.users" 64 \
    -1 \
    "compact" \
    "lz4" \
    "--config min.compaction.lag.ms=3600000 --config delete.retention.ms=86400000"

create_topic "cdc.aurora.users.addresses" 64 \
    -1 \
    "compact" \
    "lz4" \
    "--config min.compaction.lag.ms=3600000 --config delete.retention.ms=86400000"

# Aurora CDC - Restaurants database
create_topic "cdc.aurora.restaurants.restaurants" 64 \
    -1 \
    "compact" \
    "lz4" \
    "--config min.compaction.lag.ms=3600000 --config delete.retention.ms=86400000"

create_topic "cdc.aurora.restaurants.menu_items" 64 \
    -1 \
    "compact" \
    "lz4" \
    "--config min.compaction.lag.ms=3600000 --config delete.retention.ms=86400000"

# Aurora CDC - Payments database
create_topic "cdc.aurora.payments.transactions" 64 \
    -1 \
    "compact" \
    "lz4" \
    "--config min.compaction.lag.ms=3600000 --config delete.retention.ms=86400000"

# ================================================================
# Internal / Infrastructure Topics
# ================================================================

# Flink checkpointing and state
create_topic "flink.checkpoints" 16 \
    86400000 \
    "delete" \
    "lz4"

# Enriched events (output of Flink stream processing)
create_topic "orders.enriched" 128 \
    604800000 \
    "delete" \
    "lz4" \
    "--config max.message.bytes=10485760"

# Real-time aggregates for dashboards
create_topic "realtime.metrics" 32 \
    86400000 \
    "delete" \
    "lz4"

# Fraud detection events
create_topic "fraud.signals" 32 \
    2592000000 \
    "delete" \
    "lz4"

# Notifications
create_topic "notifications.push" 64 \
    172800000 \
    "delete" \
    "lz4"

create_topic "notifications.sms" 32 \
    172800000 \
    "delete" \
    "lz4"

# ================================================================
# Schema Registry internal topic
# ================================================================
create_topic "_schemas" 1 \
    -1 \
    "compact" \
    "uncompressed" \
    "--config min.insync.replicas=2"

# ================================================================
# Summary
# ================================================================
log "============================================"
log "Topic creation complete."
log "Listing all topics:"
$KAFKA_BIN --bootstrap-server "$BOOTSTRAP_SERVER" --command-config "$COMMAND_CONFIG_FILE" --list 2>/dev/null | sort
log "============================================"
log "Total topics: $($KAFKA_BIN --bootstrap-server "$BOOTSTRAP_SERVER" --command-config "$COMMAND_CONFIG_FILE" --list 2>/dev/null | wc -l)"
