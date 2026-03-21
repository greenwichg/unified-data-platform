#!/bin/bash
###############################################################################
# Kafka Cluster Rebalance Script
#
# Generates and executes a partition reassignment plan to balance topic
# partitions across brokers. Supports:
#   - Full cluster rebalance
#   - Single-topic rebalance
#   - Dry-run mode
#   - Throttled reassignment to limit replication bandwidth
#
# Usage:
#   ./kafka_rebalance.sh [--dry-run] [--topic <name>] [--throttle <bytes/s>]
#
# Environment:
#   KAFKA_BOOTSTRAP    - Kafka bootstrap servers (required)
#   KAFKA_HOME         - Kafka installation directory
#   ZOOKEEPER_CONNECT  - ZooKeeper connection string
###############################################################################

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
ZOOKEEPER_CONNECT="${ZOOKEEPER_CONNECT:-localhost:2181}"
THROTTLE_BYTES="${THROTTLE_BYTES:-52428800}"  # 50 MB/s default
DRY_RUN=false
TARGET_TOPIC=""
WORK_DIR="/tmp/kafka-rebalance-$(date +%Y%m%d_%H%M%S)"

# Zomato platform topics
PLATFORM_TOPICS=(
    "orders"
    "users"
    "menu"
    "promo"
    "topics"
    "druid-ingestion-events"
    "order-spike-alerts"
)

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --topic)
            TARGET_TOPIC="$2"
            shift 2
            ;;
        --throttle)
            THROTTLE_BYTES="$2"
            shift 2
            ;;
        --bootstrap)
            KAFKA_BOOTSTRAP="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--dry-run] [--topic <name>] [--throttle <bytes/s>]"
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
log() {
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] $*"
}

cleanup() {
    if [[ -d "$WORK_DIR" ]]; then
        log "Cleaning up work directory: $WORK_DIR"
        rm -rf "$WORK_DIR"
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
log "Kafka Rebalance Script"
log "Bootstrap: $KAFKA_BOOTSTRAP"
log "Dry-run:   $DRY_RUN"
log "Throttle:  $THROTTLE_BYTES bytes/s"

mkdir -p "$WORK_DIR"

# Get broker list
log "Fetching broker list..."
BROKER_IDS=$("$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP" 2>/dev/null | \
    grep -oP '^\S+:\d+' | sort -u | wc -l)

log "Active brokers: $BROKER_IDS"

if [[ "$BROKER_IDS" -lt 2 ]]; then
    log "ERROR: Need at least 2 brokers for rebalance (found $BROKER_IDS)"
    exit 1
fi

# ---------------------------------------------------------------------------
# Build topics list
# ---------------------------------------------------------------------------
if [[ -n "$TARGET_TOPIC" ]]; then
    TOPICS_JSON="{\"topics\": [{\"topic\": \"$TARGET_TOPIC\"}], \"version\": 1}"
else
    TOPICS_ENTRIES=""
    for topic in "${PLATFORM_TOPICS[@]}"; do
        if [[ -n "$TOPICS_ENTRIES" ]]; then
            TOPICS_ENTRIES="$TOPICS_ENTRIES,"
        fi
        TOPICS_ENTRIES="$TOPICS_ENTRIES{\"topic\": \"$topic\"}"
    done
    TOPICS_JSON="{\"topics\": [$TOPICS_ENTRIES], \"version\": 1}"
fi

echo "$TOPICS_JSON" > "$WORK_DIR/topics-to-move.json"
log "Topics file: $WORK_DIR/topics-to-move.json"

# ---------------------------------------------------------------------------
# Generate reassignment plan
# ---------------------------------------------------------------------------
log "Generating reassignment plan..."

"$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topics-to-move-json-file "$WORK_DIR/topics-to-move.json" \
    --broker-list "$(seq -s, 0 $((BROKER_IDS - 1)))" \
    --generate \
    > "$WORK_DIR/reassignment-output.txt" 2>&1

# Extract the proposed plan (second JSON block in output)
grep -A9999 "Proposed partition reassignment" "$WORK_DIR/reassignment-output.txt" | \
    tail -n +2 | head -1 > "$WORK_DIR/reassignment-plan.json"

# Save current assignment for rollback
grep -A9999 "Current partition replica assignment" "$WORK_DIR/reassignment-output.txt" | \
    tail -n +2 | head -1 > "$WORK_DIR/rollback-plan.json"

log "Reassignment plan saved to: $WORK_DIR/reassignment-plan.json"
log "Rollback plan saved to: $WORK_DIR/rollback-plan.json"

# ---------------------------------------------------------------------------
# Show plan summary
# ---------------------------------------------------------------------------
PARTITION_COUNT=$(python3 -c "
import json, sys
with open('$WORK_DIR/reassignment-plan.json') as f:
    plan = json.load(f)
print(len(plan.get('partitions', [])))
" 2>/dev/null || echo "unknown")

log "Partitions to reassign: $PARTITION_COUNT"

# ---------------------------------------------------------------------------
# Execute or show dry-run
# ---------------------------------------------------------------------------
if [[ "$DRY_RUN" == "true" ]]; then
    log "DRY RUN - no changes will be applied"
    log "Proposed reassignment plan:"
    python3 -c "
import json
with open('$WORK_DIR/reassignment-plan.json') as f:
    plan = json.load(f)
for p in plan.get('partitions', [])[:10]:
    print(f\"  {p['topic']}-{p['partition']}: {p['replicas']}\")
remaining = len(plan.get('partitions', [])) - 10
if remaining > 0:
    print(f'  ... and {remaining} more partitions')
" 2>/dev/null || cat "$WORK_DIR/reassignment-plan.json"
    log "To execute, run without --dry-run"
    exit 0
fi

log "Executing reassignment with throttle: $THROTTLE_BYTES bytes/s"

"$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --reassignment-json-file "$WORK_DIR/reassignment-plan.json" \
    --execute \
    --throttle "$THROTTLE_BYTES"

log "Reassignment initiated. Monitoring progress..."

# ---------------------------------------------------------------------------
# Monitor progress
# ---------------------------------------------------------------------------
MAX_WAIT=3600  # 1 hour max
ELAPSED=0
POLL_INTERVAL=30

while [[ $ELAPSED -lt $MAX_WAIT ]]; do
    sleep $POLL_INTERVAL
    ELAPSED=$((ELAPSED + POLL_INTERVAL))

    STATUS=$("$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
        --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --reassignment-json-file "$WORK_DIR/reassignment-plan.json" \
        --verify 2>&1)

    COMPLETED=$(echo "$STATUS" | grep -c "completed successfully" || true)
    IN_PROGRESS=$(echo "$STATUS" | grep -c "still in progress" || true)

    log "Progress: $COMPLETED completed, $IN_PROGRESS in progress (${ELAPSED}s elapsed)"

    if [[ "$IN_PROGRESS" -eq 0 ]]; then
        log "All reassignments completed!"
        break
    fi
done

# Remove throttle
log "Removing replication throttle..."
"$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --reassignment-json-file "$WORK_DIR/reassignment-plan.json" \
    --verify 2>/dev/null || true

log "=========================================="
log "Kafka Rebalance Complete"
log "  Partitions moved: $PARTITION_COUNT"
log "  Duration:         ${ELAPSED}s"
log "  Rollback file:    $WORK_DIR/rollback-plan.json"
log "=========================================="
