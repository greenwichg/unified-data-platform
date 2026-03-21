#!/usr/bin/env bash
# ============================================================================
# Zomato Data Platform - Kafka Partition Reassignment Script
#
# Generates and executes partition reassignment plans for Kafka topics.
# Used during broker additions, decommissions, or rebalancing.
#
# Usage:
#   ./reassign-partitions.sh generate --topics orders,users --brokers 1,2,3,4,5,6
#   ./reassign-partitions.sh execute  --plan /tmp/reassignment-plan.json
#   ./reassign-partitions.sh verify   --plan /tmp/reassignment-plan.json
#   ./reassign-partitions.sh status
#
# Prerequisites:
#   - kafka-reassign-partitions.sh in PATH (from Kafka installation)
#   - KAFKA_BOOTSTRAP_SERVERS and KAFKA_ZOOKEEPER set or passed as args
# ============================================================================

set -euo pipefail

# ----- Configuration -----
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka1.zomato-data.internal:9092,kafka2.zomato-data.internal:9092,kafka3.zomato-data.internal:9092}"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
WORK_DIR="/tmp/kafka-reassignment"
THROTTLE_BYTES="${THROTTLE_BYTES:-104857600}"  # 100 MB/s default throttle
LOG_FILE="${WORK_DIR}/reassignment-$(date +%Y%m%d-%H%M%S).log"

# ----- Colors -----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"; }
log_error() { echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"; }

mkdir -p "$WORK_DIR"

usage() {
    cat <<EOF
Usage: $0 <command> [options]

Commands:
  generate    Generate a reassignment plan
  execute     Execute a reassignment plan
  verify      Verify reassignment completion
  status      Show current partition distribution

Options:
  --topics TOPICS           Comma-separated list of topics (for generate)
  --brokers BROKER_IDS      Comma-separated list of target broker IDs (for generate)
  --plan PLAN_FILE          Path to reassignment plan JSON (for execute/verify)
  --throttle BYTES_PER_SEC  Throttle rate in bytes/sec (default: 100MB/s)
  --bootstrap SERVERS       Kafka bootstrap servers
  --dry-run                 Generate plan without executing

Examples:
  $0 generate --topics orders,users,menu,promo --brokers 1,2,3,4,5,6
  $0 execute --plan /tmp/kafka-reassignment/reassignment-plan.json --throttle 209715200
  $0 verify --plan /tmp/kafka-reassignment/reassignment-plan.json
  $0 status --topics orders
EOF
    exit 1
}

# ----- Generate Reassignment Plan -----
generate_plan() {
    local topics="$1"
    local brokers="$2"
    local dry_run="${3:-false}"

    log_info "Generating reassignment plan for topics: $topics -> brokers: $brokers"

    # Create topics-to-move JSON
    local topics_json="${WORK_DIR}/topics-to-move.json"
    local plan_file="${WORK_DIR}/reassignment-plan.json"
    local rollback_file="${WORK_DIR}/rollback-plan.json"

    # Build JSON array of topics
    local topic_entries=""
    IFS=',' read -ra TOPIC_ARRAY <<< "$topics"
    for topic in "${TOPIC_ARRAY[@]}"; do
        if [ -n "$topic_entries" ]; then
            topic_entries="${topic_entries},"
        fi
        topic_entries="${topic_entries}{\"topic\":\"${topic}\"}"
    done

    cat > "$topics_json" <<EOJSON
{"topics": [${topic_entries}], "version": 1}
EOJSON

    log_info "Topics JSON written to $topics_json"

    # Generate the plan
    "${KAFKA_HOME}/bin/kafka-reassign-partitions.sh" \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
        --topics-to-move-json-file "$topics_json" \
        --broker-list "$brokers" \
        --generate \
        2>&1 | tee -a "$LOG_FILE" | while IFS= read -r line; do
            if echo "$line" | grep -q "Proposed partition reassignment"; then
                # Next line will be the plan JSON
                read -r plan_json
                echo "$plan_json" | python3 -m json.tool > "$plan_file"
                log_info "Reassignment plan saved to $plan_file"
            elif echo "$line" | grep -q "Current partition replica assignment"; then
                read -r rollback_json
                echo "$rollback_json" | python3 -m json.tool > "$rollback_file"
                log_info "Rollback plan saved to $rollback_file"
            fi
        done

    if [ -f "$plan_file" ]; then
        local partition_count
        partition_count=$(python3 -c "import json; print(len(json.load(open('$plan_file'))['partitions']))" 2>/dev/null || echo "unknown")
        log_info "Plan covers $partition_count partition reassignments"

        if [ "$dry_run" = "true" ]; then
            log_info "Dry run - plan generated but not executed"
            log_info "Review plan: cat $plan_file"
            log_info "Execute with: $0 execute --plan $plan_file"
        fi
    else
        log_error "Failed to generate reassignment plan"
        exit 1
    fi
}

# ----- Execute Reassignment Plan -----
execute_plan() {
    local plan_file="$1"
    local throttle="$2"

    if [ ! -f "$plan_file" ]; then
        log_error "Plan file not found: $plan_file"
        exit 1
    fi

    local partition_count
    partition_count=$(python3 -c "import json; print(len(json.load(open('$plan_file'))['partitions']))" 2>/dev/null || echo "unknown")

    log_info "Executing reassignment plan: $plan_file ($partition_count partitions)"
    log_info "Throttle: $(numfmt --to=iec-i "$throttle" 2>/dev/null || echo "$throttle")B/s"
    log_warn "This operation will move data between brokers. Ensure sufficient disk space and network bandwidth."

    read -r -p "Continue? [y/N] " confirm
    if [[ ! "$confirm" =~ ^[yY]$ ]]; then
        log_info "Aborted by user"
        exit 0
    fi

    "${KAFKA_HOME}/bin/kafka-reassign-partitions.sh" \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
        --reassignment-json-file "$plan_file" \
        --throttle "$throttle" \
        --execute \
        2>&1 | tee -a "$LOG_FILE"

    log_info "Reassignment started. Monitor with: $0 verify --plan $plan_file"
}

# ----- Verify Reassignment -----
verify_plan() {
    local plan_file="$1"

    if [ ! -f "$plan_file" ]; then
        log_error "Plan file not found: $plan_file"
        exit 1
    fi

    log_info "Verifying reassignment status for plan: $plan_file"

    "${KAFKA_HOME}/bin/kafka-reassign-partitions.sh" \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
        --reassignment-json-file "$plan_file" \
        --verify \
        2>&1 | tee -a "$LOG_FILE"

    # Check if all partitions are complete
    local output
    output=$("${KAFKA_HOME}/bin/kafka-reassign-partitions.sh" \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
        --reassignment-json-file "$plan_file" \
        --verify 2>&1)

    if echo "$output" | grep -q "is still in progress"; then
        log_warn "Some partitions are still being reassigned"
        log_info "Re-run this command to check progress"
    else
        log_info "All partition reassignments complete"
        # Remove throttle
        "${KAFKA_HOME}/bin/kafka-reassign-partitions.sh" \
            --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
            --reassignment-json-file "$plan_file" \
            --verify \
            2>&1 | tee -a "$LOG_FILE"
        log_info "Throttle removed"
    fi
}

# ----- Show Partition Status -----
show_status() {
    local topics="${1:-}"

    log_info "Current partition distribution"
    echo ""

    if [ -n "$topics" ]; then
        IFS=',' read -ra TOPIC_ARRAY <<< "$topics"
        for topic in "${TOPIC_ARRAY[@]}"; do
            echo "=== Topic: $topic ==="
            "${KAFKA_HOME}/bin/kafka-topics.sh" \
                --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
                --describe \
                --topic "$topic" \
                2>/dev/null | head -5
            echo ""

            # Summarize replicas per broker
            "${KAFKA_HOME}/bin/kafka-topics.sh" \
                --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
                --describe \
                --topic "$topic" \
                2>/dev/null | tail -n +2 | awk '{print $8}' | tr ',' '\n' | sort | uniq -c | sort -rn | \
                while read -r count broker; do
                    echo "  Broker $broker: $count replicas"
                done
            echo ""
        done
    else
        # Show summary of all topics
        "${KAFKA_HOME}/bin/kafka-topics.sh" \
            --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
            --describe \
            2>/dev/null | grep -E "^Topic:" | awk '{print $2, $4, $6}' | column -t
    fi
}

# ----- Main -----
COMMAND="${1:-}"
shift || true

TOPICS=""
BROKERS=""
PLAN_FILE=""
DRY_RUN="false"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --topics)     TOPICS="$2"; shift 2 ;;
        --brokers)    BROKERS="$2"; shift 2 ;;
        --plan)       PLAN_FILE="$2"; shift 2 ;;
        --throttle)   THROTTLE_BYTES="$2"; shift 2 ;;
        --bootstrap)  KAFKA_BOOTSTRAP_SERVERS="$2"; shift 2 ;;
        --dry-run)    DRY_RUN="true"; shift ;;
        *)            log_error "Unknown option: $1"; usage ;;
    esac
done

case "$COMMAND" in
    generate)
        [ -z "$TOPICS" ] && { log_error "--topics required for generate"; usage; }
        [ -z "$BROKERS" ] && { log_error "--brokers required for generate"; usage; }
        generate_plan "$TOPICS" "$BROKERS" "$DRY_RUN"
        ;;
    execute)
        [ -z "$PLAN_FILE" ] && { log_error "--plan required for execute"; usage; }
        execute_plan "$PLAN_FILE" "$THROTTLE_BYTES"
        ;;
    verify)
        [ -z "$PLAN_FILE" ] && { log_error "--plan required for verify"; usage; }
        verify_plan "$PLAN_FILE"
        ;;
    status)
        show_status "$TOPICS"
        ;;
    *)
        usage
        ;;
esac
