#!/bin/bash
###############################################################################
# Apache Druid Compaction Trigger Script
#
# Triggers segment compaction for Zomato Druid datasources to:
#   - Merge small segments from real-time ingestion into optimal sizes
#   - Enforce target segment size (500MB-700MB)
#   - Clean up old segment versions after compaction
#   - Publish compaction metrics to CloudWatch
#
# Usage:
#   ./druid_compaction_trigger.sh [--datasource <name>] [--dry-run]
#
# Environment:
#   DRUID_COORDINATOR_URL  - Druid coordinator URL
#   DRUID_ROUTER_URL       - Druid router URL (for SQL queries)
#   SNS_TOPIC_ARN          - SNS topic for alerts (optional)
###############################################################################

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DRUID_COORDINATOR_URL="${DRUID_COORDINATOR_URL:-http://localhost:8081}"
DRUID_ROUTER_URL="${DRUID_ROUTER_URL:-http://localhost:8888}"
AWS_REGION="${AWS_REGION:-us-east-1}"
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:-}"
CLOUDWATCH_NAMESPACE="zomato-data-platform"

# Compaction settings
TARGET_SEGMENT_SIZE=$((500 * 1024 * 1024))  # 500 MB
MAX_ROWS_PER_SEGMENT=5000000
SKIP_OFFSET_FROM_LATEST="PT1H"  # Don't compact the last hour
MAX_CONCURRENT_TASKS=4

DRY_RUN=false
TARGET_DATASOURCE=""

# Zomato datasources
DATASOURCES=(
    "zomato_realtime_events"
)

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --datasource)
            TARGET_DATASOURCE="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --coordinator-url)
            DRUID_COORDINATOR_URL="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
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

warn() {
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] WARNING: $*" >&2
}

send_alert() {
    local message="$1"
    if [[ -n "$SNS_TOPIC_ARN" ]]; then
        aws sns publish \
            --region "$AWS_REGION" \
            --topic-arn "$SNS_TOPIC_ARN" \
            --subject "Druid Compaction Alert" \
            --message "$message" 2>/dev/null || true
    fi
}

publish_metric() {
    local metric_name="$1"
    local value="$2"
    local datasource="${3:-all}"

    aws cloudwatch put-metric-data \
        --region "$AWS_REGION" \
        --namespace "$CLOUDWATCH_NAMESPACE" \
        --metric-name "Druid${metric_name}" \
        --dimensions DataSource="$datasource" \
        --value "$value" \
        --unit Count 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
log "Druid Compaction Trigger"
log "Coordinator: $DRUID_COORDINATOR_URL"
log "Dry-run: $DRY_RUN"

# Verify coordinator is healthy
COORD_STATUS=$(curl -s --max-time 10 \
    "${DRUID_COORDINATOR_URL}/status/health" 2>/dev/null || echo "false")

if [[ "$COORD_STATUS" != "true" ]]; then
    log "ERROR: Druid coordinator is not healthy"
    send_alert "Druid coordinator unhealthy at $DRUID_COORDINATOR_URL"
    exit 1
fi

log "Coordinator: HEALTHY"

# ---------------------------------------------------------------------------
# Check current compaction status
# ---------------------------------------------------------------------------
check_compaction_status() {
    local datasource="$1"

    local running_tasks
    running_tasks=$(curl -s --max-time 10 \
        "${DRUID_COORDINATOR_URL}/druid/indexer/v1/runningTasks" 2>/dev/null | \
        python3 -c "
import json, sys
tasks = json.load(sys.stdin)
compaction = [t for t in tasks if t.get('type') == 'compact' and '$datasource' in t.get('dataSource', '')]
print(len(compaction))
" 2>/dev/null || echo "0")

    log "  Running compaction tasks for $datasource: $running_tasks"
    return "$running_tasks"
}

# ---------------------------------------------------------------------------
# Get segment statistics
# ---------------------------------------------------------------------------
get_segment_stats() {
    local datasource="$1"

    local stats
    stats=$(curl -s --max-time 10 \
        "${DRUID_COORDINATOR_URL}/druid/coordinator/v1/datasources/${datasource}" 2>/dev/null || echo "{}")

    local segment_count
    segment_count=$(echo "$stats" | python3 -c "
import json, sys
data = json.load(sys.stdin)
segments = data.get('segments', {})
print(segments.get('count', 0))
" 2>/dev/null || echo "0")

    local total_size
    total_size=$(echo "$stats" | python3 -c "
import json, sys
data = json.load(sys.stdin)
segments = data.get('segments', {})
print(segments.get('size', 0))
" 2>/dev/null || echo "0")

    log "  Segments: $segment_count (total size: $((total_size / 1024 / 1024)) MB)"
    publish_metric "SegmentCount" "$segment_count" "$datasource"
}

# ---------------------------------------------------------------------------
# Submit compaction task
# ---------------------------------------------------------------------------
submit_compaction() {
    local datasource="$1"

    local compaction_spec
    compaction_spec=$(cat <<EOF
{
  "type": "compact",
  "dataSource": "${datasource}",
  "ioConfig": {
    "type": "compact",
    "inputSpec": {
      "type": "interval",
      "interval": "2000-01-01/3000-01-01"
    }
  },
  "tuningConfig": {
    "type": "index_parallel",
    "maxRowsPerSegment": ${MAX_ROWS_PER_SEGMENT},
    "targetPartitionSize": ${TARGET_SEGMENT_SIZE},
    "maxNumConcurrentSubTasks": ${MAX_CONCURRENT_TASKS},
    "totalNumMergeTasks": 1,
    "forceGuaranteedRollup": true,
    "partitionsSpec": {
      "type": "hashed",
      "targetRowsPerSegment": ${MAX_ROWS_PER_SEGMENT}
    }
  },
  "granularitySpec": {
    "segmentGranularity": "HOUR",
    "queryGranularity": "MINUTE"
  },
  "skipOffsetFromLatest": "${SKIP_OFFSET_FROM_LATEST}"
}
EOF
)

    if [[ "$DRY_RUN" == "true" ]]; then
        log "  DRY RUN: Would submit compaction for $datasource"
        log "  Spec: $(echo "$compaction_spec" | python3 -m json.tool 2>/dev/null || echo "$compaction_spec")"
        return 0
    fi

    log "  Submitting compaction task for $datasource..."

    local response
    response=$(curl -s --max-time 30 \
        -X POST \
        -H "Content-Type: application/json" \
        -d "$compaction_spec" \
        "${DRUID_COORDINATOR_URL}/druid/indexer/v1/task" 2>/dev/null || echo '{"error": "request failed"}')

    local task_id
    task_id=$(echo "$response" | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(data.get('task', 'UNKNOWN'))
" 2>/dev/null || echo "UNKNOWN")

    if [[ "$task_id" == "UNKNOWN" ]]; then
        warn "Failed to submit compaction for $datasource: $response"
        publish_metric "CompactionFailure" 1 "$datasource"
        return 1
    fi

    log "  Compaction task submitted: $task_id"
    publish_metric "CompactionSubmitted" 1 "$datasource"
    return 0
}

# ---------------------------------------------------------------------------
# Auto-compaction configuration
# ---------------------------------------------------------------------------
configure_auto_compaction() {
    local datasource="$1"

    local config
    config=$(cat <<EOF
{
  "dataSource": "${datasource}",
  "taskPriority": 25,
  "inputSegmentSizeBytes": ${TARGET_SEGMENT_SIZE},
  "skipOffsetFromLatest": "${SKIP_OFFSET_FROM_LATEST}",
  "tuningConfig": {
    "type": "index_parallel",
    "maxRowsPerSegment": ${MAX_ROWS_PER_SEGMENT},
    "maxNumConcurrentSubTasks": ${MAX_CONCURRENT_TASKS}
  },
  "granularitySpec": {
    "segmentGranularity": "HOUR"
  }
}
EOF
)

    if [[ "$DRY_RUN" == "true" ]]; then
        log "  DRY RUN: Would configure auto-compaction for $datasource"
        return 0
    fi

    curl -s --max-time 15 \
        -X POST \
        -H "Content-Type: application/json" \
        -d "$config" \
        "${DRUID_COORDINATOR_URL}/druid/coordinator/v1/config/compaction" \
        >/dev/null 2>&1

    log "  Auto-compaction configured for $datasource"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
log "=========================================="

if [[ -n "$TARGET_DATASOURCE" ]]; then
    ds_list=("$TARGET_DATASOURCE")
else
    ds_list=("${DATASOURCES[@]}")
fi

TOTAL_SUBMITTED=0
TOTAL_FAILED=0

for ds in "${ds_list[@]}"; do
    log ""
    log "--- Datasource: $ds ---"

    get_segment_stats "$ds"

    # Check if compaction is already running
    if check_compaction_status "$ds"; then
        running=$?
        if [[ "$running" -gt 0 ]]; then
            log "  Skipping: compaction already in progress"
            continue
        fi
    fi

    # Configure auto-compaction
    configure_auto_compaction "$ds"

    # Submit manual compaction
    if submit_compaction "$ds"; then
        TOTAL_SUBMITTED=$((TOTAL_SUBMITTED + 1))
    else
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
    fi
done

log ""
log "=========================================="
log "Compaction Summary"
log "  Submitted: $TOTAL_SUBMITTED"
log "  Failed:    $TOTAL_FAILED"
log "=========================================="

if [[ $TOTAL_FAILED -gt 0 ]]; then
    send_alert "Druid compaction: $TOTAL_FAILED datasource(s) failed to compact"
    exit 1
fi
