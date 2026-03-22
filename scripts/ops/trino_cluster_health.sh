#!/bin/bash
###############################################################################
# Athena Workgroup Health Check Script
#
# MIGRATION NOTE: This script replaces the former Trino cluster health check.
# Trino on ECS has been replaced by Amazon Athena (serverless).
# Health checks now use AWS CLI to query Athena workgroup status and
# CloudWatch metrics.
#
# Checks health of all Zomato Athena workgroups (adhoc, etl, reporting):
#   - Workgroup status and configuration
#   - Running/queued query counts (via CloudWatch)
#   - Query failure rates
#   - Data scanned metrics
#
# Usage:
#   ./trino_cluster_health.sh [--workgroup <adhoc|etl|reporting|all>]
#
# Environment:
#   AWS_REGION            - AWS region (default: us-east-1)
#   SNS_TOPIC_ARN         - SNS topic for alerts (optional)
###############################################################################

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AWS_REGION="${AWS_REGION:-us-east-1}"
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:-}"
CLOUDWATCH_NAMESPACE="AWS/Athena"
CUSTOM_NAMESPACE="zomato-data-platform"

TARGET_WORKGROUP="${1:-all}"
EXIT_CODE=0

WORKGROUPS=("zomato-adhoc" "zomato-etl" "zomato-reporting")

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
    local subject="${2:-Athena Health Alert}"

    if [[ -n "$SNS_TOPIC_ARN" ]]; then
        aws sns publish \
            --region "$AWS_REGION" \
            --topic-arn "$SNS_TOPIC_ARN" \
            --subject "$subject" \
            --message "$message" 2>/dev/null || true
    fi
}

publish_metric() {
    local workgroup="$1"
    local metric_name="$2"
    local value="$3"
    local unit="${4:-Count}"

    aws cloudwatch put-metric-data \
        --region "$AWS_REGION" \
        --namespace "$CUSTOM_NAMESPACE" \
        --metric-name "Athena${metric_name}" \
        --dimensions AthenaWorkgroup="$workgroup" \
        --value "$value" \
        --unit "$unit" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Health check functions
# ---------------------------------------------------------------------------
check_workgroup_status() {
    local workgroup="$1"

    log "Checking workgroup status: $workgroup"

    local status
    status=$(aws athena get-work-group \
        --region "$AWS_REGION" \
        --work-group "$workgroup" \
        --query 'WorkGroup.State' \
        --output text 2>/dev/null || echo "UNKNOWN")

    if [[ "$status" != "ENABLED" ]]; then
        warn "$workgroup workgroup is not ENABLED (status: $status)"
        publish_metric "$workgroup" "WorkgroupHealthy" 0
        return 1
    fi

    log "  Workgroup Status: ENABLED"
    publish_metric "$workgroup" "WorkgroupHealthy" 1
    return 0
}

check_running_queries() {
    local workgroup="$1"

    # List running queries via Athena API
    local running_count
    running_count=$(aws athena list-query-executions \
        --region "$AWS_REGION" \
        --work-group "$workgroup" \
        --max-results 50 \
        --query 'QueryExecutionIds' \
        --output text 2>/dev/null | wc -w || echo "0")

    log "  Recent query executions (last 50 window): $running_count"
    publish_metric "$workgroup" "RecentQueries" "$running_count"

    return 0
}

check_cloudwatch_metrics() {
    local workgroup="$1"
    local end_time
    end_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    local start_time
    start_time=$(date -u -d '15 minutes ago' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-15M +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo "$end_time")

    # Check total query count from CloudWatch
    local query_count
    query_count=$(aws cloudwatch get-metric-statistics \
        --region "$AWS_REGION" \
        --namespace "$CLOUDWATCH_NAMESPACE" \
        --metric-name "TotalExecutionTime" \
        --dimensions Name=WorkGroup,Value="$workgroup" \
        --start-time "$start_time" \
        --end-time "$end_time" \
        --period 900 \
        --statistics SampleCount \
        --query 'Datapoints[0].SampleCount' \
        --output text 2>/dev/null || echo "0")

    log "  Queries (last 15m): ${query_count:-0}"

    # Check data scanned
    local data_scanned
    data_scanned=$(aws cloudwatch get-metric-statistics \
        --region "$AWS_REGION" \
        --namespace "$CLOUDWATCH_NAMESPACE" \
        --metric-name "ProcessedBytes" \
        --dimensions Name=WorkGroup,Value="$workgroup" \
        --start-time "$start_time" \
        --end-time "$end_time" \
        --period 900 \
        --statistics Sum \
        --query 'Datapoints[0].Sum' \
        --output text 2>/dev/null || echo "0")

    local data_scanned_gb
    data_scanned_gb=$(python3 -c "print(f'{${data_scanned:-0} / (1024**3):.2f}')" 2>/dev/null || echo "0")
    log "  Data scanned (last 15m): ${data_scanned_gb} GB"

    publish_metric "$workgroup" "DataScannedGB" "$data_scanned_gb" "Gigabytes"

    return 0
}

check_failed_queries() {
    local workgroup="$1"

    # Sample recent query executions and check for failures
    local query_ids
    query_ids=$(aws athena list-query-executions \
        --region "$AWS_REGION" \
        --work-group "$workgroup" \
        --max-results 20 \
        --query 'QueryExecutionIds' \
        --output text 2>/dev/null || echo "")

    if [[ -z "$query_ids" ]]; then
        log "  No recent queries to check for failures"
        return 0
    fi

    local failed_count=0
    local total_count=0

    for qid in $query_ids; do
        total_count=$((total_count + 1))
        local state
        state=$(aws athena get-query-execution \
            --region "$AWS_REGION" \
            --query-execution-id "$qid" \
            --query 'QueryExecution.Status.State' \
            --output text 2>/dev/null || echo "UNKNOWN")

        if [[ "$state" == "FAILED" ]]; then
            failed_count=$((failed_count + 1))
        fi
    done

    log "  Recent query status: $failed_count failed out of $total_count sampled"
    publish_metric "$workgroup" "FailedQueries" "$failed_count"

    if [[ "$total_count" -gt 0 ]]; then
        local failure_pct=$((failed_count * 100 / total_count))
        if [[ "$failure_pct" -gt 30 ]]; then
            warn "$workgroup: High query failure rate ($failure_pct%)"
            send_alert "CRITICAL: Athena workgroup $workgroup has $failure_pct% query failure rate ($failed_count/$total_count)" \
                       "CRITICAL: Athena Query Failures"
            return 1
        fi
    fi

    return 0
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
log "=========================================="
log "Athena Workgroup Health Check"
log "(Replaces former Trino Cluster Health Check)"
log "=========================================="

if [[ "$TARGET_WORKGROUP" == "all" ]]; then
    workgroups=("${WORKGROUPS[@]}")
elif [[ "$TARGET_WORKGROUP" == "--workgroup" ]]; then
    # Handle --workgroup flag
    workgroups=("zomato-${2:-all}")
    if [[ "${2:-all}" == "all" ]]; then
        workgroups=("${WORKGROUPS[@]}")
    fi
else
    workgroups=("zomato-$TARGET_WORKGROUP")
fi

for workgroup in "${workgroups[@]}"; do
    log ""
    log "--- Workgroup: $workgroup ---"

    if ! check_workgroup_status "$workgroup"; then
        EXIT_CODE=1
        continue
    fi

    check_running_queries "$workgroup" || EXIT_CODE=1
    check_cloudwatch_metrics "$workgroup" || EXIT_CODE=1
    check_failed_queries "$workgroup" || EXIT_CODE=1
done

log ""
log "=========================================="
if [[ $EXIT_CODE -eq 0 ]]; then
    log "Overall Status: HEALTHY"
else
    log "Overall Status: DEGRADED"
fi
log "=========================================="

exit $EXIT_CODE
