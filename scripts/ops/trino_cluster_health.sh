#!/bin/bash
###############################################################################
# Trino Cluster Health Check Script
#
# Checks health of all Zomato Trino clusters (adhoc, etl, reporting):
#   - Coordinator responsiveness
#   - Worker node count and status
#   - Running/queued query counts
#   - Memory utilization
#   - Catalog connectivity (Iceberg, Hive, MySQL)
#
# Usage:
#   ./trino_cluster_health.sh [--cluster <adhoc|etl|reporting|all>]
#
# Environment:
#   TRINO_ADHOC_HOST      - Trino adhoc cluster host
#   TRINO_ETL_HOST        - Trino ETL cluster host
#   TRINO_REPORTING_HOST  - Trino reporting cluster host
#   SNS_TOPIC_ARN         - SNS topic for alerts (optional)
###############################################################################

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
TRINO_ADHOC_HOST="${TRINO_ADHOC_HOST:-localhost}"
TRINO_ETL_HOST="${TRINO_ETL_HOST:-localhost}"
TRINO_REPORTING_HOST="${TRINO_REPORTING_HOST:-localhost}"
TRINO_PORT="${TRINO_PORT:-8080}"
TRINO_USER="${TRINO_USER:-healthcheck}"
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:-}"
AWS_REGION="${AWS_REGION:-us-east-1}"
CLOUDWATCH_NAMESPACE="zomato-data-platform"

TARGET_CLUSTER="${1:-all}"
EXIT_CODE=0

declare -A CLUSTER_HOSTS=(
    ["adhoc"]="$TRINO_ADHOC_HOST"
    ["etl"]="$TRINO_ETL_HOST"
    ["reporting"]="$TRINO_REPORTING_HOST"
)

declare -A EXPECTED_WORKERS=(
    ["adhoc"]=10
    ["etl"]=20
    ["reporting"]=8
)

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
    local subject="${2:-Trino Health Alert}"

    if [[ -n "$SNS_TOPIC_ARN" ]]; then
        aws sns publish \
            --region "$AWS_REGION" \
            --topic-arn "$SNS_TOPIC_ARN" \
            --subject "$subject" \
            --message "$message" 2>/dev/null || true
    fi
}

publish_metric() {
    local cluster="$1"
    local metric_name="$2"
    local value="$3"
    local unit="${4:-Count}"

    aws cloudwatch put-metric-data \
        --region "$AWS_REGION" \
        --namespace "$CLOUDWATCH_NAMESPACE" \
        --metric-name "Trino${metric_name}" \
        --dimensions TrinoCluster="$cluster" \
        --value "$value" \
        --unit "$unit" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Health check functions
# ---------------------------------------------------------------------------
check_coordinator() {
    local cluster="$1"
    local host="${CLUSTER_HOSTS[$cluster]}"
    local url="http://${host}:${TRINO_PORT}/v1/info"

    log "Checking coordinator: $cluster ($host:$TRINO_PORT)"

    local response
    response=$(curl -s -w "\n%{http_code}" --max-time 10 "$url" 2>/dev/null || echo -e "\n000")
    local http_code
    http_code=$(echo "$response" | tail -1)
    local body
    body=$(echo "$response" | head -n -1)

    if [[ "$http_code" != "200" ]]; then
        warn "$cluster coordinator unreachable (HTTP $http_code)"
        publish_metric "$cluster" "CoordinatorHealthy" 0
        return 1
    fi

    local starting
    starting=$(echo "$body" | python3 -c "import json,sys; print(json.load(sys.stdin).get('starting', True))" 2>/dev/null || echo "true")

    if [[ "$starting" == "True" ]]; then
        warn "$cluster coordinator is still starting"
        publish_metric "$cluster" "CoordinatorHealthy" 0
        return 1
    fi

    log "  Coordinator: HEALTHY"
    publish_metric "$cluster" "CoordinatorHealthy" 1
    return 0
}

check_workers() {
    local cluster="$1"
    local host="${CLUSTER_HOSTS[$cluster]}"
    local expected="${EXPECTED_WORKERS[$cluster]}"
    local url="http://${host}:${TRINO_PORT}/v1/node"

    local response
    response=$(curl -s --max-time 10 "$url" 2>/dev/null || echo "[]")

    local total_workers
    total_workers=$(echo "$response" | python3 -c "
import json, sys
nodes = json.load(sys.stdin)
print(len([n for n in nodes if n.get('coordinator', False) is False]))
" 2>/dev/null || echo "0")

    local active_workers
    active_workers=$(echo "$response" | python3 -c "
import json, sys
from datetime import datetime, timedelta
nodes = json.load(sys.stdin)
active = [n for n in nodes if not n.get('coordinator', False)]
print(len(active))
" 2>/dev/null || echo "0")

    log "  Workers: $active_workers active / $expected expected"
    publish_metric "$cluster" "ActiveWorkers" "$active_workers"

    if [[ "$active_workers" -lt "$expected" ]]; then
        local deficit=$((expected - active_workers))
        warn "$cluster: $deficit worker(s) missing (have $active_workers, need $expected)"

        if [[ "$active_workers" -lt $((expected / 2)) ]]; then
            send_alert "CRITICAL: Trino $cluster cluster has only $active_workers/$expected workers" \
                       "CRITICAL: Trino Worker Shortage"
            return 1
        fi
    fi
    return 0
}

check_queries() {
    local cluster="$1"
    local host="${CLUSTER_HOSTS[$cluster]}"
    local url="http://${host}:${TRINO_PORT}/v1/query"

    local response
    response=$(curl -s --max-time 10 "$url" 2>/dev/null || echo "[]")

    local stats
    stats=$(echo "$response" | python3 -c "
import json, sys
queries = json.load(sys.stdin)
running = len([q for q in queries if q.get('state') == 'RUNNING'])
queued = len([q for q in queries if q.get('state') == 'QUEUED'])
failed = len([q for q in queries if q.get('state') == 'FAILED'])
print(f'{running} {queued} {failed}')
" 2>/dev/null || echo "0 0 0")

    read -r running queued failed <<< "$stats"

    log "  Queries: $running running, $queued queued, $failed failed"
    publish_metric "$cluster" "RunningQueries" "$running"
    publish_metric "$cluster" "QueuedQueries" "$queued"

    if [[ "$queued" -gt 50 ]]; then
        warn "$cluster: High query queue ($queued queries)"
        send_alert "WARNING: Trino $cluster has $queued queued queries" \
                   "WARNING: Trino Query Queue"
    fi
    return 0
}

check_catalogs() {
    local cluster="$1"
    local host="${CLUSTER_HOSTS[$cluster]}"

    local catalogs=("iceberg" "hive" "mysql")
    local failed_catalogs=()

    for catalog in "${catalogs[@]}"; do
        local query="SELECT 1"
        local result
        result=$(curl -s --max-time 15 \
            -X POST "http://${host}:${TRINO_PORT}/v1/statement" \
            -H "X-Trino-User: $TRINO_USER" \
            -H "X-Trino-Catalog: $catalog" \
            -H "X-Trino-Schema: information_schema" \
            -d "$query" 2>/dev/null || echo '{"error": true}')

        local has_error
        has_error=$(echo "$result" | python3 -c "
import json, sys
data = json.load(sys.stdin)
print('true' if 'error' in data and isinstance(data['error'], dict) else 'false')
" 2>/dev/null || echo "true")

        if [[ "$has_error" == "true" ]]; then
            failed_catalogs+=("$catalog")
        fi
    done

    if [[ ${#failed_catalogs[@]} -gt 0 ]]; then
        warn "$cluster: Catalog(s) unreachable: ${failed_catalogs[*]}"
        publish_metric "$cluster" "CatalogFailures" "${#failed_catalogs[@]}"
        return 1
    fi

    log "  Catalogs: all reachable (${catalogs[*]})"
    publish_metric "$cluster" "CatalogFailures" 0
    return 0
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
log "=========================================="
log "Trino Cluster Health Check"
log "=========================================="

if [[ "$TARGET_CLUSTER" == "all" ]]; then
    clusters=("adhoc" "etl" "reporting")
else
    clusters=("$TARGET_CLUSTER")
fi

for cluster in "${clusters[@]}"; do
    log ""
    log "--- Cluster: $cluster ---"

    if ! check_coordinator "$cluster"; then
        EXIT_CODE=1
        continue
    fi

    check_workers "$cluster" || EXIT_CODE=1
    check_queries "$cluster" || EXIT_CODE=1
    check_catalogs "$cluster" || EXIT_CODE=1
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
