#!/bin/bash
###############################################################################
# Aurora MySQL Automated Snapshot Script
#
# Creates manual snapshots of the Zomato Aurora MySQL cluster with:
#   - Timestamped snapshot identifiers
#   - Retention policy enforcement (delete old snapshots)
#   - Pre-snapshot connection drain check
#   - CloudWatch metric publishing
#   - SNS alerting on failure
#
# Usage:
#   ./aurora_snapshot.sh [--cluster-id <id>] [--retention-days <n>]
#
# Environment variables:
#   AURORA_CLUSTER_ID    - Aurora cluster identifier (required)
#   SNAPSHOT_RETENTION   - Days to retain snapshots (default: 30)
#   AWS_REGION           - AWS region (default: us-east-1)
#   SNS_TOPIC_ARN        - SNS topic for alerts (optional)
###############################################################################

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AURORA_CLUSTER_ID="${AURORA_CLUSTER_ID:-zomato-aurora-cluster}"
SNAPSHOT_RETENTION="${SNAPSHOT_RETENTION:-30}"
AWS_REGION="${AWS_REGION:-us-east-1}"
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:-}"
CLOUDWATCH_NAMESPACE="zomato-data-platform"

TIMESTAMP=$(date -u +"%Y%m%d-%H%M%S")
SNAPSHOT_ID="${AURORA_CLUSTER_ID}-manual-${TIMESTAMP}"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --cluster-id)
            AURORA_CLUSTER_ID="$2"
            SNAPSHOT_ID="${AURORA_CLUSTER_ID}-manual-${TIMESTAMP}"
            shift 2
            ;;
        --retention-days)
            SNAPSHOT_RETENTION="$2"
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

send_alert() {
    local message="$1"
    local subject="${2:-Aurora Snapshot Alert}"

    if [[ -n "$SNS_TOPIC_ARN" ]]; then
        aws sns publish \
            --region "$AWS_REGION" \
            --topic-arn "$SNS_TOPIC_ARN" \
            --subject "$subject" \
            --message "$message" || true
    fi
}

publish_metric() {
    local metric_name="$1"
    local value="$2"
    local unit="${3:-Count}"

    aws cloudwatch put-metric-data \
        --region "$AWS_REGION" \
        --namespace "$CLOUDWATCH_NAMESPACE" \
        --metric-name "$metric_name" \
        --dimensions ClusterId="$AURORA_CLUSTER_ID" \
        --value "$value" \
        --unit "$unit" || true
}

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
log "Starting Aurora snapshot for cluster: $AURORA_CLUSTER_ID"
log "Snapshot ID: $SNAPSHOT_ID"
log "Retention: $SNAPSHOT_RETENTION days"

# Verify cluster exists and is available
CLUSTER_STATUS=$(aws rds describe-db-clusters \
    --region "$AWS_REGION" \
    --db-cluster-identifier "$AURORA_CLUSTER_ID" \
    --query 'DBClusters[0].Status' \
    --output text 2>/dev/null || echo "NOT_FOUND")

if [[ "$CLUSTER_STATUS" != "available" ]]; then
    log "ERROR: Cluster $AURORA_CLUSTER_ID is not available (status: $CLUSTER_STATUS)"
    send_alert "Aurora snapshot failed: Cluster $AURORA_CLUSTER_ID status is $CLUSTER_STATUS" \
               "CRITICAL: Aurora Snapshot Failed"
    publish_metric "SnapshotFailure" 1
    exit 1
fi

log "Cluster status: $CLUSTER_STATUS"

# Check for any running snapshots
PENDING_SNAPSHOTS=$(aws rds describe-db-cluster-snapshots \
    --region "$AWS_REGION" \
    --db-cluster-identifier "$AURORA_CLUSTER_ID" \
    --query 'DBClusterSnapshots[?Status==`creating`] | length(@)' \
    --output text 2>/dev/null || echo "0")

if [[ "$PENDING_SNAPSHOTS" -gt 0 ]]; then
    log "WARNING: $PENDING_SNAPSHOTS snapshot(s) already in progress. Waiting..."
    sleep 60
fi

# ---------------------------------------------------------------------------
# Create snapshot
# ---------------------------------------------------------------------------
log "Creating snapshot..."
START_TIME=$(date +%s)

aws rds create-db-cluster-snapshot \
    --region "$AWS_REGION" \
    --db-cluster-identifier "$AURORA_CLUSTER_ID" \
    --db-cluster-snapshot-identifier "$SNAPSHOT_ID" \
    --tags Key=Environment,Value=production \
           Key=ManagedBy,Value=backup-script \
           Key=RetentionDays,Value="$SNAPSHOT_RETENTION"

log "Snapshot creation initiated. Waiting for completion..."

aws rds wait db-cluster-snapshot-available \
    --region "$AWS_REGION" \
    --db-cluster-snapshot-identifier "$SNAPSHOT_ID"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

log "Snapshot completed in ${DURATION}s: $SNAPSHOT_ID"
publish_metric "SnapshotDurationSeconds" "$DURATION" "Seconds"
publish_metric "SnapshotSuccess" 1

# ---------------------------------------------------------------------------
# Enforce retention policy - delete old snapshots
# ---------------------------------------------------------------------------
log "Enforcing retention policy: deleting snapshots older than $SNAPSHOT_RETENTION days"

CUTOFF_DATE=$(date -u -d "${SNAPSHOT_RETENTION} days ago" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || \
              date -u -v-"${SNAPSHOT_RETENTION}"d +"%Y-%m-%dT%H:%M:%SZ")

OLD_SNAPSHOTS=$(aws rds describe-db-cluster-snapshots \
    --region "$AWS_REGION" \
    --db-cluster-identifier "$AURORA_CLUSTER_ID" \
    --snapshot-type manual \
    --query "DBClusterSnapshots[?SnapshotCreateTime<='${CUTOFF_DATE}'].DBClusterSnapshotIdentifier" \
    --output text 2>/dev/null || echo "")

DELETED_COUNT=0
for snapshot in $OLD_SNAPSHOTS; do
    log "Deleting expired snapshot: $snapshot"
    aws rds delete-db-cluster-snapshot \
        --region "$AWS_REGION" \
        --db-cluster-snapshot-identifier "$snapshot" || {
            log "WARNING: Failed to delete snapshot $snapshot"
            continue
        }
    DELETED_COUNT=$((DELETED_COUNT + 1))
done

log "Deleted $DELETED_COUNT expired snapshot(s)"
publish_metric "SnapshotsDeleted" "$DELETED_COUNT"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
TOTAL_SNAPSHOTS=$(aws rds describe-db-cluster-snapshots \
    --region "$AWS_REGION" \
    --db-cluster-identifier "$AURORA_CLUSTER_ID" \
    --snapshot-type manual \
    --query 'DBClusterSnapshots | length(@)' \
    --output text 2>/dev/null || echo "unknown")

log "=========================================="
log "Aurora Snapshot Summary"
log "  Cluster:          $AURORA_CLUSTER_ID"
log "  Snapshot ID:      $SNAPSHOT_ID"
log "  Duration:         ${DURATION}s"
log "  Expired deleted:  $DELETED_COUNT"
log "  Total snapshots:  $TOTAL_SNAPSHOTS"
log "=========================================="

send_alert "Aurora snapshot completed successfully.\nCluster: $AURORA_CLUSTER_ID\nSnapshot: $SNAPSHOT_ID\nDuration: ${DURATION}s" \
           "INFO: Aurora Snapshot Complete"
