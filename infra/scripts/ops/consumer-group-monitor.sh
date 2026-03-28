#!/usr/bin/env bash
# ============================================================================
# Zomato Data Platform - Kafka Consumer Group Lag Monitoring Script
#
# MIGRATION NOTE: In production, this script now runs against Amazon MSK.
# MSK bootstrap servers should be set via KAFKA_BOOTSTRAP_SERVERS env var.
# When using MSK with IAM auth, ensure the kafka-consumer-groups.sh command
# uses --command-config with MSK IAM SASL properties.
# You can also use Amazon CloudWatch metrics for MSK consumer group lag
# monitoring as an alternative to this script.
#
# Monitors consumer group lag across all Zomato data platform consumer groups.
# Publishes lag metrics to CloudWatch and optionally triggers SNS alerts
# when lag exceeds configured thresholds.
#
# Usage:
#   ./consumer-group-monitor.sh                        # One-shot check
#   ./consumer-group-monitor.sh --continuous --interval 30  # Continuous monitoring
#   ./consumer-group-monitor.sh --group flink-cdc-processor # Single group
#   ./consumer-group-monitor.sh --alert-only           # Only show groups exceeding threshold
#
# Typically runs as a systemd service or cron job on a monitoring instance.
# ============================================================================

set -euo pipefail

# ----- Configuration -----
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka1.zomato-data.internal:9092,kafka2.zomato-data.internal:9092,kafka3.zomato-data.internal:9092}"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
AWS_REGION="${AWS_REGION:-ap-south-1}"
CLOUDWATCH_NAMESPACE="${CLOUDWATCH_NAMESPACE:-zomato-data-platform}"
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:-}"
LOG_DIR="/var/log/kafka-monitoring"
LOG_FILE="${LOG_DIR}/consumer-group-monitor.log"

# Consumer groups to monitor and their lag thresholds
declare -A LAG_THRESHOLDS=(
    ["flink-cdc-processor"]=500000
    ["flink-realtime-events"]=200000
    ["ec2-druid-feeder"]=500000
    ["druid-orders-ingestion"]=1000000
    ["druid-realtime-events-ingestion"]=500000
)

# ----- Colors -----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

mkdir -p "$LOG_DIR"

log_info()  { echo -e "${GREEN}[INFO]${NC}  $(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"; }
log_error() { echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"; }

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --group GROUP          Monitor a specific consumer group (default: all known groups)
  --continuous           Run continuously instead of one-shot
  --interval SECONDS     Polling interval in seconds (default: 30)
  --alert-only           Only show groups exceeding lag threshold
  --publish-metrics      Publish lag metrics to CloudWatch
  --no-color             Disable colored output
  --bootstrap SERVERS    Kafka bootstrap servers
  --threshold LAG        Override default lag threshold for all groups
  --json                 Output in JSON format
  -h, --help             Show this help message

Environment:
  KAFKA_BOOTSTRAP_SERVERS    Bootstrap servers (default: internal cluster)
  SNS_TOPIC_ARN              SNS topic for alert notifications
  CLOUDWATCH_NAMESPACE       CloudWatch namespace for metrics
EOF
    exit 0
}

# ----- Get consumer group lag -----
get_group_lag() {
    local group="$1"

    "${KAFKA_HOME}/bin/kafka-consumer-groups.sh" \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
        --describe \
        --group "$group" \
        2>/dev/null
}

# ----- Parse lag from consumer group output -----
parse_total_lag() {
    local output="$1"
    echo "$output" | tail -n +2 | awk '{
        if ($6 ~ /^[0-9]+$/) total += $6
    } END { print total+0 }'
}

# ----- Parse per-topic lag -----
parse_topic_lag() {
    local output="$1"
    echo "$output" | tail -n +2 | awk '{
        if ($6 ~ /^[0-9]+$/) lag[$1] += $6
    } END {
        for (topic in lag) print topic, lag[topic]
    }' | sort -k2 -rn
}

# ----- Parse per-partition details -----
parse_partition_details() {
    local output="$1"
    echo "$output" | tail -n +2 | awk '$6 ~ /^[0-9]+$/ && $6 > 0 {
        printf "  %-30s partition=%-4s current=%-12s end=%-12s lag=%s\n", $1, $2, $4, $5, $6
    }' | sort -t= -k4 -rn | head -20
}

# ----- Publish metrics to CloudWatch -----
publish_cloudwatch_metrics() {
    local group="$1"
    local total_lag="$2"
    local topic_lags="$3"

    if ! command -v aws &>/dev/null; then
        log_warn "AWS CLI not found, skipping CloudWatch publish"
        return
    fi

    # Publish total lag
    aws cloudwatch put-metric-data \
        --region "$AWS_REGION" \
        --namespace "$CLOUDWATCH_NAMESPACE" \
        --metric-name "KafkaConsumerGroupLag" \
        --dimensions "ConsumerGroup=$group" \
        --value "$total_lag" \
        --unit Count \
        --timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        2>/dev/null || log_warn "Failed to publish CloudWatch metric for $group"

    # Publish per-topic lag
    while IFS=' ' read -r topic lag; do
        [ -z "$topic" ] && continue
        aws cloudwatch put-metric-data \
            --region "$AWS_REGION" \
            --namespace "$CLOUDWATCH_NAMESPACE" \
            --metric-name "KafkaConsumerTopicLag" \
            --dimensions "ConsumerGroup=$group,Topic=$topic" \
            --value "$lag" \
            --unit Count \
            --timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            2>/dev/null || true
    done <<< "$topic_lags"
}

# ----- Send SNS alert -----
send_alert() {
    local group="$1"
    local total_lag="$2"
    local threshold="$3"

    if [ -z "$SNS_TOPIC_ARN" ]; then
        return
    fi

    local message
    message=$(cat <<EOMSG
ALERT: Kafka Consumer Group Lag Exceeded Threshold

Consumer Group: $group
Current Lag: $(numfmt --grouping "$total_lag" 2>/dev/null || echo "$total_lag")
Threshold: $(numfmt --grouping "$threshold" 2>/dev/null || echo "$threshold")
Cluster: $KAFKA_BOOTSTRAP_SERVERS
Timestamp: $(date -u '+%Y-%m-%d %H:%M:%S UTC')

Action Required: Check consumer health, scaling, and Kafka cluster status.
Dashboard: https://grafana.zomato-data.internal/d/kafka-consumer-lag
EOMSG
    )

    aws sns publish \
        --region "$AWS_REGION" \
        --topic-arn "$SNS_TOPIC_ARN" \
        --subject "ALERT: Kafka lag exceeded for $group" \
        --message "$message" \
        2>/dev/null && log_info "SNS alert sent for $group" || log_warn "Failed to send SNS alert"
}

# ----- Monitor a single consumer group -----
monitor_group() {
    local group="$1"
    local alert_only="${2:-false}"
    local publish_metrics="${3:-false}"
    local json_output="${4:-false}"
    local override_threshold="${5:-}"

    local output
    output=$(get_group_lag "$group" 2>/dev/null)

    if [ -z "$output" ] || echo "$output" | grep -q "Consumer group.*does not exist"; then
        if [ "$alert_only" != "true" ]; then
            log_warn "Consumer group '$group' not found or has no active members"
        fi
        return
    fi

    local total_lag
    total_lag=$(parse_total_lag "$output")

    local topic_lags
    topic_lags=$(parse_topic_lag "$output")

    local threshold="${override_threshold:-${LAG_THRESHOLDS[$group]:-1000000}}"
    local is_critical=false
    if [ "$total_lag" -gt "$threshold" ]; then
        is_critical=true
    fi

    if [ "$alert_only" = "true" ] && [ "$is_critical" = "false" ]; then
        return
    fi

    if [ "$json_output" = "true" ]; then
        local topics_json="{"
        local first=true
        while IFS=' ' read -r topic lag; do
            [ -z "$topic" ] && continue
            if [ "$first" = "true" ]; then first=false; else topics_json="${topics_json},"; fi
            topics_json="${topics_json}\"${topic}\":${lag}"
        done <<< "$topic_lags"
        topics_json="${topics_json}}"

        echo "{\"group\":\"$group\",\"total_lag\":$total_lag,\"threshold\":$threshold,\"critical\":$is_critical,\"topic_lags\":$topics_json,\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    else
        echo ""
        if [ "$is_critical" = "true" ]; then
            echo -e "${RED}=== Consumer Group: $group [CRITICAL] ===${NC}"
        else
            echo -e "${CYAN}=== Consumer Group: $group ===${NC}"
        fi

        echo -e "  Total Lag: $(numfmt --grouping "$total_lag" 2>/dev/null || echo "$total_lag") (threshold: $(numfmt --grouping "$threshold" 2>/dev/null || echo "$threshold"))"
        echo ""
        echo "  Per-Topic Lag:"
        while IFS=' ' read -r topic lag; do
            [ -z "$topic" ] && continue
            printf "    %-40s %s\n" "$topic" "$(numfmt --grouping "$lag" 2>/dev/null || echo "$lag")"
        done <<< "$topic_lags"

        # Show top lagging partitions
        local partition_details
        partition_details=$(parse_partition_details "$output")
        if [ -n "$partition_details" ]; then
            echo ""
            echo "  Top Lagging Partitions:"
            echo "$partition_details"
        fi
    fi

    # Publish CloudWatch metrics
    if [ "$publish_metrics" = "true" ]; then
        publish_cloudwatch_metrics "$group" "$total_lag" "$topic_lags"
    fi

    # Send alert if critical
    if [ "$is_critical" = "true" ]; then
        send_alert "$group" "$total_lag" "$threshold"
    fi
}

# ----- Main -----
SPECIFIC_GROUP=""
CONTINUOUS=false
INTERVAL=30
ALERT_ONLY=false
PUBLISH_METRICS=false
JSON_OUTPUT=false
OVERRIDE_THRESHOLD=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --group)           SPECIFIC_GROUP="$2"; shift 2 ;;
        --continuous)      CONTINUOUS=true; shift ;;
        --interval)        INTERVAL="$2"; shift 2 ;;
        --alert-only)      ALERT_ONLY=true; shift ;;
        --publish-metrics) PUBLISH_METRICS=true; shift ;;
        --no-color)        RED=""; GREEN=""; YELLOW=""; CYAN=""; NC=""; shift ;;
        --bootstrap)       KAFKA_BOOTSTRAP_SERVERS="$2"; shift 2 ;;
        --threshold)       OVERRIDE_THRESHOLD="$2"; shift 2 ;;
        --json)            JSON_OUTPUT=true; shift ;;
        -h|--help)         usage ;;
        *)                 log_error "Unknown option: $1"; usage ;;
    esac
done

run_check() {
    if [ -n "$SPECIFIC_GROUP" ]; then
        monitor_group "$SPECIFIC_GROUP" "$ALERT_ONLY" "$PUBLISH_METRICS" "$JSON_OUTPUT" "$OVERRIDE_THRESHOLD"
    else
        if [ "$JSON_OUTPUT" != "true" ]; then
            log_info "Checking consumer group lag ($(date -u '+%Y-%m-%d %H:%M:%S UTC'))"
        fi

        for group in "${!LAG_THRESHOLDS[@]}"; do
            monitor_group "$group" "$ALERT_ONLY" "$PUBLISH_METRICS" "$JSON_OUTPUT" "$OVERRIDE_THRESHOLD"
        done

        # Also check for any unknown active consumer groups
        local all_groups
        all_groups=$("${KAFKA_HOME}/bin/kafka-consumer-groups.sh" \
            --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
            --list 2>/dev/null || echo "")

        for group in $all_groups; do
            if [ -z "${LAG_THRESHOLDS[$group]+x}" ]; then
                monitor_group "$group" "$ALERT_ONLY" "$PUBLISH_METRICS" "$JSON_OUTPUT" "$OVERRIDE_THRESHOLD"
            fi
        done
    fi
}

if [ "$CONTINUOUS" = "true" ]; then
    log_info "Starting continuous monitoring (interval: ${INTERVAL}s)"
    while true; do
        run_check
        sleep "$INTERVAL"
    done
else
    run_check
fi
