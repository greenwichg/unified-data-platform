###############################################################################
# Monitoring Module - CloudWatch dashboards and alerts
###############################################################################

# ---------- SNS Topic for Alerts ----------
resource "aws_sns_topic" "alerts" {
  name = "${var.project_name}-${var.environment}-alerts"
  tags = var.tags
}

# ---------- CloudWatch Dashboard ----------
resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${var.project_name}-${var.environment}-overview"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Kafka - Messages Per Minute"
          metrics = [["${var.project_name}", "KafkaMessagesPerMinute", "Environment", var.environment]]
          period  = 60
          stat    = "Sum"
          region  = data.aws_region.current.name
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Flink - Records Processed"
          metrics = [["${var.project_name}", "FlinkRecordsProcessed", "Environment", var.environment]]
          period  = 60
          stat    = "Sum"
          region  = data.aws_region.current.name
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title   = "Druid - Query Latency (ms)"
          metrics = [["${var.project_name}", "DruidQueryLatencyMs", "Environment", var.environment]]
          period  = 60
          stat    = "Average"
          region  = data.aws_region.current.name
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title   = "Athena - Active Queries"
          metrics = [["AWS/Athena", "TotalExecutionTime", "WorkGroup", "${var.project_name}-${var.environment}-adhoc"]]
          period  = 60
          stat    = "SampleCount"
          region  = data.aws_region.current.name
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 24
        height = 6
        properties = {
          title = "Pipeline Lag (seconds)"
          metrics = [
            ["${var.project_name}", "Pipeline1Lag", "Environment", var.environment],
            ["${var.project_name}", "Pipeline2Lag", "Environment", var.environment],
            ["${var.project_name}", "Pipeline3Lag", "Environment", var.environment],
            ["${var.project_name}", "Pipeline4Lag", "Environment", var.environment]
          ]
          period = 60
          stat   = "Maximum"
          region = data.aws_region.current.name
        }
      }
    ]
  })
}

# ---------- Alarms ----------
resource "aws_cloudwatch_metric_alarm" "kafka_lag" {
  alarm_name          = "${var.project_name}-${var.environment}-kafka-consumer-lag"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "KafkaConsumerLag"
  namespace           = var.project_name
  period              = 300
  statistic           = "Maximum"
  threshold           = 1000000
  alarm_description   = "Kafka consumer lag exceeded 1M messages"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    Environment = var.environment
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "flink_checkpoint_failure" {
  alarm_name          = "${var.project_name}-${var.environment}-flink-checkpoint-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "FlinkCheckpointFailures"
  namespace           = var.project_name
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Flink checkpoint failure detected"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    Environment = var.environment
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "druid_query_latency" {
  alarm_name          = "${var.project_name}-${var.environment}-druid-high-latency"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "DruidQueryLatencyMs"
  namespace           = var.project_name
  period              = 300
  extended_statistic  = "p99"
  threshold           = 5000
  alarm_description   = "Druid p99 query latency exceeded 5 seconds"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    Environment = var.environment
  }

  tags = var.tags
}

data "aws_region" "current" {}

