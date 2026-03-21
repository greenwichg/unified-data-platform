###############################################################################
# Monitoring Module - Outputs
###############################################################################

output "sns_topic_arn" {
  description = "ARN of the alerts SNS topic"
  value       = aws_sns_topic.alerts.arn
}

output "sns_topic_name" {
  description = "Name of the alerts SNS topic"
  value       = aws_sns_topic.alerts.name
}

output "dashboard_name" {
  description = "Name of the CloudWatch dashboard"
  value       = aws_cloudwatch_dashboard.main.dashboard_name
}

output "kafka_lag_alarm_arn" {
  description = "ARN of the Kafka consumer lag alarm"
  value       = aws_cloudwatch_metric_alarm.kafka_lag.arn
}

output "flink_checkpoint_alarm_arn" {
  description = "ARN of the Flink checkpoint failure alarm"
  value       = aws_cloudwatch_metric_alarm.flink_checkpoint_failure.arn
}

output "druid_latency_alarm_arn" {
  description = "ARN of the Druid high latency alarm"
  value       = aws_cloudwatch_metric_alarm.druid_query_latency.arn
}
