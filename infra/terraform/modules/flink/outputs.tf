###############################################################################
# Flink Module - Outputs
# Amazon Managed Service for Apache Flink
###############################################################################

output "application_arns" {
  description = "Map of Managed Flink application ARNs"
  value       = { for k, v in aws_kinesisanalyticsv2_application.flink : k => v.arn }
}

output "application_names" {
  description = "Map of Managed Flink application names"
  value       = { for k, v in aws_kinesisanalyticsv2_application.flink : k => v.name }
}

output "iam_role_arn" {
  description = "ARN of the Managed Flink IAM role"
  value       = aws_iam_role.flink.arn
}

output "security_group_id" {
  description = "Security group ID for Managed Flink VPC connectivity"
  value       = aws_security_group.flink.id
}

output "log_group_names" {
  description = "Map of CloudWatch log group names for each Flink application"
  value       = { for k, v in aws_cloudwatch_log_group.flink : k => v.name }
}
