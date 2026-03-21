###############################################################################
# Debezium Module - Outputs
###############################################################################

output "security_group_id" {
  description = "Security group ID for Debezium Connect workers"
  value       = aws_security_group.debezium.id
}

output "service_name" {
  description = "Name of the Debezium Connect ECS service"
  value       = aws_ecs_service.debezium.name
}

output "task_definition_arn" {
  description = "ARN of the Debezium Connect task definition"
  value       = aws_ecs_task_definition.debezium.arn
}

output "task_role_arn" {
  description = "ARN of the Debezium ECS task IAM role"
  value       = aws_iam_role.debezium_task.arn
}

output "execution_role_arn" {
  description = "ARN of the Debezium ECS task execution IAM role"
  value       = aws_iam_role.debezium_execution.arn
}

output "connect_rest_endpoint" {
  description = "Debezium Kafka Connect REST API endpoint (service discovery)"
  value       = "http://debezium-connect.${var.environment}.internal:8083"
}

output "service_discovery_arn" {
  description = "ARN of the Debezium service discovery registration"
  value       = aws_service_discovery_service.debezium.arn
}

output "log_group_name" {
  description = "CloudWatch log group for Debezium Connect workers"
  value       = aws_cloudwatch_log_group.debezium.name
}
