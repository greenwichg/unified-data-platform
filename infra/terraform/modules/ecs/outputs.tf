###############################################################################
# ECS Module - Outputs
###############################################################################

output "cluster_id" {
  description = "ID of the ECS cluster"
  value       = aws_ecs_cluster.main.id
}

output "cluster_arn" {
  description = "ARN of the ECS cluster"
  value       = aws_ecs_cluster.main.arn
}

output "cluster_name" {
  description = "Name of the ECS cluster"
  value       = aws_ecs_cluster.main.name
}

output "security_group_id" {
  description = "Security group ID for ECS services"
  value       = aws_security_group.ecs.id
}

output "task_execution_role_arn" {
  description = "ARN of the ECS task execution IAM role"
  value       = aws_iam_role.ecs_task_execution.arn
}

output "debezium_task_definition_arn" {
  description = "ARN of the Debezium task definition"
  value       = aws_ecs_task_definition.debezium.arn
}

output "debezium_service_name" {
  description = "Name of the Debezium ECS service"
  value       = aws_ecs_service.debezium.name
}
