###############################################################################
# Trino Module - Outputs
###############################################################################

output "cluster_id" {
  description = "ECS cluster ID for Trino"
  value       = aws_ecs_cluster.trino.id
}

output "cluster_arn" {
  description = "ECS cluster ARN for Trino"
  value       = aws_ecs_cluster.trino.arn
}

output "security_group_id" {
  description = "Security group ID for Trino"
  value       = aws_security_group.trino.id
}

output "iam_role_arn" {
  description = "ARN of the Trino IAM role"
  value       = aws_iam_role.trino.arn
}

output "coordinator_task_definition_arns" {
  description = "Map of Trino coordinator task definition ARNs by cluster type"
  value       = { for k, v in aws_ecs_task_definition.trino_coordinator : k => v.arn }
}

output "worker_task_definition_arns" {
  description = "Map of Trino worker task definition ARNs by cluster type"
  value       = { for k, v in aws_ecs_task_definition.trino_worker : k => v.arn }
}

output "worker_service_names" {
  description = "Map of Trino worker ECS service names by cluster type"
  value       = { for k, v in aws_ecs_service.trino_worker : k => v.name }
}
