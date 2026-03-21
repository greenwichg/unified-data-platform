###############################################################################
# Flink Module - Outputs
###############################################################################

output "cluster_id" {
  description = "ECS cluster ID for Flink"
  value       = aws_ecs_cluster.flink.id
}

output "cluster_arn" {
  description = "ECS cluster ARN for Flink"
  value       = aws_ecs_cluster.flink.arn
}

output "security_group_id" {
  description = "Security group ID for Flink"
  value       = aws_security_group.flink.id
}

output "jobmanager_task_definition_arn" {
  description = "ARN of the Flink JobManager task definition"
  value       = aws_ecs_task_definition.flink_jobmanager.arn
}

output "taskmanager_task_definition_arn" {
  description = "ARN of the Flink TaskManager task definition"
  value       = aws_ecs_task_definition.flink_taskmanager.arn
}

output "iam_role_arn" {
  description = "ARN of the Flink IAM role"
  value       = aws_iam_role.flink.arn
}

output "jobmanager_log_group" {
  description = "CloudWatch log group for Flink JobManager"
  value       = aws_cloudwatch_log_group.flink_jobmanager.name
}

output "taskmanager_log_group" {
  description = "CloudWatch log group for Flink TaskManager"
  value       = aws_cloudwatch_log_group.flink_taskmanager.name
}
