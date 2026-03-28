###############################################################################
# Superset Module - Outputs
###############################################################################

output "alb_dns_name" {
  description = "DNS name of the Superset ALB"
  value       = aws_lb.superset.dns_name
}

output "alb_arn" {
  description = "ARN of the Superset ALB"
  value       = aws_lb.superset.arn
}

output "alb_zone_id" {
  description = "Route53 zone ID of the Superset ALB"
  value       = aws_lb.superset.zone_id
}

output "web_service_name" {
  description = "Name of the Superset web ECS service"
  value       = aws_ecs_service.superset_web.name
}

output "worker_service_name" {
  description = "Name of the Superset worker ECS service"
  value       = aws_ecs_service.superset_worker.name
}

output "beat_service_name" {
  description = "Name of the Superset beat ECS service"
  value       = aws_ecs_service.superset_beat.name
}

output "security_group_id" {
  description = "Security group ID for Superset ECS tasks"
  value       = aws_security_group.superset.id
}

output "alb_security_group_id" {
  description = "Security group ID for the Superset ALB"
  value       = aws_security_group.superset_alb.id
}

output "task_role_arn" {
  description = "ARN of the Superset ECS task IAM role"
  value       = aws_iam_role.superset_task.arn
}

output "execution_role_arn" {
  description = "ARN of the Superset ECS task execution IAM role"
  value       = aws_iam_role.superset_execution.arn
}

output "redis_endpoint" {
  description = "Primary endpoint address of the Superset Redis cluster"
  value       = aws_elasticache_replication_group.superset.primary_endpoint_address
}

output "target_group_arn" {
  description = "ARN of the Superset ALB target group"
  value       = aws_lb_target_group.superset.arn
}

output "log_group_name" {
  description = "CloudWatch log group for Superset services"
  value       = aws_cloudwatch_log_group.superset.name
}
