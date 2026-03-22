###############################################################################
# Redash Module - Outputs
###############################################################################

output "alb_dns_name" {
  description = "DNS name of the Redash ALB"
  value       = aws_lb.redash.dns_name
}

output "alb_arn" {
  description = "ARN of the Redash ALB"
  value       = aws_lb.redash.arn
}

output "alb_zone_id" {
  description = "Route53 zone ID of the Redash ALB"
  value       = aws_lb.redash.zone_id
}

output "server_service_name" {
  description = "Name of the Redash server ECS service"
  value       = aws_ecs_service.redash_server.name
}

output "worker_service_name" {
  description = "Name of the Redash worker ECS service"
  value       = aws_ecs_service.redash_worker.name
}

output "scheduler_service_name" {
  description = "Name of the Redash scheduler ECS service"
  value       = aws_ecs_service.redash_scheduler.name
}

output "security_group_id" {
  description = "Security group ID for Redash ECS tasks"
  value       = aws_security_group.redash.id
}

output "alb_security_group_id" {
  description = "Security group ID for the Redash ALB"
  value       = aws_security_group.redash_alb.id
}

output "task_role_arn" {
  description = "ARN of the Redash ECS task IAM role"
  value       = aws_iam_role.redash_task.arn
}

output "execution_role_arn" {
  description = "ARN of the Redash ECS task execution IAM role"
  value       = aws_iam_role.redash_execution.arn
}

output "redis_endpoint" {
  description = "Primary endpoint address of the Redash Redis cluster"
  value       = aws_elasticache_replication_group.redash.primary_endpoint_address
}

output "target_group_arn" {
  description = "ARN of the Redash ALB target group"
  value       = aws_lb_target_group.redash.arn
}

output "log_group_name" {
  description = "CloudWatch log group for Redash services"
  value       = aws_cloudwatch_log_group.redash.name
}
