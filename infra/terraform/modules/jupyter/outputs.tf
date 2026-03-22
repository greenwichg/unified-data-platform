###############################################################################
# JupyterHub Module - Outputs
###############################################################################

output "alb_dns_name" {
  description = "DNS name of the JupyterHub ALB"
  value       = aws_lb.jupyter.dns_name
}

output "security_group_id" {
  description = "Security group ID for JupyterHub tasks"
  value       = aws_security_group.jupyter.id
}

output "service_name" {
  description = "ECS service name for JupyterHub"
  value       = aws_ecs_service.jupyter.name
}
