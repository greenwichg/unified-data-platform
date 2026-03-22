###############################################################################
# Kafka Consumer Fleet Module - Outputs
###############################################################################

output "security_group_id" {
  description = "Security group ID for the consumer fleet"
  value       = aws_security_group.consumer_fleet.id
}

output "autoscaling_group_name" {
  description = "Name of the consumer fleet Auto-Scaling Group"
  value       = aws_autoscaling_group.consumer_fleet.name
}

output "autoscaling_group_arn" {
  description = "ARN of the consumer fleet Auto-Scaling Group"
  value       = aws_autoscaling_group.consumer_fleet.arn
}

output "iam_role_arn" {
  description = "IAM role ARN for the consumer fleet instances"
  value       = aws_iam_role.consumer_fleet.arn
}

output "launch_template_id" {
  description = "Launch template ID for the consumer fleet"
  value       = aws_launch_template.consumer_fleet.id
}
