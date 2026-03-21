###############################################################################
# Kafka Module - Outputs
###############################################################################

output "security_group_id" {
  description = "Security group ID for the Kafka cluster"
  value       = aws_security_group.kafka.id
}

output "broker_asg_name" {
  description = "Name of the Kafka broker Auto Scaling Group"
  value       = aws_autoscaling_group.kafka.name
}

output "broker_asg_arn" {
  description = "ARN of the Kafka broker Auto Scaling Group"
  value       = aws_autoscaling_group.kafka.arn
}

output "launch_template_id" {
  description = "ID of the Kafka broker launch template"
  value       = aws_launch_template.kafka.id
}

output "iam_role_arn" {
  description = "ARN of the Kafka IAM role"
  value       = aws_iam_role.kafka.arn
}

output "instance_profile_arn" {
  description = "ARN of the Kafka instance profile"
  value       = aws_iam_instance_profile.kafka.arn
}
