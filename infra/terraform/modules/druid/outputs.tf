###############################################################################
# Druid Module - Outputs
###############################################################################

output "security_group_id" {
  description = "Security group ID for Druid"
  value       = aws_security_group.druid.id
}

output "iam_role_arn" {
  description = "ARN of the Druid IAM role"
  value       = aws_iam_role.druid.arn
}

output "instance_profile_arn" {
  description = "ARN of the Druid instance profile"
  value       = aws_iam_instance_profile.druid.arn
}

output "launch_template_ids" {
  description = "Map of Druid launch template IDs by node type"
  value       = { for k, v in aws_launch_template.druid : k => v.id }
}

output "asg_names" {
  description = "Map of Druid ASG names by node type"
  value       = { for k, v in aws_autoscaling_group.druid : k => v.name }
}

output "asg_arns" {
  description = "Map of Druid ASG ARNs by node type"
  value       = { for k, v in aws_autoscaling_group.druid : k => v.arn }
}
