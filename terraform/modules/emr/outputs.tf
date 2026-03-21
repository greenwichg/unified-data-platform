###############################################################################
# EMR Module - Outputs
###############################################################################

output "cluster_id" {
  description = "ID of the EMR Spark cluster"
  value       = aws_emr_cluster.spark.id
}

output "cluster_name" {
  description = "Name of the EMR Spark cluster"
  value       = aws_emr_cluster.spark.name
}

output "master_public_dns" {
  description = "Public DNS of the EMR master node"
  value       = aws_emr_cluster.spark.master_public_dns
}

output "master_security_group_id" {
  description = "Security group ID for the EMR master node"
  value       = aws_security_group.emr_master.id
}

output "core_security_group_id" {
  description = "Security group ID for the EMR core nodes"
  value       = aws_security_group.emr_core.id
}

output "service_role_arn" {
  description = "ARN of the EMR service IAM role"
  value       = aws_iam_role.emr_service.arn
}

output "ec2_role_arn" {
  description = "ARN of the EMR EC2 IAM role"
  value       = aws_iam_role.emr_ec2.arn
}

output "ec2_instance_profile_arn" {
  description = "ARN of the EMR EC2 instance profile"
  value       = aws_iam_instance_profile.emr_ec2.arn
}
