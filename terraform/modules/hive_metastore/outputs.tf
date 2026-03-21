###############################################################################
# Hive Metastore Module - Outputs
###############################################################################

output "thrift_endpoint" {
  description = "Hive Metastore Thrift endpoint (service discovery DNS)"
  value       = "thrift://hive-metastore.${var.environment}.internal:9083"
}

output "thrift_host" {
  description = "Hive Metastore Thrift host for catalog configuration"
  value       = "hive-metastore.${var.environment}.internal"
}

output "thrift_port" {
  description = "Hive Metastore Thrift port"
  value       = 9083
}

output "security_group_id" {
  description = "Security group ID for the Hive Metastore Thrift service"
  value       = aws_security_group.hive_metastore.id
}

output "security_group_db_id" {
  description = "Security group ID for the Hive Metastore RDS database"
  value       = aws_security_group.hive_metastore_db.id
}

output "rds_cluster_endpoint" {
  description = "Writer endpoint for the Hive Metastore Aurora MySQL cluster"
  value       = aws_rds_cluster.hive_metastore.endpoint
}

output "rds_cluster_reader_endpoint" {
  description = "Reader endpoint for the Hive Metastore Aurora MySQL cluster"
  value       = aws_rds_cluster.hive_metastore.reader_endpoint
}

output "rds_cluster_id" {
  description = "ID of the Hive Metastore Aurora MySQL cluster"
  value       = aws_rds_cluster.hive_metastore.id
}

output "ecs_cluster_id" {
  description = "ECS cluster ID running the Hive Metastore Thrift service"
  value       = aws_ecs_cluster.hive_metastore.id
}

output "ecs_service_name" {
  description = "Name of the ECS service running Hive Metastore"
  value       = aws_ecs_service.hive_metastore.name
}

output "task_role_arn" {
  description = "ARN of the Hive Metastore ECS task IAM role"
  value       = aws_iam_role.hive_metastore.arn
}

output "service_discovery_namespace_id" {
  description = "Service discovery namespace ID for internal DNS"
  value       = aws_service_discovery_private_dns_namespace.hive_metastore.id
}

output "log_group_name" {
  description = "CloudWatch log group for Hive Metastore"
  value       = aws_cloudwatch_log_group.hive_metastore.name
}
