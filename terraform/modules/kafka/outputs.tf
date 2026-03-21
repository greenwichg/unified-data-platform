###############################################################################
# Kafka Module - Outputs (Amazon MSK)
###############################################################################

output "bootstrap_brokers_tls" {
  description = "TLS connection string for MSK brokers"
  value       = aws_msk_cluster.this.bootstrap_brokers_tls
}

output "bootstrap_brokers_iam" {
  description = "IAM authentication connection string for MSK brokers"
  value       = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
}

output "zookeeper_connect_string" {
  description = "ZooKeeper connection string (MSK managed)"
  value       = aws_msk_cluster.this.zookeeper_connect_string
}

output "cluster_arn" {
  description = "ARN of the MSK cluster"
  value       = aws_msk_cluster.this.arn
}

output "cluster_name" {
  description = "Name of the MSK cluster"
  value       = aws_msk_cluster.this.cluster_name
}

output "security_group_id" {
  description = "Security group ID for the MSK cluster"
  value       = aws_security_group.kafka.id
}
