###############################################################################
# Outputs - Dev Environment - Zomato Data Platform
###############################################################################

# ===================== VPC =====================

output "vpc_id" {
  description = "ID of the VPC"
  value       = module.vpc.vpc_id
}

output "private_subnet_ids" {
  description = "IDs of private subnets"
  value       = module.vpc.private_subnet_ids
}

output "data_subnet_ids" {
  description = "IDs of data subnets"
  value       = module.vpc.data_subnet_ids
}

# ===================== S3 Data Lake =====================

output "s3_raw_bucket" {
  description = "Name of the S3 raw data bucket"
  value       = module.s3.raw_bucket_name
}

output "s3_processed_bucket" {
  description = "Name of the S3 processed data bucket"
  value       = module.s3.processed_bucket_name
}

output "s3_checkpoints_bucket" {
  description = "Name of the S3 checkpoints bucket"
  value       = module.s3.checkpoints_bucket_name
}

# ===================== Aurora =====================

output "aurora_endpoint" {
  description = "Aurora cluster writer endpoint"
  value       = module.aurora.cluster_endpoint
}

# ===================== Kafka =====================

output "kafka_security_group_id" {
  description = "Security group ID for Kafka brokers"
  value       = module.kafka.security_group_id
}

output "kafka_secondary_security_group_id" {
  description = "Security group ID for secondary Kafka (Druid ingestion) brokers"
  value       = module.kafka_secondary.security_group_id
}

output "consumer_fleet_asg_name" {
  description = "Auto-Scaling Group name for the Kafka consumer fleet"
  value       = module.kafka_consumer_fleet.autoscaling_group_name
}

# ===================== EMR =====================

output "emr_cluster_id" {
  description = "EMR cluster ID"
  value       = module.emr.cluster_id
}

# ===================== Airflow =====================

output "airflow_url" {
  description = "Airflow webserver URL"
  value       = module.airflow.webserver_url
}
