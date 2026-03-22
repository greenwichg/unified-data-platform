###############################################################################
# Debezium Module - Variables
###############################################################################

variable "project_name" {
  description = "Project name prefix for all resources"
  type        = string
  default     = "zomato-data-platform"
}

variable "environment" {
  description = "Environment (dev/staging/prod)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID for Debezium resources"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block for security group rules"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for ECS tasks"
  type        = list(string)
}

variable "ecs_cluster_id" {
  description = "ECS cluster ID to deploy Debezium workers"
  type        = string
}

variable "ecs_cluster_name" {
  description = "ECS cluster name for auto-scaling and alarm dimensions"
  type        = string
}

variable "service_discovery_namespace_id" {
  description = "Service discovery namespace ID for internal DNS registration"
  type        = string
}

variable "debezium_image" {
  description = "Docker image for Debezium Kafka Connect"
  type        = string
  default     = "debezium/connect:2.5"
}

variable "kafka_bootstrap_servers" {
  description = "Kafka/MSK bootstrap servers (comma-separated)"
  type        = string
}

variable "schema_registry_url" {
  description = "Confluent Schema Registry URL"
  type        = string
}

variable "kafka_sasl_secret_arn" {
  description = "ARN of the Secrets Manager secret for Kafka SASL credentials"
  type        = string
  default     = ""
}

variable "task_cpu" {
  description = "CPU units for Debezium Connect task"
  type        = number
  default     = 2048
}

variable "task_memory" {
  description = "Memory (MiB) for Debezium Connect task"
  type        = number
  default     = 8192
}

variable "jvm_heap_mb" {
  description = "JVM heap size in MB for Kafka Connect worker"
  type        = number
  default     = 6144
}

variable "desired_count" {
  description = "Desired number of Debezium Connect workers"
  type        = number
  default     = 3
}

variable "max_count" {
  description = "Maximum number of Debezium Connect workers for auto-scaling"
  type        = number
  default     = 6
}

variable "alarm_sns_topic_arns" {
  description = "SNS topic ARNs for CloudWatch alarm notifications"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
