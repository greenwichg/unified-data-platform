###############################################################################
# ECS Module - Variables
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
  description = "VPC ID for ECS cluster"
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs for ECS tasks"
  type        = list(string)
}

variable "kafka_bootstrap_servers" {
  description = "MSK bootstrap servers for Debezium Kafka Connect workers"
  type        = string
}

variable "glue_registry_name" {
  description = "AWS Glue Schema Registry name for Avro serialization"
  type        = string
  default     = "zomato-schema-registry"
}

variable "aws_region" {
  description = "AWS region for Glue Schema Registry"
  type        = string
  default     = "ap-south-1"
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
