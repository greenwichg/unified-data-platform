###############################################################################
# Flink Module - Variables
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
  description = "VPC ID for Flink cluster"
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs for Flink tasks"
  type        = list(string)
}

variable "kafka_security_group_id" {
  description = "Security group ID of the Kafka cluster"
  type        = string
}

variable "s3_checkpoints_bucket" {
  description = "S3 bucket name for Flink checkpoints and savepoints"
  type        = string
}

variable "s3_output_bucket" {
  description = "S3 bucket name for Flink output data"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
