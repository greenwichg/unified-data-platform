###############################################################################
# Druid Module - Variables
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
  description = "VPC ID for Druid cluster"
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs for Druid nodes"
  type        = list(string)
}

variable "s3_deep_storage_bucket" {
  description = "S3 bucket name for Druid deep storage"
  type        = string
}

variable "kafka_secondary_security_group_id" {
  description = "Security group ID of the secondary MSK cluster for Druid ingestion"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
