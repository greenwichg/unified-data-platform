###############################################################################
# Kafka Module - Variables (Amazon MSK)
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
  description = "VPC ID for MSK cluster"
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs across 3 AZs for MSK brokers"
  type        = list(string)
}

variable "kafka_version" {
  description = "Apache Kafka version for MSK"
  type        = string
  default     = "3.6.1"
}

variable "instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.r8g.4xlarge"
}

variable "ebs_volume_size" {
  description = "EBS volume size in GB per MSK broker"
  type        = number
  default     = 2000
}

variable "number_of_brokers" {
  description = "Total number of MSK broker nodes (must be a multiple of the number of AZs)"
  type        = number
  default     = 9
}

variable "enhanced_monitoring" {
  description = "MSK enhanced monitoring level"
  type        = string
  default     = "PER_TOPIC_PER_BROKER"
}

variable "log_bucket" {
  description = "S3 bucket name for MSK broker logs"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
