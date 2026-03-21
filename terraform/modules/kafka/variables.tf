###############################################################################
# Kafka Module - Variables
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
  description = "VPC ID for Kafka cluster"
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs for Kafka brokers"
  type        = list(string)
}

variable "instance_type" {
  description = "EC2 instance type for Kafka brokers"
  type        = string
  default     = "r6g.4xlarge"
}

variable "broker_count" {
  description = "Number of Kafka brokers"
  type        = number
  default     = 9
}

variable "ebs_volume_size" {
  description = "EBS volume size in GB per broker"
  type        = number
  default     = 2000
}

variable "kafka_version" {
  description = "Apache Kafka version"
  type        = string
  default     = "3.6.1"
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
