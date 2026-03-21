###############################################################################
# Hive Metastore Module - Variables
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
  description = "VPC ID for Hive Metastore"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block for security group rules"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for Hive Metastore ECS service"
  type        = list(string)
}

variable "data_subnet_ids" {
  description = "Data subnet IDs for Hive Metastore RDS"
  type        = list(string)
}

variable "s3_warehouse_bucket" {
  description = "S3 bucket name for Hive warehouse data"
  type        = string
}

variable "db_instance_class" {
  description = "RDS instance class for Hive Metastore database"
  type        = string
  default     = "db.r6g.large"
}

variable "db_instance_count" {
  description = "Number of RDS instances"
  type        = number
  default     = 2
}

variable "task_cpu" {
  description = "CPU units for Hive Metastore ECS task"
  type        = number
  default     = 2048
}

variable "task_memory" {
  description = "Memory (MiB) for Hive Metastore ECS task"
  type        = number
  default     = 4096
}

variable "desired_count" {
  description = "Desired number of Hive Metastore tasks"
  type        = number
  default     = 2
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
