###############################################################################
# Variables - Staging Environment - Zomato Data Platform
###############################################################################

variable "aws_region" {
  description = "AWS region for staging environment"
  type        = string
  default     = "ap-south-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "staging"
}

# ===================== VPC =====================

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "List of availability zones"
  type        = list(string)
  default     = ["ap-south-1a", "ap-south-1b", "ap-south-1c"]
}

# ===================== Aurora =====================

variable "aurora_instance_class" {
  description = "Instance class for Aurora MySQL"
  type        = string
  default     = "db.r6g.2xlarge"
}

variable "aurora_instance_count" {
  description = "Number of Aurora instances"
  type        = number
  default     = 2
}

# ===================== Kafka =====================

variable "kafka_instance_type" {
  description = "MSK broker instance type (migrated from self-hosted EC2 Kafka)"
  type        = string
  default     = "kafka.r8g.2xlarge"
}

variable "kafka_broker_count" {
  description = "Number of Kafka brokers"
  type        = number
  default     = 6
}

variable "kafka_ebs_volume_size" {
  description = "EBS volume size in GB for Kafka brokers"
  type        = number
  default     = 1000
}

# ===================== EMR (Spark) =====================

variable "emr_master_instance_type" {
  description = "Instance type for EMR master node"
  type        = string
  default     = "r8g.2xlarge"
}

variable "emr_core_instance_type" {
  description = "Instance type for EMR core nodes"
  type        = string
  default     = "r8g.4xlarge"
}

variable "emr_core_instance_count" {
  description = "Number of EMR core instances"
  type        = number
  default     = 3
}

# ===================== Dashboard Layer =====================

variable "certificate_arn" {
  description = "ACM certificate ARN for HTTPS on dashboard ALBs"
  type        = string
}

variable "superset_database_url" {
  description = "PostgreSQL connection URL for Superset metadata"
  type        = string
  sensitive   = true
}

variable "superset_secret_key_arn" {
  description = "Secrets Manager ARN for Superset SECRET_KEY"
  type        = string
}

variable "redash_database_url" {
  description = "PostgreSQL connection URL for Redash metadata"
  type        = string
  sensitive   = true
}

variable "redash_cookie_secret_arn" {
  description = "Secrets Manager ARN for Redash cookie secret"
  type        = string
}

# ===================== Athena (replaced Trino) =====================
# Trino worker count is no longer needed - Athena is serverless.
# Kept for backward compatibility during migration; will be removed.

variable "trino_etl_worker_count" {
  description = "DEPRECATED: Trino replaced by serverless Athena. This variable is unused."
  type        = number
  default     = 0
}
