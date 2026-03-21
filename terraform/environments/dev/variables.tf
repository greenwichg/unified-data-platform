###############################################################################
# Variables - Dev Environment - Zomato Data Platform
###############################################################################

variable "aws_region" {
  description = "AWS region for dev environment"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
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
  default     = ["us-east-1a", "us-east-1b"]
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
  description = "EC2 instance type for Kafka brokers"
  type        = string
  default     = "r6g.2xlarge"
}

variable "kafka_broker_count" {
  description = "Number of Kafka brokers"
  type        = number
  default     = 3
}

variable "kafka_ebs_volume_size" {
  description = "EBS volume size in GB for Kafka brokers"
  type        = number
  default     = 500
}

# ===================== EMR (Spark) =====================

variable "emr_master_instance_type" {
  description = "Instance type for EMR master node"
  type        = string
  default     = "r6g.xlarge"
}

variable "emr_core_instance_type" {
  description = "Instance type for EMR core nodes"
  type        = string
  default     = "r6g.2xlarge"
}

variable "emr_core_instance_count" {
  description = "Number of EMR core instances"
  type        = number
  default     = 2
}
