###############################################################################
# Variables - Dev Environment - Zomato Data Platform
###############################################################################

variable "aws_region" {
  description = "AWS region for dev environment"
  type        = string
  default     = "ap-south-1"
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
  default     = ["ap-south-1a", "ap-south-1b"]
}

# ===================== Aurora =====================

variable "aurora_instance_class" {
  description = "Instance class for Aurora MySQL"
  type        = string
  default     = "db.t3.medium" # dev: scaled down from db.r6g.4xlarge
}

variable "aurora_instance_count" {
  description = "Number of Aurora instances"
  type        = number
  default     = 1 # dev: single instance, no reader
}

# ===================== Kafka =====================

variable "kafka_instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.t3.small" # dev: scaled down from kafka.r8g.4xlarge
}

variable "kafka_broker_count" {
  description = "Number of Kafka brokers"
  type        = number
  default     = 2 # dev: 2 brokers across 2 AZs (ap-south-1a/b)
}

variable "kafka_ebs_volume_size" {
  description = "EBS volume size in GB for Kafka brokers"
  type        = number
  default     = 20 # dev: scaled down from 2000 GB
}

# ===================== EMR (Spark) =====================

variable "emr_master_instance_type" {
  description = "Instance type for EMR master node"
  type        = string
  default     = "m5.xlarge" # dev: scaled down from r8g.2xlarge
}

variable "emr_core_instance_type" {
  description = "Instance type for EMR core nodes"
  type        = string
  default     = "m5.xlarge" # dev: scaled down from r8g.4xlarge
}

variable "emr_core_instance_count" {
  description = "Number of EMR core instances"
  type        = number
  default     = 1 # dev: single core node
}
