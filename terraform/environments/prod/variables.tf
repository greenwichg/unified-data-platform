###############################################################################
# Variables - Production Environment - Zomato Data Platform
###############################################################################

variable "aws_region" {
  description = "AWS region for production environment"
  type        = string
  default     = "ap-south-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "prod"
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
  default     = "db.r6g.4xlarge"
}

variable "aurora_instance_count" {
  description = "Number of Aurora instances"
  type        = number
  default     = 3
}

# ===================== Kafka =====================

variable "kafka_instance_type" {
  description = "EC2 instance type for Kafka brokers"
  type        = string
  default     = "i3en.6xlarge"
}

variable "kafka_broker_count" {
  description = "Number of Kafka brokers"
  type        = number
  default     = 30
}

variable "kafka_ebs_volume_size" {
  description = "EBS volume size in GB for Kafka brokers"
  type        = number
  default     = 5000
}

# ===================== EMR (Spark) =====================

variable "emr_master_instance_type" {
  description = "Instance type for EMR master node"
  type        = string
  default     = "r6g.4xlarge"
}

variable "emr_core_instance_type" {
  description = "Instance type for EMR core nodes"
  type        = string
  default     = "r6g.8xlarge"
}

variable "emr_core_instance_count" {
  description = "Number of EMR core instances"
  type        = number
  default     = 10
}

# ===================== Trino =====================

variable "trino_etl_worker_count" {
  description = "Number of Trino ETL worker nodes"
  type        = number
  default     = 20
}
