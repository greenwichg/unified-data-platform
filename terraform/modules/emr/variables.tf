###############################################################################
# EMR Module - Variables
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
  description = "VPC ID for EMR cluster"
  type        = string
}

variable "subnet_id" {
  description = "Subnet for EMR cluster"
  type        = string
}

variable "s3_log_bucket" {
  description = "S3 bucket name for EMR logs"
  type        = string
}

variable "s3_output_bucket" {
  description = "S3 bucket name for EMR output data"
  type        = string
}

variable "master_instance_type" {
  description = "EC2 instance type for EMR master node"
  type        = string
  default     = "r6g.2xlarge"
}

variable "core_instance_type" {
  description = "EC2 instance type for EMR core nodes"
  type        = string
  default     = "r6g.4xlarge"
}

variable "core_instance_count" {
  description = "Number of EMR core instances"
  type        = number
  default     = 5
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
