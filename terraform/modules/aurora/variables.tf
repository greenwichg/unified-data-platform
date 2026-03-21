###############################################################################
# Aurora MySQL Module - Variables
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
  description = "VPC ID for Aurora cluster"
  type        = string
}

variable "subnet_ids" {
  description = "Data subnet IDs for Aurora"
  type        = list(string)
}

variable "instance_class" {
  description = "Aurora instance class"
  type        = string
  default     = "db.r6g.4xlarge"
}

variable "instance_count" {
  description = "Number of Aurora instances"
  type        = number
  default     = 3
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
