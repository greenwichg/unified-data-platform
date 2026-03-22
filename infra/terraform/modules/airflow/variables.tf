###############################################################################
# Airflow Module - Variables
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
  description = "VPC ID for Airflow"
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs for Airflow"
  type        = list(string)
}

variable "s3_dags_bucket" {
  description = "S3 bucket name for Airflow DAGs"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
