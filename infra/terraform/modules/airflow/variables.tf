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

variable "environment_class" {
  description = "MWAA environment class (mw1.small, mw1.medium, mw1.large)"
  type        = string
  default     = "mw1.large"
}

variable "max_workers" {
  description = "Maximum number of Airflow workers"
  type        = number
  default     = 25
}

variable "min_workers" {
  description = "Minimum number of Airflow workers"
  type        = number
  default     = 5
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
