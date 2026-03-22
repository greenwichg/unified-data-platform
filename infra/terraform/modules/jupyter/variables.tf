###############################################################################
# JupyterHub Module - Variables
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
  description = "VPC ID for JupyterHub resources"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for ECS tasks and ALB"
  type        = list(string)
}

variable "ecs_cluster_id" {
  description = "ECS cluster ID to deploy JupyterHub"
  type        = string
}

variable "jupyter_image" {
  description = "Docker image for JupyterHub"
  type        = string
  default     = "jupyterhub/jupyterhub:4.0"
}

variable "jupyter_port" {
  description = "Port JupyterHub listens on"
  type        = number
  default     = 8000
}

variable "certificate_arn" {
  description = "ACM certificate ARN for HTTPS on the ALB"
  type        = string
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access JupyterHub ALB"
  type        = list(string)
  default     = ["10.0.0.0/8"]
}

variable "s3_data_bucket" {
  description = "S3 bucket name for data lake access from notebooks"
  type        = string
}

variable "druid_broker_url" {
  description = "Druid broker URL for notebook queries"
  type        = string
  default     = "http://druid-broker.zomato-data.internal:8082"
}

variable "cpu" {
  description = "CPU units for JupyterHub task"
  type        = number
  default     = 2048
}

variable "memory" {
  description = "Memory (MiB) for JupyterHub task"
  type        = number
  default     = 8192
}

variable "desired_count" {
  description = "Desired number of JupyterHub tasks"
  type        = number
  default     = 1
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
