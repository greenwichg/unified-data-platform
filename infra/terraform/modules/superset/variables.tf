###############################################################################
# Superset Module - Variables
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
  description = "VPC ID for Superset resources"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for ECS tasks and ALB"
  type        = list(string)
}

variable "ecs_cluster_id" {
  description = "ECS cluster ID to deploy Superset services"
  type        = string
}

variable "ecs_cluster_name" {
  description = "ECS cluster name for auto-scaling resource IDs"
  type        = string
}

variable "superset_image" {
  description = "Docker image for Superset"
  type        = string
  default     = "apache/superset:3.1.0"
}

variable "superset_port" {
  description = "Port the Superset web server listens on"
  type        = number
  default     = 8088
}

variable "superset_database_url" {
  description = "PostgreSQL connection URL for Superset metadata database"
  type        = string
  sensitive   = true
}

variable "secret_key_arn" {
  description = "ARN of the Secrets Manager secret for Superset SECRET_KEY"
  type        = string
}

variable "certificate_arn" {
  description = "ACM certificate ARN for HTTPS on the ALB"
  type        = string
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access Superset ALB"
  type        = list(string)
  default     = ["10.0.0.0/8"]
}

variable "web_cpu" {
  description = "CPU units for Superset web task"
  type        = number
  default     = 2048
}

variable "web_memory" {
  description = "Memory (MiB) for Superset web task"
  type        = number
  default     = 4096
}

variable "worker_cpu" {
  description = "CPU units for Superset Celery worker task"
  type        = number
  default     = 2048
}

variable "worker_memory" {
  description = "Memory (MiB) for Superset Celery worker task"
  type        = number
  default     = 4096
}

variable "web_desired_count" {
  description = "Desired number of Superset web tasks"
  type        = number
  default     = 2
}

variable "web_max_count" {
  description = "Maximum number of Superset web tasks for auto-scaling"
  type        = number
  default     = 6
}

variable "worker_desired_count" {
  description = "Desired number of Superset Celery worker tasks"
  type        = number
  default     = 3
}

variable "redis_node_type" {
  description = "ElastiCache node type for Superset Redis"
  type        = string
  default     = "cache.r6g.large"
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
