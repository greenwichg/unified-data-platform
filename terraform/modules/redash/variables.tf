###############################################################################
# Redash Module - Variables
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
  description = "VPC ID for Redash resources"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for ECS tasks and ALB"
  type        = list(string)
}

variable "ecs_cluster_id" {
  description = "ECS cluster ID to deploy Redash services"
  type        = string
}

variable "ecs_cluster_name" {
  description = "ECS cluster name for auto-scaling resource IDs"
  type        = string
}

variable "redash_image" {
  description = "Docker image for Redash"
  type        = string
  default     = "redash/redash:10.1.0.b50633"
}

variable "redash_port" {
  description = "Port the Redash server listens on"
  type        = number
  default     = 5000
}

variable "redash_database_url" {
  description = "PostgreSQL connection URL for Redash metadata database"
  type        = string
  sensitive   = true
}

variable "cookie_secret_arn" {
  description = "ARN of the Secrets Manager secret for Redash cookie secret"
  type        = string
}

variable "certificate_arn" {
  description = "ACM certificate ARN for HTTPS on the ALB"
  type        = string
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access Redash ALB"
  type        = list(string)
  default     = ["10.0.0.0/8"]
}

variable "server_cpu" {
  description = "CPU units for Redash server task"
  type        = number
  default     = 1024
}

variable "server_memory" {
  description = "Memory (MiB) for Redash server task"
  type        = number
  default     = 2048
}

variable "worker_cpu" {
  description = "CPU units for Redash worker task"
  type        = number
  default     = 2048
}

variable "worker_memory" {
  description = "Memory (MiB) for Redash worker task"
  type        = number
  default     = 4096
}

variable "server_desired_count" {
  description = "Desired number of Redash server tasks"
  type        = number
  default     = 2
}

variable "server_max_count" {
  description = "Maximum number of Redash server tasks for auto-scaling"
  type        = number
  default     = 4
}

variable "worker_desired_count" {
  description = "Desired number of Redash worker tasks"
  type        = number
  default     = 3
}

variable "redis_node_type" {
  description = "ElastiCache node type for Redash Redis"
  type        = string
  default     = "cache.r6g.large"
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
