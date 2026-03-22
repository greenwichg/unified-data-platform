###############################################################################
# Kafka Consumer Fleet Module - Variables
# EC2 Auto-Scaling consumer group for Pipeline 4 -> Druid ingestion
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
  description = "VPC ID"
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs for the consumer fleet"
  type        = list(string)
}

variable "instance_type" {
  description = "EC2 instance type for consumer fleet nodes"
  type        = string
  default     = "r8g.xlarge"
}

variable "desired_capacity" {
  description = "Desired number of consumer instances"
  type        = number
  default     = 3
}

variable "min_size" {
  description = "Minimum number of consumer instances"
  type        = number
  default     = 2
}

variable "max_size" {
  description = "Maximum number of consumer instances"
  type        = number
  default     = 12
}

# ---------- Kafka Configuration ----------

variable "kafka_primary_security_group_id" {
  description = "Security group ID of the primary MSK cluster"
  type        = string
}

variable "kafka_secondary_security_group_id" {
  description = "Security group ID of the secondary MSK cluster"
  type        = string
}

variable "kafka_primary_cluster_arn" {
  description = "ARN of the primary MSK cluster"
  type        = string
}

variable "kafka_secondary_cluster_arn" {
  description = "ARN of the secondary MSK cluster"
  type        = string
}

variable "kafka_primary_bootstrap_servers" {
  description = "Bootstrap servers for the primary MSK cluster"
  type        = string
}

variable "kafka_secondary_bootstrap_servers" {
  description = "Bootstrap servers for the secondary MSK cluster"
  type        = string
}

variable "source_topics" {
  description = "Kafka topics to consume from on the primary cluster"
  type        = list(string)
  default     = ["orders", "users", "menu", "promo"]
}

variable "destination_topic" {
  description = "Kafka topic to produce to on the secondary cluster"
  type        = string
  default     = "druid-ingestion-events"
}

variable "consumer_group_id" {
  description = "Kafka consumer group ID"
  type        = string
  default     = "druid-feeder-consumer-group"
}

# ---------- Scaling Configuration ----------

variable "scale_out_lag_threshold" {
  description = "Consumer lag threshold to trigger scale-out"
  type        = number
  default     = 50000
}

variable "scale_in_lag_threshold" {
  description = "Consumer lag threshold to trigger scale-in"
  type        = number
  default     = 5000
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
