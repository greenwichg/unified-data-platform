###############################################################################
# Flink Module - Variables
# Amazon Managed Service for Apache Flink (formerly Kinesis Data Analytics)
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
  description = "VPC ID for Managed Flink applications (VPC connectivity to MSK)"
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs for Managed Flink VPC configuration"
  type        = list(string)
}

variable "kafka_security_group_id" {
  description = "Security group ID of the MSK/Kafka cluster"
  type        = string
}

variable "kafka_bootstrap_servers" {
  description = "MSK/Kafka bootstrap server addresses"
  type        = string
}

variable "schema_registry_url" {
  description = "Schema Registry URL for Avro/Protobuf serialization"
  type        = string
  default     = "http://schema-registry.zomato-data.internal:8081"
}

variable "s3_checkpoints_bucket" {
  description = "S3 bucket name for Flink checkpoints and savepoints"
  type        = string
}

variable "s3_output_bucket" {
  description = "S3 bucket name for Flink output data (data lake)"
  type        = string
}

variable "code_s3_bucket" {
  description = "S3 bucket containing Flink application JAR artifacts"
  type        = string
}

variable "runtime_environment" {
  description = "Apache Flink runtime version for Managed Flink applications"
  type        = string
  default     = "FLINK-1_18"

  validation {
    condition     = contains(["FLINK-1_15", "FLINK-1_18", "FLINK-1_19"], var.runtime_environment)
    error_message = "runtime_environment must be one of: FLINK-1_15, FLINK-1_18, FLINK-1_19"
  }
}

variable "application_configs" {
  description = "Map of Flink application configurations (name -> settings)"
  type = map(object({
    code_s3_path = string
  }))

  default = {
    "pipeline1-cep" = {
      code_s3_path = "flink-apps/pipeline1-cep/pipeline1-cep-latest.zip"
    }
    "pipeline2-cdc" = {
      code_s3_path = "flink-apps/pipeline2-cdc/pipeline2-cdc-latest.zip"
    }
    "pipeline4-realtime" = {
      code_s3_path = "flink-apps/pipeline4-realtime/pipeline4-realtime-latest.zip"
    }
  }
}

variable "auto_scaling_enabled" {
  description = "Enable auto-scaling for Managed Flink applications"
  type        = bool
  default     = true
}

variable "parallelism_override" {
  description = "Override parallelism for all Flink applications (0 = use defaults)"
  type        = number
  default     = 0
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
