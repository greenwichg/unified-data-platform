###############################################################################
# DynamoDB Module - Variables
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

variable "s3_raw_bucket" {
  description = "S3 bucket name for raw DynamoDB stream JSON output (Pipeline 3)"
  type        = string
}

variable "lambda_s3_bucket" {
  description = "S3 bucket where the Lambda deployment package is stored"
  type        = string
}

variable "lambda_s3_key" {
  description = "S3 key for the Lambda deployment package"
  type        = string
  default     = "lambda/dynamodb_stream_processor.zip"
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
