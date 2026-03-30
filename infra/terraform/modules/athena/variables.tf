###############################################################################
# Athena Module - Variables (replacing Trino EC2/ECS variables)
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

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}

# ---------- Athena Workgroup Configuration ----------

variable "workgroup_names" {
  description = "Names of the Athena workgroups to create"
  type        = list(string)
  default     = ["adhoc", "etl", "reporting"]
}

variable "adhoc_bytes_scanned_cutoff" {
  description = "Bytes scanned cutoff per query for the adhoc workgroup"
  type        = number
  default     = 10737418240 # 10 GB
}

variable "etl_bytes_scanned_cutoff" {
  description = "Bytes scanned cutoff per query for the etl workgroup"
  type        = number
  default     = 107374182400 # 100 GB
}

variable "reporting_bytes_scanned_cutoff" {
  description = "Bytes scanned cutoff per query for the reporting workgroup"
  type        = number
  default     = 53687091200 # 50 GB
}

# ---------- Results and Encryption ----------

variable "result_bucket" {
  description = "S3 bucket name for Athena query results and data lake storage"
  type        = string
}

variable "encryption_type" {
  description = "Encryption type for Athena query results (SSE_S3, SSE_KMS, CSE_KMS)"
  type        = string
  default     = "SSE_S3"
}
