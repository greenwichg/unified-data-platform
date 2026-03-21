###############################################################################
# S3 Module - Outputs
###############################################################################

output "raw_bucket_name" {
  description = "Name of the raw data lake S3 bucket"
  value       = aws_s3_bucket.data_lake_raw.bucket
}

output "raw_bucket_arn" {
  description = "ARN of the raw data lake S3 bucket"
  value       = aws_s3_bucket.data_lake_raw.arn
}

output "raw_bucket_id" {
  description = "ID of the raw data lake S3 bucket"
  value       = aws_s3_bucket.data_lake_raw.id
}

output "processed_bucket_name" {
  description = "Name of the processed data lake S3 bucket"
  value       = aws_s3_bucket.data_lake_processed.bucket
}

output "processed_bucket_arn" {
  description = "ARN of the processed data lake S3 bucket"
  value       = aws_s3_bucket.data_lake_processed.arn
}

output "processed_bucket_id" {
  description = "ID of the processed data lake S3 bucket"
  value       = aws_s3_bucket.data_lake_processed.id
}

output "checkpoints_bucket_name" {
  description = "Name of the checkpoints S3 bucket"
  value       = aws_s3_bucket.checkpoints.bucket
}

output "checkpoints_bucket_arn" {
  description = "ARN of the checkpoints S3 bucket"
  value       = aws_s3_bucket.checkpoints.arn
}

output "airflow_logs_bucket_name" {
  description = "Name of the Airflow logs S3 bucket"
  value       = aws_s3_bucket.airflow_logs.bucket
}

output "airflow_logs_bucket_arn" {
  description = "ARN of the Airflow logs S3 bucket"
  value       = aws_s3_bucket.airflow_logs.arn
}
