###############################################################################
# Athena Module - Outputs (replacing Trino ECS outputs)
###############################################################################

output "workgroup_names" {
  description = "Map of Athena workgroup names by use case"
  value       = { for k, v in aws_athena_workgroup.this : k => v.name }
}

output "workgroup_arns" {
  description = "Map of Athena workgroup ARNs by use case"
  value       = { for k, v in aws_athena_workgroup.this : k => v.arn }
}

output "data_catalog_name" {
  description = "Name of the Athena Glue Data Catalog"
  value       = aws_athena_data_catalog.glue.name
}

output "result_s3_paths" {
  description = "Map of S3 result output paths by workgroup"
  value = {
    for k, v in aws_athena_workgroup.this : k =>
    "s3://${var.result_bucket}/${var.project_name}/${var.environment}/athena-results/${k}/"
  }
}

output "glue_database_names" {
  description = "Names of the Glue Catalog databases"
  value = {
    raw     = aws_glue_catalog_database.zomato_raw.name
    curated = aws_glue_catalog_database.zomato_curated.name
    gold    = aws_glue_catalog_database.zomato_gold.name
  }
}

output "iam_role_arn" {
  description = "ARN of the Athena IAM role"
  value       = aws_iam_role.athena.arn
}
