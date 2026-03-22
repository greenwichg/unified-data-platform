###############################################################################
# Airflow Module - Outputs
###############################################################################

output "airflow_arn" {
  description = "ARN of the MWAA environment"
  value       = aws_mwaa_environment.main.arn
}

output "webserver_url" {
  description = "URL of the Airflow web server"
  value       = aws_mwaa_environment.main.webserver_url
}

output "security_group_id" {
  description = "Security group ID for Airflow"
  value       = aws_security_group.airflow.id
}

output "iam_role_arn" {
  description = "ARN of the Airflow execution IAM role"
  value       = aws_iam_role.airflow.arn
}

output "environment_name" {
  description = "Name of the MWAA environment"
  value       = aws_mwaa_environment.main.name
}
