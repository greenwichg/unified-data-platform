###############################################################################
# DynamoDB Module - Outputs
###############################################################################

output "orders_table_name" {
  description = "Name of the orders DynamoDB table"
  value       = aws_dynamodb_table.orders.name
}

output "orders_table_arn" {
  description = "ARN of the orders DynamoDB table"
  value       = aws_dynamodb_table.orders.arn
}

output "orders_stream_arn" {
  description = "Stream ARN of the orders DynamoDB table"
  value       = aws_dynamodb_table.orders.stream_arn
}

output "payments_table_name" {
  description = "Name of the payments DynamoDB table"
  value       = aws_dynamodb_table.payments.name
}

output "payments_table_arn" {
  description = "ARN of the payments DynamoDB table"
  value       = aws_dynamodb_table.payments.arn
}

output "payments_stream_arn" {
  description = "Stream ARN of the payments DynamoDB table"
  value       = aws_dynamodb_table.payments.stream_arn
}

output "user_locations_table_name" {
  description = "Name of the user locations DynamoDB table"
  value       = aws_dynamodb_table.user_locations.name
}

output "user_locations_table_arn" {
  description = "ARN of the user locations DynamoDB table"
  value       = aws_dynamodb_table.user_locations.arn
}

output "user_locations_stream_arn" {
  description = "Stream ARN of the user locations DynamoDB table"
  value       = aws_dynamodb_table.user_locations.stream_arn
}

output "stream_processor_lambda_arn" {
  description = "ARN of the DynamoDB Streams → S3 Lambda function (Pipeline 3)"
  value       = aws_lambda_function.stream_processor.arn
}

output "stream_processor_lambda_name" {
  description = "Name of the DynamoDB Streams → S3 Lambda function"
  value       = aws_lambda_function.stream_processor.function_name
}

output "stream_dlq_arn" {
  description = "ARN of the DynamoDB stream processor dead-letter queue"
  value       = aws_sqs_queue.stream_dlq.arn
}
