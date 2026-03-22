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
