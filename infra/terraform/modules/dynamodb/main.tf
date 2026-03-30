###############################################################################
# DynamoDB Module - Source for Pipeline 3 (DynamoDB Streams)
# Stores order, payment, and location data
###############################################################################

variable "project_name" {
  type    = string
  default = "zomato-data-platform"
}

variable "environment" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}

# ---------- Orders Table ----------
resource "aws_dynamodb_table" "orders" {
  name         = "${var.project_name}-${var.environment}-orders"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "order_id"
  range_key    = "created_at"

  # Enable DynamoDB Streams for Pipeline 3
  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "order_id"
    type = "S"
  }

  attribute {
    name = "created_at"
    type = "S"
  }

  attribute {
    name = "user_id"
    type = "S"
  }

  attribute {
    name = "restaurant_id"
    type = "S"
  }

  global_secondary_index {
    name            = "user-orders-index"
    hash_key        = "user_id"
    range_key       = "created_at"
    projection_type = "ALL"
  }

  global_secondary_index {
    name            = "restaurant-orders-index"
    hash_key        = "restaurant_id"
    range_key       = "created_at"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-orders"
  })
}

# ---------- Payments Table ----------
resource "aws_dynamodb_table" "payments" {
  name         = "${var.project_name}-${var.environment}-payments"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "payment_id"
  range_key    = "order_id"

  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "payment_id"
    type = "S"
  }

  attribute {
    name = "order_id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-payments"
  })
}

# ---------- User Locations Table ----------
resource "aws_dynamodb_table" "user_locations" {
  name         = "${var.project_name}-${var.environment}-user-locations"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "user_id"
  range_key    = "timestamp"

  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "user_id"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-user-locations"
  })
}

# ---------- Lambda: DynamoDB Streams → S3 JSON (Pipeline 3) ----------
# Deploys dynamodb_stream_processor.py as a Lambda function triggered by
# all three DynamoDB Streams. Writes raw JSON to S3 for downstream Spark EMR.

resource "aws_lambda_function" "stream_processor" {
  function_name = "${var.project_name}-${var.environment}-dynamodb-stream-processor"
  description   = "Pipeline 3: Processes DynamoDB Streams events and writes JSON to S3"
  role          = aws_iam_role.dynamodb_streams_consumer.arn
  handler       = "dynamodb_stream_processor.process_stream_event"
  runtime       = "python3.11"
  timeout       = 300
  memory_size   = 512

  s3_bucket = var.lambda_s3_bucket
  s3_key    = var.lambda_s3_key

  environment {
    variables = {
      S3_BUCKET = var.s3_raw_bucket
    }
  }

  tags = merge(var.tags, {
    Name     = "${var.project_name}-${var.environment}-dynamodb-stream-processor"
    Pipeline = "pipeline3-dynamodb-streams"
  })
}

resource "aws_cloudwatch_log_group" "stream_processor" {
  name              = "/aws/lambda/${aws_lambda_function.stream_processor.function_name}"
  retention_in_days = 30
  tags              = var.tags
}

# Add S3 write permission to the Lambda execution role
resource "aws_iam_role_policy" "lambda_s3_write" {
  name = "dynamodb-stream-processor-s3-write"
  role = aws_iam_role.dynamodb_streams_consumer.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject", "s3:GetObject"]
      Resource = "arn:aws:s3:::${var.s3_raw_bucket}/pipeline3-dynamodb/*"
    }]
  })
}

# Event source mapping: orders stream → Lambda
resource "aws_lambda_event_source_mapping" "orders_stream" {
  event_source_arn              = aws_dynamodb_table.orders.stream_arn
  function_name                 = aws_lambda_function.stream_processor.arn
  starting_position             = "TRIM_HORIZON"
  batch_size                    = 1000
  maximum_batching_window_in_seconds = 10
  bisect_batch_on_function_error = true

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.stream_dlq.arn
    }
  }
}

# Event source mapping: payments stream → Lambda
resource "aws_lambda_event_source_mapping" "payments_stream" {
  event_source_arn              = aws_dynamodb_table.payments.stream_arn
  function_name                 = aws_lambda_function.stream_processor.arn
  starting_position             = "TRIM_HORIZON"
  batch_size                    = 1000
  maximum_batching_window_in_seconds = 10
  bisect_batch_on_function_error = true

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.stream_dlq.arn
    }
  }
}

# Event source mapping: user_locations stream → Lambda
resource "aws_lambda_event_source_mapping" "user_locations_stream" {
  event_source_arn              = aws_dynamodb_table.user_locations.stream_arn
  function_name                 = aws_lambda_function.stream_processor.arn
  starting_position             = "TRIM_HORIZON"
  batch_size                    = 500
  maximum_batching_window_in_seconds = 5
  bisect_batch_on_function_error = true

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.stream_dlq.arn
    }
  }
}

# Dead-letter queue for failed stream batches
resource "aws_sqs_queue" "stream_dlq" {
  name                      = "${var.project_name}-${var.environment}-dynamodb-stream-dlq"
  message_retention_seconds = 1209600 # 14 days
  tags                      = var.tags
}

resource "aws_iam_role_policy" "lambda_sqs_dlq" {
  name = "dynamodb-stream-processor-sqs-dlq"
  role = aws_iam_role.dynamodb_streams_consumer.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:SendMessage"]
      Resource = aws_sqs_queue.stream_dlq.arn
    }]
  })
}

