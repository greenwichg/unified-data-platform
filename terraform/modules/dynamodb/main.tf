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

# ---------- Outputs ----------
output "orders_table_name" {
  value = aws_dynamodb_table.orders.name
}

output "orders_stream_arn" {
  value = aws_dynamodb_table.orders.stream_arn
}

output "payments_table_name" {
  value = aws_dynamodb_table.payments.name
}

output "payments_stream_arn" {
  value = aws_dynamodb_table.payments.stream_arn
}

output "user_locations_table_name" {
  value = aws_dynamodb_table.user_locations.name
}

output "user_locations_stream_arn" {
  value = aws_dynamodb_table.user_locations.stream_arn
}
