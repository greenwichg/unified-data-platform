###############################################################################
# DynamoDB Module - IAM Roles and Policies
###############################################################################

# ---------- DynamoDB Streams Consumer Role ----------
resource "aws_iam_role" "dynamodb_streams_consumer" {
  name = "${var.project_name}-${var.environment}-dynamodb-streams-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "dynamodb_streams_consumer" {
  name = "dynamodb-streams-consumer"
  role = aws_iam_role.dynamodb_streams_consumer.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:DescribeStream",
          "dynamodb:GetRecords",
          "dynamodb:GetShardIterator",
          "dynamodb:ListStreams"
        ]
        Resource = [
          aws_dynamodb_table.orders.stream_arn,
          aws_dynamodb_table.payments.stream_arn,
          aws_dynamodb_table.user_locations.stream_arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:DescribeTable",
          "dynamodb:GetItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = [
          aws_dynamodb_table.orders.arn,
          "${aws_dynamodb_table.orders.arn}/index/*",
          aws_dynamodb_table.payments.arn,
          "${aws_dynamodb_table.payments.arn}/index/*",
          aws_dynamodb_table.user_locations.arn,
          "${aws_dynamodb_table.user_locations.arn}/index/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

# ---------- DynamoDB Backup Role ----------
resource "aws_iam_role" "dynamodb_backup" {
  name = "${var.project_name}-${var.environment}-dynamodb-backup-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "backup.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "dynamodb_backup" {
  name = "dynamodb-backup-policy"
  role = aws_iam_role.dynamodb_backup.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:CreateBackup",
          "dynamodb:DescribeBackup",
          "dynamodb:ListBackups",
          "dynamodb:RestoreTableFromBackup",
          "dynamodb:DeleteBackup"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "backup:DescribeBackupVault",
          "backup:CopyIntoBackupVault"
        ]
        Resource = "*"
      }
    ]
  })
}
