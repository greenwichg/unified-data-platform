###############################################################################
# Trino Module - IAM Roles and Policies
###############################################################################

resource "aws_iam_role" "trino" {
  name = "${var.project_name}-${var.environment}-trino-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "trino_s3" {
  name = "trino-data-access"
  role = aws_iam_role.trino.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:*"]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role_policy" "trino_ecs_execution" {
  name = "trino-ecs-execution"
  role = aws_iam_role.trino.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage"
        ]
        Resource = "*"
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

resource "aws_iam_role_policy" "trino_cloudwatch" {
  name = "trino-cloudwatch"
  role = aws_iam_role.trino.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "cloudwatch:PutMetricData"
      ]
      Resource = "*"
    }]
  })
}
