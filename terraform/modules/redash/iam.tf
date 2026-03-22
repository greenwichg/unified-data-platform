###############################################################################
# Redash Module - IAM Roles and Policies
# Grants access to query Athena, Druid, Aurora, and read data sources
###############################################################################

# ---------- ECS Task Execution Role ----------
resource "aws_iam_role" "redash_execution" {
  name = "${var.project_name}-${var.environment}-redash-execution-role"

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

resource "aws_iam_role_policy_attachment" "redash_execution" {
  role       = aws_iam_role.redash_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "redash_execution_secrets" {
  name = "redash-execution-secrets"
  role = aws_iam_role.redash_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          var.cookie_secret_arn
        ]
      },
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

# ---------- ECS Task Role ----------
resource "aws_iam_role" "redash_task" {
  name = "${var.project_name}-${var.environment}-redash-task-role"

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

resource "aws_iam_role_policy" "redash_s3_read" {
  name = "redash-s3-read"
  role = aws_iam_role.redash_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "S3QueryResults"
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ]
      Resource = ["*"]
    }]
  })
}

resource "aws_iam_role_policy" "redash_athena" {
  name = "redash-athena-access"
  role = aws_iam_role.redash_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "athena:StartQueryExecution",
        "athena:StopQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:ListQueryExecutions",
        "athena:GetWorkGroup"
      ]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy" "redash_cloudwatch" {
  name = "redash-cloudwatch"
  role = aws_iam_role.redash_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "cloudwatch:PutMetricData",
        "cloudwatch:GetMetricData",
        "cloudwatch:ListMetrics"
      ]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy" "redash_secrets" {
  name = "redash-secrets-read"
  role = aws_iam_role.redash_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "secretsmanager:GetSecretValue"
      ]
      Resource = [
        "arn:aws:secretsmanager:*:*:secret:${var.project_name}/${var.environment}/redash/*"
      ]
    }]
  })
}
