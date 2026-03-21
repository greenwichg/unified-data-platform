###############################################################################
# Superset Module - IAM Roles and Policies
# Grants access to query Trino, Druid, Aurora, and S3 data sources
###############################################################################

# ---------- ECS Task Execution Role ----------
resource "aws_iam_role" "superset_execution" {
  name = "${var.project_name}-${var.environment}-superset-execution-role"

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

resource "aws_iam_role_policy_attachment" "superset_execution" {
  role       = aws_iam_role.superset_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "superset_execution_secrets" {
  name = "superset-execution-secrets"
  role = aws_iam_role.superset_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          var.secret_key_arn
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
resource "aws_iam_role" "superset_task" {
  name = "${var.project_name}-${var.environment}-superset-task-role"

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

resource "aws_iam_role_policy" "superset_s3_read" {
  name = "superset-s3-read"
  role = aws_iam_role.superset_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "S3DataLakeRead"
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

resource "aws_iam_role_policy" "superset_athena" {
  name = "superset-athena-access"
  role = aws_iam_role.superset_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
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
      },
      {
        Sid    = "AthenaResultsBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.project_name}-${var.environment}-athena-results",
          "arn:aws:s3:::${var.project_name}-${var.environment}-athena-results/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "superset_glue_read" {
  name = "superset-glue-read"
  role = aws_iam_role.superset_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartition",
        "glue:GetPartitions"
      ]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy" "superset_cloudwatch" {
  name = "superset-cloudwatch"
  role = aws_iam_role.superset_task.id

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

resource "aws_iam_role_policy" "superset_secrets" {
  name = "superset-secrets-read"
  role = aws_iam_role.superset_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "secretsmanager:GetSecretValue"
      ]
      Resource = [
        "arn:aws:secretsmanager:*:*:secret:${var.project_name}/${var.environment}/superset/*"
      ]
    }]
  })
}
