###############################################################################
# Debezium Module - IAM Roles and Policies
# Grants access to MSK/Kafka, S3 for offset storage, Secrets Manager, and
# CloudWatch for monitoring
###############################################################################

# ---------- ECS Task Execution Role ----------
resource "aws_iam_role" "debezium_execution" {
  name = "${var.project_name}-${var.environment}-debezium-execution-role"

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

resource "aws_iam_role_policy_attachment" "debezium_execution" {
  role       = aws_iam_role.debezium_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "debezium_execution_extras" {
  name = "debezium-execution-extras"
  role = aws_iam_role.debezium_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = compact([
          var.kafka_sasl_secret_arn
        ])
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
resource "aws_iam_role" "debezium_task" {
  name = "${var.project_name}-${var.environment}-debezium-task-role"

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

# S3 access for connector offset backup and schema history
resource "aws_iam_role_policy" "debezium_s3" {
  name = "debezium-s3-access"
  role = aws_iam_role.debezium_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = ["*"]
    }]
  })
}

# MSK/Kafka IAM authentication (if using IAM auth with MSK)
resource "aws_iam_role_policy" "debezium_msk" {
  name = "debezium-msk-access"
  role = aws_iam_role.debezium_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "MSKConnect"
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster"
        ]
        Resource = "*"
      },
      {
        Sid    = "MSKTopics"
        Effect = "Allow"
        Action = [
          "kafka-cluster:CreateTopic",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:AlterTopic",
          "kafka-cluster:ReadData",
          "kafka-cluster:WriteData"
        ]
        Resource = "*"
      },
      {
        Sid    = "MSKGroups"
        Effect = "Allow"
        Action = [
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeGroup"
        ]
        Resource = "*"
      }
    ]
  })
}

# Secrets Manager for database credentials
resource "aws_iam_role_policy" "debezium_secrets" {
  name = "debezium-secrets-read"
  role = aws_iam_role.debezium_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "secretsmanager:GetSecretValue"
      ]
      Resource = [
        "arn:aws:secretsmanager:*:*:secret:${var.project_name}/${var.environment}/aurora/*",
        "arn:aws:secretsmanager:*:*:secret:${var.project_name}/${var.environment}/debezium/*"
      ]
    }]
  })
}

# CloudWatch metrics for connector monitoring
resource "aws_iam_role_policy" "debezium_cloudwatch" {
  name = "debezium-cloudwatch"
  role = aws_iam_role.debezium_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "cloudwatch:PutMetricData",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      Resource = "*"
    }]
  })
}
