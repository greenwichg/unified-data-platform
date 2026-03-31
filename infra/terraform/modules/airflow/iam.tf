###############################################################################
# Airflow Module - IAM Roles and Policies
###############################################################################

resource "aws_iam_role" "airflow" {
  name = "${var.project_name}-${var.environment}-airflow-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "airflow_execution" {
  name = "airflow-execution-policy"
  role = aws_iam_role.airflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:*"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_dags_bucket}",
          "arn:aws:s3:::${var.s3_dags_bucket}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups"
        ]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "airflow:PublishMetrics"
        ]
        Resource = "arn:aws:airflow:*:*:environment/${var.project_name}-${var.environment}-airflow"
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:DescribeLogStreams",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetQueryResults"
        ]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetEncryptionConfiguration"
        ]
        Resource = "arn:aws:s3:::${var.s3_dags_bucket}"
      },
      {
        Effect = "Allow"
        Action = [
          "emr:*",
          "ecs:*",
          "ec2:Describe*"
        ]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role_policy" "airflow_sqs" {
  name = "airflow-sqs-access"
  role = aws_iam_role.airflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "sqs:ChangeMessageVisibility",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
        "sqs:GetQueueUrl",
        "sqs:ReceiveMessage",
        "sqs:SendMessage"
      ]
      Resource = "arn:aws:sqs:*:*:airflow-celery-*"
    }]
  })
}

resource "aws_iam_role_policy" "airflow_kms" {
  name = "airflow-kms-access"
  role = aws_iam_role.airflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "kms:Decrypt",
        "kms:DescribeKey",
        "kms:GenerateDataKey*",
        "kms:Encrypt"
      ]
      Resource = "*"
      Condition = {
        StringLike = {
          "kms:ViaService" = "sqs.*.amazonaws.com"
        }
      }
    }]
  })
}
