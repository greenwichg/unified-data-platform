###############################################################################
# Hive Metastore Module - IAM Roles and Policies
# Grants access to S3 data lake, AWS Glue catalog, Secrets Manager, and ECR
###############################################################################

# ---------- ECS Task Execution Role ----------
resource "aws_iam_role" "hive_metastore_execution" {
  name = "${var.project_name}-${var.environment}-hive-metastore-execution-role"

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

resource "aws_iam_role_policy_attachment" "hive_metastore_execution" {
  role       = aws_iam_role.hive_metastore_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "hive_metastore_execution_secrets" {
  name = "hive-metastore-execution-secrets"
  role = aws_iam_role.hive_metastore_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "secretsmanager:GetSecretValue"
      ]
      Resource = [
        aws_rds_cluster.hive_metastore.master_user_secret[0].secret_arn
      ]
    }]
  })
}

# ---------- Hive Metastore Task Role - S3 Access ----------
resource "aws_iam_role_policy" "hive_metastore_s3" {
  name = "hive-metastore-s3-access"
  role = aws_iam_role.hive_metastore.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3DataLakeRead"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_warehouse_bucket}",
          "arn:aws:s3:::${var.s3_warehouse_bucket}/*"
        ]
      },
      {
        Sid    = "S3DataLakeWrite"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_warehouse_bucket}/warehouse/*"
        ]
      }
    ]
  })
}

# ---------- Hive Metastore Task Role - Glue Catalog Access ----------
resource "aws_iam_role_policy" "hive_metastore_glue" {
  name = "hive-metastore-glue-catalog"
  role = aws_iam_role.hive_metastore.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "GlueCatalogAccess"
      Effect = "Allow"
      Action = [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:CreateDatabase",
        "glue:UpdateDatabase",
        "glue:DeleteDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:BatchDeleteTable",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:CreatePartition",
        "glue:UpdatePartition",
        "glue:DeletePartition",
        "glue:BatchCreatePartition",
        "glue:BatchDeletePartition",
        "glue:BatchGetPartition",
        "glue:GetUserDefinedFunction",
        "glue:GetUserDefinedFunctions",
        "glue:CreateUserDefinedFunction",
        "glue:UpdateUserDefinedFunction",
        "glue:DeleteUserDefinedFunction"
      ]
      Resource = [
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*"
      ]
    }]
  })
}

# ---------- Hive Metastore Task Role - CloudWatch ----------
resource "aws_iam_role_policy" "hive_metastore_cloudwatch" {
  name = "hive-metastore-cloudwatch"
  role = aws_iam_role.hive_metastore.id

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

data "aws_caller_identity" "current" {}
