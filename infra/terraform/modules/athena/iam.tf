###############################################################################
# Athena Module - IAM Roles and Policies
# Migrated from Trino on ECS to serverless Amazon Athena.
# ECS task execution role replaced by Athena service role.
# Glue Data Catalog access replaces Hive Metastore connectivity.
###############################################################################

resource "aws_iam_role" "athena_service" {
  name = "${var.project_name}-${var.environment}-athena-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "athena.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "athena_s3" {
  name = "athena-data-access"
  role = aws_iam_role.athena_service.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "S3DataLakeRead"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = ["*"]
      },
      {
        Sid    = "S3AthenaResultsWrite"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.project_name}-${var.environment}-athena-results",
          "arn:aws:s3:::${var.project_name}-${var.environment}-athena-results/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "athena_glue_catalog" {
  name = "athena-glue-catalog-access"
  role = aws_iam_role.athena_service.id

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
        "glue:GetPartitions",
        "glue:BatchGetPartition",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:CreatePartition",
        "glue:UpdatePartition",
        "glue:DeletePartition",
        "glue:BatchCreatePartition",
        "glue:BatchDeletePartition"
      ]
      Resource = ["*"]
    }]
  })
}

resource "aws_iam_role_policy" "athena_cloudwatch" {
  name = "athena-cloudwatch"
  role = aws_iam_role.athena_service.id

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
