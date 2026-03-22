###############################################################################
# Druid Module - IAM Roles and Policies
###############################################################################

resource "aws_iam_role" "druid" {
  name = "${var.project_name}-${var.environment}-druid-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "druid_s3" {
  name = "druid-s3-access"
  role = aws_iam_role.druid.id

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
      Resource = [
        "arn:aws:s3:::${var.s3_deep_storage_bucket}",
        "arn:aws:s3:::${var.s3_deep_storage_bucket}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "druid_cloudwatch" {
  name = "druid-cloudwatch"
  role = aws_iam_role.druid.id

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

resource "aws_iam_role_policy" "druid_ec2_describe" {
  name = "druid-ec2-describe"
  role = aws_iam_role.druid.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "ec2:DescribeInstances",
        "ec2:DescribeTags"
      ]
      Resource = "*"
    }]
  })
}

resource "aws_iam_instance_profile" "druid" {
  name = "${var.project_name}-${var.environment}-druid-profile"
  role = aws_iam_role.druid.name
}
