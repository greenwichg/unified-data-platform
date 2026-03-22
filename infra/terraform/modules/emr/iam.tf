###############################################################################
# EMR Module - IAM Roles and Policies
###############################################################################

# ---------- EMR Service Role ----------
resource "aws_iam_role" "emr_service" {
  name = "${var.project_name}-${var.environment}-emr-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "elasticmapreduce.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

# ---------- EMR EC2 Role ----------
resource "aws_iam_role" "emr_ec2" {
  name = "${var.project_name}-${var.environment}-emr-ec2-role"

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

resource "aws_iam_role_policy" "emr_ec2_s3" {
  name = "emr-s3-dynamodb-access"
  role = aws_iam_role.emr_ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_output_bucket}",
          "arn:aws:s3:::${var.s3_output_bucket}/*",
          "arn:aws:s3:::${var.s3_log_bucket}",
          "arn:aws:s3:::${var.s3_log_bucket}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:DescribeTable",
          "dynamodb:Scan",
          "dynamodb:Query",
          "dynamodb:GetItem",
          "dynamodb:DescribeStream",
          "dynamodb:GetRecords",
          "dynamodb:GetShardIterator",
          "dynamodb:ListStreams"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "emr_ec2_cloudwatch" {
  name = "emr-cloudwatch"
  role = aws_iam_role.emr_ec2.id

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

resource "aws_iam_instance_profile" "emr_ec2" {
  name = "${var.project_name}-${var.environment}-emr-ec2-profile"
  role = aws_iam_role.emr_ec2.name
}

# ---------- EMR Auto Scaling Role ----------
resource "aws_iam_role" "emr_autoscaling" {
  name = "${var.project_name}-${var.environment}-emr-autoscaling-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = ["elasticmapreduce.amazonaws.com", "application-autoscaling.amazonaws.com"]
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "emr_autoscaling" {
  role       = aws_iam_role.emr_autoscaling.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforAutoScalingRole"
}
