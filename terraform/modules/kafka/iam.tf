###############################################################################
# Kafka Module - IAM Roles and Policies
###############################################################################

resource "aws_iam_role" "kafka" {
  name = "${var.project_name}-${var.environment}-kafka-role"

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

resource "aws_iam_role_policy" "kafka_s3" {
  name = "kafka-s3-access"
  role = aws_iam_role.kafka.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ]
      Resource = ["*"]
    }]
  })
}

resource "aws_iam_role_policy" "kafka_cloudwatch" {
  name = "kafka-cloudwatch"
  role = aws_iam_role.kafka.id

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

resource "aws_iam_role_policy" "kafka_ec2_describe" {
  name = "kafka-ec2-describe"
  role = aws_iam_role.kafka.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "ec2:DescribeInstances",
        "ec2:DescribeTags",
        "autoscaling:DescribeAutoScalingGroups"
      ]
      Resource = "*"
    }]
  })
}

resource "aws_iam_instance_profile" "kafka" {
  name = "${var.project_name}-${var.environment}-kafka-profile"
  role = aws_iam_role.kafka.name
}
