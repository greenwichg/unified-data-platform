###############################################################################
# S3 Module - IAM Roles and Policies
###############################################################################

# ---------- Data Lake Read-Only Role ----------
resource "aws_iam_role" "data_lake_readonly" {
  name = "${var.project_name}-${var.environment}-s3-readonly-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = ["ecs-tasks.amazonaws.com", "ec2.amazonaws.com"]
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "data_lake_readonly" {
  name = "data-lake-readonly"
  role = aws_iam_role.data_lake_readonly.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ]
      Resource = [
        aws_s3_bucket.data_lake_raw.arn,
        "${aws_s3_bucket.data_lake_raw.arn}/*",
        aws_s3_bucket.data_lake_processed.arn,
        "${aws_s3_bucket.data_lake_processed.arn}/*"
      ]
    }]
  })
}

# ---------- Data Lake Read-Write Role ----------
resource "aws_iam_role" "data_lake_readwrite" {
  name = "${var.project_name}-${var.environment}-s3-readwrite-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = ["ecs-tasks.amazonaws.com", "ec2.amazonaws.com"]
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "data_lake_readwrite" {
  name = "data-lake-readwrite"
  role = aws_iam_role.data_lake_readwrite.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ]
      Resource = [
        aws_s3_bucket.data_lake_raw.arn,
        "${aws_s3_bucket.data_lake_raw.arn}/*",
        aws_s3_bucket.data_lake_processed.arn,
        "${aws_s3_bucket.data_lake_processed.arn}/*",
        aws_s3_bucket.checkpoints.arn,
        "${aws_s3_bucket.checkpoints.arn}/*"
      ]
    }]
  })
}

# ---------- S3 Replication Role ----------
resource "aws_iam_role" "s3_replication" {
  name = "${var.project_name}-${var.environment}-s3-replication-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "s3.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "s3_replication" {
  name = "s3-replication"
  role = aws_iam_role.s3_replication.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetReplicationConfiguration",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake_raw.arn,
          aws_s3_bucket.data_lake_processed.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObjectVersionForReplication",
          "s3:GetObjectVersionAcl",
          "s3:GetObjectVersionTagging"
        ]
        Resource = [
          "${aws_s3_bucket.data_lake_raw.arn}/*",
          "${aws_s3_bucket.data_lake_processed.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ReplicateObject",
          "s3:ReplicateDelete",
          "s3:ReplicateTags"
        ]
        Resource = "*"
      }
    ]
  })
}
