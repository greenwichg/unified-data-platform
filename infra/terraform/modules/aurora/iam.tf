###############################################################################
# Aurora MySQL Module - IAM Roles and Policies
###############################################################################

# ---------- RDS Enhanced Monitoring Role ----------
resource "aws_iam_role" "aurora_monitoring" {
  name = "${var.project_name}-${var.environment}-aurora-monitoring-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "monitoring.rds.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "aurora_monitoring" {
  role       = aws_iam_role.aurora_monitoring.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

# ---------- Aurora S3 Export Role ----------
resource "aws_iam_role" "aurora_s3_export" {
  name = "${var.project_name}-${var.environment}-aurora-s3-export-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "rds.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "aurora_s3_export" {
  name = "aurora-s3-export"
  role = aws_iam_role.aurora_s3_export.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:AbortMultipartUpload"
      ]
      Resource = ["*"]
    }]
  })
}

resource "aws_rds_cluster_role_association" "s3_export" {
  db_cluster_identifier = aws_rds_cluster.main.id
  feature_name          = "s3Export"
  role_arn              = aws_iam_role.aurora_s3_export.arn
}
