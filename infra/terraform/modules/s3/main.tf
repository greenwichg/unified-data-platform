###############################################################################
# S3 Module - Data Lake Storage for Zomato Data Platform
# All pipelines converge to S3 with ORC format + Iceberg tables
###############################################################################

# ---------- Raw Data Lake Bucket ----------
resource "aws_s3_bucket" "data_lake_raw" {
  bucket = "${var.project_name}-${var.environment}-raw-data-lake"

  tags = merge(var.tags, {
    Name    = "${var.project_name}-${var.environment}-raw-data-lake"
    Purpose = "Raw ingestion data from all pipelines"
  })
}

resource "aws_s3_bucket_versioning" "data_lake_raw" {
  bucket = aws_s3_bucket.data_lake_raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_raw" {
  bucket = aws_s3_bucket.data_lake_raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake_raw" {
  bucket = aws_s3_bucket.data_lake_raw.id

  rule {
    id     = "transition-to-ia"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# ---------- Processed Data Lake Bucket (ORC/Iceberg) ----------
resource "aws_s3_bucket" "data_lake_processed" {
  bucket = "${var.project_name}-${var.environment}-processed-data-lake"

  tags = merge(var.tags, {
    Name    = "${var.project_name}-${var.environment}-processed-data-lake"
    Purpose = "Processed ORC/Iceberg data for Athena queries"
  })
}

resource "aws_s3_bucket_versioning" "data_lake_processed" {
  bucket = aws_s3_bucket.data_lake_processed.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_processed" {
  bucket = aws_s3_bucket.data_lake_processed.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake_processed" {
  bucket = aws_s3_bucket.data_lake_processed.id

  rule {
    id     = "intelligent-tiering"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "INTELLIGENT_TIERING"
    }
  }

  rule {
    id     = "archive-old-partitions"
    status = "Enabled"

    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }
}

# ---------- Pipeline-specific prefixes (created as objects) ----------
resource "aws_s3_object" "pipeline_prefixes" {
  for_each = toset([
    "pipeline1-batch-etl/spark-jdbc-output/",
    "pipeline1-batch-etl/orc/",
    "pipeline2-cdc/flink-output/",
    "pipeline2-cdc/iceberg/",
    "pipeline3-dynamodb/json-raw/",
    "pipeline3-dynamodb/spark-output/",
    "pipeline3-dynamodb/orc/",
    "pipeline4-realtime/flink-output/",
    "pipeline4-realtime/druid-segments/",
  ])

  bucket  = aws_s3_bucket.data_lake_raw.id
  key     = each.value
  content = ""
}

# ---------- Airflow Logs Bucket ----------
resource "aws_s3_bucket" "airflow_logs" {
  bucket = "${var.project_name}-${var.environment}-airflow-logs"

  tags = merge(var.tags, {
    Name    = "${var.project_name}-${var.environment}-airflow-logs"
    Purpose = "Airflow task logs"
  })
}

resource "aws_s3_bucket_lifecycle_configuration" "airflow_logs" {
  bucket = aws_s3_bucket.airflow_logs.id

  rule {
    id     = "expire-old-logs"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = 90
    }
  }
}

# ---------- Checkpoint Bucket (Flink/Spark) ----------
resource "aws_s3_bucket" "checkpoints" {
  bucket = "${var.project_name}-${var.environment}-checkpoints"

  tags = merge(var.tags, {
    Name    = "${var.project_name}-${var.environment}-checkpoints"
    Purpose = "Flink and Spark checkpoints"
  })
}

resource "aws_s3_bucket_lifecycle_configuration" "checkpoints" {
  bucket = aws_s3_bucket.checkpoints.id

  rule {
    id     = "cleanup-old-checkpoints"
    status = "Enabled"

    expiration {
      days = 7
    }
  }
}

# ---------- Block Public Access (all buckets) ----------
resource "aws_s3_bucket_public_access_block" "data_lake_raw" {
  bucket                  = aws_s3_bucket.data_lake_raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "data_lake_processed" {
  bucket                  = aws_s3_bucket.data_lake_processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "airflow_logs" {
  bucket                  = aws_s3_bucket.airflow_logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "checkpoints" {
  bucket                  = aws_s3_bucket.checkpoints.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

