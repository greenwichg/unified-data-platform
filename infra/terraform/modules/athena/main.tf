###############################################################################
# Athena Module - Serverless Query Engine (replacing self-hosted Trino)
# Amazon Athena is Trino-based and serverless. Three workgroups replace the
# three ECS-based Trino clusters: adhoc, etl, reporting.
# Glue Data Catalog replaces the self-hosted Hive Metastore.
###############################################################################

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# ---------- Athena Workgroup Definitions ----------
locals {
  athena_workgroups = {
    adhoc = {
      description            = "Ad-hoc queries from analysts"
      bytes_scanned_cutoff   = 10737418240 # 10 GB
      per_query_memory_mb    = 4096        # 4 GB
      concurrent_query_limit = 50
    }
    etl = {
      description            = "ETL queries from Airflow"
      bytes_scanned_cutoff   = 107374182400 # 100 GB
      per_query_memory_mb    = 8192         # 8 GB
      concurrent_query_limit = 20
    }
    reporting = {
      description            = "Dashboard and reporting queries"
      bytes_scanned_cutoff   = 53687091200 # 50 GB
      per_query_memory_mb    = 6144        # 6 GB
      concurrent_query_limit = 100
    }
  }
}

# ---------- Athena Workgroups ----------
resource "aws_athena_workgroup" "this" {
  for_each = local.athena_workgroups
  name     = "${var.project_name}-${var.environment}-${each.key}"

  description = each.value.description
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    bytes_scanned_cutoff_per_query = each.value.bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${var.result_bucket}/${var.project_name}/${var.environment}/athena-results/${each.key}/"

      encryption_configuration {
        encryption_option = var.encryption_type
      }
    }
  }

  tags = merge(var.tags, {
    Name         = "${var.project_name}-${var.environment}-athena-${each.key}"
    WorkgroupUse = each.key
  })
}

# ---------- Glue Data Catalog (replaces Hive Metastore) ----------
resource "aws_athena_data_catalog" "glue" {
  name        = "${var.project_name}-${var.environment}-glue-catalog"
  description = "AWS Glue Data Catalog (replaces self-hosted Hive Metastore)"
  type        = "GLUE"

  parameters = {
    "catalog-id" = data.aws_caller_identity.current.account_id
  }

  tags = var.tags
}

# ---------- Glue Catalog Databases (replaces Hive Metastore schemas) ----------
resource "aws_glue_catalog_database" "zomato_raw" {
  name        = "zomato_raw"
  catalog_id  = data.aws_caller_identity.current.account_id
  description = "Raw ingestion layer - landing zone for Kafka/Flink data"

  location_uri = "s3://${var.result_bucket}/raw/"
}

resource "aws_glue_catalog_database" "zomato_curated" {
  name        = "zomato_curated"
  catalog_id  = data.aws_caller_identity.current.account_id
  description = "Curated layer - cleaned, deduped, enriched data"

  location_uri = "s3://${var.result_bucket}/curated/"
}

resource "aws_glue_catalog_database" "zomato_gold" {
  name        = "zomato_gold"
  catalog_id  = data.aws_caller_identity.current.account_id
  description = "Gold layer - pre-aggregated metrics and KPIs for dashboards"

  location_uri = "s3://${var.result_bucket}/gold/"
}

# ---------- Athena Named Queries (common saved queries) ----------
resource "aws_athena_named_query" "daily_gmv_report" {
  name        = "daily-gmv-report"
  workgroup   = aws_athena_workgroup.this["reporting"].name
  database    = aws_glue_catalog_database.zomato_gold.name
  description = "Daily GMV summary with city breakdown and day-over-day comparison"

  query = <<-SQL
    SELECT metric_date, city_name, SUM(gross_merchandise_value) AS gmv,
           SUM(total_orders) AS total_orders, SUM(completed_orders) AS completed_orders
    FROM daily_order_metrics
    WHERE metric_date = CURRENT_DATE
    GROUP BY metric_date, city_name
    ORDER BY gmv DESC
  SQL
}

resource "aws_athena_named_query" "delivery_sla_check" {
  name        = "delivery-sla-check"
  workgroup   = aws_athena_workgroup.this["reporting"].name
  database    = aws_glue_catalog_database.zomato_gold.name
  description = "Current day delivery SLA breach rates by city"

  query = <<-SQL
    SELECT metric_date, city_name,
           SUM(total_deliveries) AS total_deliveries,
           ROUND(SUM(sla_breach_count) * 100.0 / NULLIF(SUM(total_deliveries), 0), 2) AS sla_breach_pct
    FROM delivery_metrics
    WHERE metric_date = CURRENT_DATE AND metric_hour IS NOT NULL
    GROUP BY metric_date, city_name
    ORDER BY sla_breach_pct DESC
  SQL
}

resource "aws_athena_named_query" "active_user_count" {
  name        = "active-user-count"
  workgroup   = aws_athena_workgroup.this["adhoc"].name
  database    = aws_glue_catalog_database.zomato_gold.name
  description = "Count of active users by segment from latest cohort snapshot"

  query = <<-SQL
    SELECT user_segment, lifecycle_stage, COUNT(DISTINCT user_id) AS user_count
    FROM user_cohorts
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM user_cohorts)
    GROUP BY user_segment, lifecycle_stage
    ORDER BY user_count DESC
  SQL
}

# ---------- IAM Role for Athena ----------
resource "aws_iam_role" "athena" {
  name = "${var.project_name}-${var.environment}-athena-role"

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

resource "aws_iam_role_policy" "athena_access" {
  name = "athena-data-access"
  role = aws_iam_role.athena.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3DataLakeAccess"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation",
        "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "arn:aws:s3:::${var.result_bucket}",
          "arn:aws:s3:::${var.result_bucket}/*"
        ]
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = ["glue:GetDatabase", "glue:GetDatabases",
          "glue:GetTable", "glue:GetTables", "glue:GetPartition",
          "glue:GetPartitions", "glue:BatchGetPartition",
          "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
          "glue:CreatePartition", "glue:UpdatePartition",
          "glue:DeletePartition", "glue:BatchCreatePartition",
        "glue:BatchDeletePartition"]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*"
        ]
      },
      {
        Sid    = "AthenaAccess"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:StopQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetWorkGroup",
          "athena:ListNamedQueries",
          "athena:GetNamedQuery"
        ]
        Resource = ["*"]
      }
    ]
  })
}
