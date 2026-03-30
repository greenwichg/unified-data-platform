###############################################################################
# Flink Module - Amazon Managed Service for Apache Flink
# Replaces self-hosted Flink on ECS with fully managed service
# Used by Pipeline 1 (CEP), Pipeline 2 (CDC), and Pipeline 4 (Real-time Events)
###############################################################################

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  name_prefix = "${var.project_name}-${var.environment}"

  flink_applications = {
    "pipeline1-cep" = {
      description          = "Batch CEP processing"
      runtime_environment  = var.runtime_environment
      parallelism          = var.parallelism_override > 0 ? var.parallelism_override : 8
      parallelism_per_kpu  = 2
      auto_scaling_enabled = var.auto_scaling_enabled
      code_s3_bucket       = var.code_s3_bucket
      code_s3_key          = var.application_configs["pipeline1-cep"].code_s3_path
      environment_properties = {
        "kafka" = {
          "bootstrap.servers"   = var.kafka_bootstrap_servers
          "schema.registry.url" = var.schema_registry_url
        }
        "s3" = {
          "checkpoint.bucket" = var.s3_checkpoints_bucket
          "checkpoint.prefix" = "pipeline1-cep/flink-checkpoints"
          "output.bucket"     = var.s3_output_bucket
          "output.prefix"     = "pipeline1-batch-etl/iceberg"
        }
        "iceberg" = {
          "catalog.type" = "hive"
          "catalog.name" = "zomato_iceberg"
          "warehouse"    = "s3://${var.s3_output_bucket}/pipeline1-batch-etl/iceberg"
        }
      }
    }

    "pipeline2-cdc" = {
      description          = "CDC stream processing"
      runtime_environment  = var.runtime_environment
      parallelism          = var.parallelism_override > 0 ? var.parallelism_override : 16
      parallelism_per_kpu  = 2
      auto_scaling_enabled = var.auto_scaling_enabled
      code_s3_bucket       = var.code_s3_bucket
      code_s3_key          = var.application_configs["pipeline2-cdc"].code_s3_path
      environment_properties = {
        "kafka" = {
          "bootstrap.servers"   = var.kafka_bootstrap_servers
          "schema.registry.url" = var.schema_registry_url
          "consumer.group.id"   = "flink-cdc-processor"
        }
        "s3" = {
          "checkpoint.bucket" = var.s3_checkpoints_bucket
          "checkpoint.prefix" = "pipeline2-cdc/flink-checkpoints"
          "output.bucket"     = var.s3_output_bucket
          "output.prefix"     = "pipeline2-cdc/iceberg"
        }
        "iceberg" = {
          "catalog.type" = "hive"
          "catalog.name" = "zomato_iceberg"
          "warehouse"    = "s3://${var.s3_output_bucket}/pipeline2-cdc/iceberg"
          "database"     = "zomato_cdc"
        }
      }
    }

    "pipeline4-realtime" = {
      description          = "Realtime event processing"
      runtime_environment  = var.runtime_environment
      parallelism          = var.parallelism_override > 0 ? var.parallelism_override : 32
      parallelism_per_kpu  = 4
      auto_scaling_enabled = var.auto_scaling_enabled
      code_s3_bucket       = var.code_s3_bucket
      code_s3_key          = var.application_configs["pipeline4-realtime"].code_s3_path
      environment_properties = {
        "kafka" = {
          "bootstrap.servers"   = var.kafka_bootstrap_servers
          "schema.registry.url" = var.schema_registry_url
          "consumer.group.id"   = "flink-realtime-events"
        }
        "s3" = {
          "checkpoint.bucket" = var.s3_checkpoints_bucket
          "checkpoint.prefix" = "pipeline4-realtime/flink-checkpoints"
          "output.bucket"     = var.s3_output_bucket
          "output.prefix"     = "pipeline4-realtime/orc"
        }
        "iceberg" = {
          "catalog.type" = "hive"
          "catalog.name" = "zomato_iceberg"
          "warehouse"    = "s3://${var.s3_output_bucket}/pipeline4-realtime/iceberg"
        }
      }
    }
  }
}

# ---------- Security Group ----------
resource "aws_security_group" "flink" {
  name_prefix = "${local.name_prefix}-managed-flink-"
  vpc_id      = var.vpc_id

  # Managed Flink needs outbound access to MSK, S3, Glue, CloudWatch
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound traffic for Managed Flink"
  }

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-managed-flink-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# Allow Managed Flink to connect to MSK/Kafka
resource "aws_security_group_rule" "kafka_from_flink" {
  type                     = "ingress"
  from_port                = 9092
  to_port                  = 9098
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.flink.id
  security_group_id        = var.kafka_security_group_id
  description              = "MSK access from Amazon Managed Flink"
}

# MSK/Kafka cluster access
resource "aws_iam_role_policy" "flink_msk" {
  name = "managed-flink-msk-access"
  role = aws_iam_role.flink.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "MSKClusterAccess"
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster",
          "kafka-cluster:DescribeClusterV2",
          "kafka-cluster:AlterCluster",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:CreateTopic",
          "kafka-cluster:AlterTopic",
          "kafka-cluster:WriteData",
          "kafka-cluster:ReadData",
          "kafka-cluster:DescribeGroup",
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DeleteGroup"
        ]
        Resource = "*"
      }
    ]
  })
}

# Glue catalog access for Iceberg tables
resource "aws_iam_role_policy" "flink_glue" {
  name = "managed-flink-glue-access"
  role = aws_iam_role.flink.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:BatchGetPartition",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchCreatePartition"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*"
        ]
      }
    ]
  })
}

# CloudWatch logs and metrics
resource "aws_iam_role_policy" "flink_cloudwatch" {
  name = "managed-flink-cloudwatch-access"
  role = aws_iam_role.flink.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogsMetrics"
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = "*"
      }
    ]
  })
}

# VPC access for Managed Flink to reach MSK in VPC
resource "aws_iam_role_policy" "flink_vpc" {
  name = "managed-flink-vpc-access"
  role = aws_iam_role.flink.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "VPCAccess"
        Effect = "Allow"
        Action = [
          "ec2:DescribeVpcs",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeNetworkInterfaces",
          "ec2:CreateNetworkInterface",
          "ec2:CreateNetworkInterfacePermission",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeDhcpOptions"
        ]
        Resource = "*"
      }
    ]
  })
}

# ---------- CloudWatch Log Groups ----------
resource "aws_cloudwatch_log_group" "flink" {
  for_each = local.flink_applications

  name              = "/aws/managed-flink/${local.name_prefix}-${each.key}"
  retention_in_days = 30
  tags              = var.tags
}

resource "aws_cloudwatch_log_stream" "flink" {
  for_each = local.flink_applications

  name           = "${each.key}-log-stream"
  log_group_name = aws_cloudwatch_log_group.flink[each.key].name
}

# ---------- Amazon Managed Service for Apache Flink Applications ----------
resource "aws_kinesisanalyticsv2_application" "flink" {
  for_each = local.flink_applications

  name                   = "${local.name_prefix}-${each.key}"
  description            = each.value.description
  runtime_environment    = each.value.runtime_environment
  service_execution_role = aws_iam_role.flink.arn

  application_configuration {

    flink_application_configuration {

      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = 60000
        min_pause_between_checkpoints = 30000
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        metrics_level      = "TASK"
        log_level          = "INFO"
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = each.value.parallelism
        parallelism_per_kpu  = each.value.parallelism_per_kpu
        auto_scaling_enabled = each.value.auto_scaling_enabled
      }
    }

    application_code_configuration {
      code_content_type = "ZIPFILE"

      code_content {
        s3_content_location {
          bucket_arn = "arn:aws:s3:::${each.value.code_s3_bucket}"
          file_key   = each.value.code_s3_key
        }
      }
    }

    environment_properties {
      dynamic "property_group" {
        for_each = each.value.environment_properties

        content {
          property_group_id = property_group.key

          property_map = property_group.value
        }
      }
    }

    vpc_configuration {
      subnet_ids         = var.subnet_ids
      security_group_ids = [aws_security_group.flink.id]
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${aws_cloudwatch_log_group.flink[each.key].name}:log-stream:${aws_cloudwatch_log_stream.flink[each.key].name}"
  }

  tags = merge(var.tags, {
    Name     = "${local.name_prefix}-${each.key}"
    Pipeline = each.key
  })
}

# ---------- Application Snapshots for State Management ----------
resource "aws_kinesisanalyticsv2_application_snapshot" "flink" {
  for_each = local.flink_applications

  application_name = aws_kinesisanalyticsv2_application.flink[each.key].name
  snapshot_name    = "${each.key}-baseline-snapshot"
}
