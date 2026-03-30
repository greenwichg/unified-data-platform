###############################################################################
# Kafka Module - Amazon MSK (Managed Streaming for Apache Kafka)
# 450M+ messages per minute across all pipelines
###############################################################################

# ---------- Data Sources ----------
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ---------- Security Group ----------
resource "aws_security_group" "kafka" {
  name_prefix = "${var.project_name}-${var.environment}-msk-"
  vpc_id      = var.vpc_id

  # Kafka TLS port
  ingress {
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    self        = true
    description = "MSK TLS communication"
  }

  # Kafka IAM auth port
  ingress {
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    self        = true
    description = "MSK IAM authentication"
  }

  # ZooKeeper port (MSK managed, but clients may need access)
  ingress {
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    self        = true
    description = "ZooKeeper client (MSK managed)"
  }

  # JMX monitoring (MSK open monitoring)
  ingress {
    from_port   = 11001
    to_port     = 11002
    protocol    = "tcp"
    self        = true
    description = "MSK open monitoring (Prometheus JMX and Node exporter)"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-msk-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- CloudWatch Log Group ----------
resource "aws_cloudwatch_log_group" "msk" {
  name              = "/msk/${var.project_name}"
  retention_in_days = 30
  tags              = var.tags
}

# ---------- S3 Bucket for MSK Logs ----------
data "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-${var.environment}-raw"
}

# ---------- MSK Configuration ----------
resource "aws_msk_configuration" "this" {
  name           = "${var.project_name}-${var.environment}-msk-config"
  kafka_versions = [var.kafka_version]
  description    = "MSK configuration for ${var.project_name} ${var.environment}"

  server_properties = <<-PROPERTIES
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=12
    log.retention.hours=168
    message.max.bytes=10485760
    compression.type=producer
    log.cleanup.policy=delete,compact
  PROPERTIES

  tags = var.tags
}

# ---------- MSK Cluster ----------
resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.project_name}-${var.environment}-msk"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.number_of_brokers

  broker_node_group_info {
    instance_type   = var.instance_type
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.kafka.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.ebs_volume_size
        provisioned_throughput {
          enabled           = true
          volume_throughput = 250
        }
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
    # Use AWS managed key for at-rest encryption
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
      s3_logs {
        enabled = true
        bucket  = data.aws_s3_bucket.data_lake.id
        prefix  = "msk-logs/${var.project_name}-${var.environment}"
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  enhanced_monitoring = var.enhanced_monitoring

  tags = merge(var.tags, {
    Name      = "${var.project_name}-${var.environment}-msk"
    Component = "kafka"
  })
}
