###############################################################################
# EMR Module - Spark on EMR for Pipeline 3 (DynamoDB Streams → S3)
###############################################################################

# ---------- Security Groups ----------
resource "aws_security_group" "emr_master" {
  name_prefix = "${var.project_name}-${var.environment}-emr-master-"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-emr-master-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "emr_core" {
  name_prefix = "${var.project_name}-${var.environment}-emr-core-"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-emr-core-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- EMR Cluster ----------
resource "aws_emr_cluster" "spark" {
  name          = "${var.project_name}-${var.environment}-spark-emr"
  release_label = "emr-7.0.0"
  applications  = ["Spark", "Hadoop"] # Hive Metastore replaced by AWS Glue Data Catalog
  service_role  = aws_iam_role.emr_service.arn

  ec2_attributes {
    instance_profile                  = aws_iam_instance_profile.emr_ec2.arn
    subnet_id                         = var.subnet_id
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_core.id
  }

  master_instance_group {
    instance_type  = var.master_instance_type
    instance_count = 1

    ebs_config {
      size                 = 256
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  core_instance_group {
    instance_type  = var.core_instance_type
    instance_count = var.core_instance_count
    bid_price      = var.use_spot_instances ? "${var.spot_bid_price_percent}%" : null

    ebs_config {
      size                 = 512
      type                 = "gp3"
      volumes_per_instance = 2
    }

    autoscaling_policy = jsonencode({
      Constraints = {
        MinCapacity = var.core_instance_count
        MaxCapacity = var.core_instance_count * 3
      }
      Rules = [{
        Name = "ScaleOutOnMemory"
        Action = {
          SimpleScalingPolicyConfiguration = {
            AdjustmentType    = "CHANGE_IN_CAPACITY"
            ScalingAdjustment = 2
            CoolDown          = 300
          }
        }
        Trigger = {
          CloudWatchAlarmDefinition = {
            ComparisonOperator = "LESS_THAN"
            MetricName         = "YARNMemoryAvailablePercentage"
            Period             = 300
            Statistic          = "AVERAGE"
            Threshold          = 15.0
            Unit               = "PERCENT"
            Namespace          = "AWS/ElasticMapReduce"
          }
        }
      }]
    })
  }

  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.dynamicAllocation.enabled"               = "true"
        "spark.shuffle.service.enabled"                 = "true"
        "spark.sql.orc.enabled"                         = "true"
        "spark.sql.hive.convertMetastoreOrc"            = "true"
        "spark.serializer"                              = "org.apache.spark.serializer.KryoSerializer"
        "spark.sql.adaptive.enabled"                    = "true"
        "spark.sql.adaptive.coalescePartitions.enabled" = "true"
      }
    },
    {
      Classification = "spark-hive-site"
      Properties = {
        # Using AWS Glue Data Catalog as the metastore (replaces self-hosted Hive Metastore)
        "hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    }
  ])

  log_uri = "s3://${var.s3_log_bucket}/emr-logs/"

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-spark-emr"
  })
}

