###############################################################################
# EMR Module - Spark on EMR for Pipeline 3 (DynamoDB Streams → S3)
###############################################################################

variable "project_name" {
  type    = string
  default = "zomato-data-platform"
}

variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "subnet_id" {
  description = "Subnet for EMR cluster"
  type        = string
}

variable "s3_log_bucket" {
  type = string
}

variable "s3_output_bucket" {
  type = string
}

variable "master_instance_type" {
  type    = string
  default = "r6g.2xlarge"
}

variable "core_instance_type" {
  type    = string
  default = "r6g.4xlarge"
}

variable "core_instance_count" {
  type    = number
  default = 5
}

variable "tags" {
  type    = map(string)
  default = {}
}

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

# ---------- IAM Roles ----------
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

resource "aws_iam_instance_profile" "emr_ec2" {
  name = "${var.project_name}-${var.environment}-emr-ec2-profile"
  role = aws_iam_role.emr_ec2.name
}

# ---------- EMR Cluster ----------
resource "aws_emr_cluster" "spark" {
  name          = "${var.project_name}-${var.environment}-spark-emr"
  release_label = "emr-7.0.0"
  applications  = ["Spark", "Hadoop", "Hive"]
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
            AdjustmentType = "CHANGE_IN_CAPACITY"
            ScalingAdjustment = 2
            CoolDown = 300
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
        "spark.dynamicAllocation.enabled"          = "true"
        "spark.shuffle.service.enabled"            = "true"
        "spark.sql.orc.enabled"                    = "true"
        "spark.sql.hive.convertMetastoreOrc"       = "true"
        "spark.serializer"                         = "org.apache.spark.serializer.KryoSerializer"
        "spark.sql.adaptive.enabled"               = "true"
        "spark.sql.adaptive.coalescePartitions.enabled" = "true"
      }
    },
    {
      Classification = "spark-hive-site"
      Properties = {
        "hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    }
  ])

  log_uri = "s3://${var.s3_log_bucket}/emr-logs/"

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-spark-emr"
  })
}

# ---------- Outputs ----------
output "cluster_id" {
  value = aws_emr_cluster.spark.id
}

output "master_public_dns" {
  value = aws_emr_cluster.spark.master_public_dns
}
