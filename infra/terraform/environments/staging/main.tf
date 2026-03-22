###############################################################################
# Staging Environment - Zomato Data Platform
# Pre-production validation with medium-scale resources
###############################################################################

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.30"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "zomato-data-platform"
      Environment = "staging"
      ManagedBy   = "terraform"
    }
  }
}

locals {
  project_name = "zomato-data-platform"
  tags = {
    Project     = local.project_name
    Environment = var.environment
  }
}

# ===================== VPC =====================
module "vpc" {
  source             = "../../modules/vpc"
  environment        = var.environment
  vpc_cidr           = var.vpc_cidr
  availability_zones = var.availability_zones
  tags               = local.tags
}

# ===================== S3 Data Lake =====================
module "s3" {
  source      = "../../modules/s3"
  environment = var.environment
  tags        = local.tags
}

# ===================== Aurora MySQL =====================
module "aurora" {
  source         = "../../modules/aurora"
  environment    = var.environment
  vpc_id         = module.vpc.vpc_id
  subnet_ids     = module.vpc.data_subnet_ids
  instance_class = var.aurora_instance_class
  instance_count = var.aurora_instance_count
  tags           = local.tags
}

# ===================== DynamoDB =====================
module "dynamodb" {
  source      = "../../modules/dynamodb"
  environment = var.environment
  tags        = local.tags
}

# ===================== Amazon MSK (replacing self-hosted Kafka on EC2) =====================
module "kafka" {
  source          = "../../modules/kafka"
  environment     = var.environment
  vpc_id          = module.vpc.vpc_id
  subnet_ids      = module.vpc.private_subnet_ids
  instance_type   = var.kafka_instance_type
  broker_count    = var.kafka_broker_count
  ebs_volume_size = var.kafka_ebs_volume_size
  tags            = local.tags
}

# ===================== Amazon MSK Secondary (Pipeline 4 -> Druid ingestion) =====================
module "kafka_secondary" {
  source              = "../../modules/kafka"
  project_name        = "${local.project_name}-realtime"
  environment         = var.environment
  vpc_id              = module.vpc.vpc_id
  subnet_ids          = module.vpc.private_subnet_ids
  instance_type       = "kafka.r8g.xlarge"
  number_of_brokers   = 3
  ebs_volume_size     = 500
  enhanced_monitoring = "PER_TOPIC_PER_BROKER"
  tags = merge(local.tags, {
    Cluster = "secondary"
    Purpose = "druid-ingestion"
  })
}

# ===================== Kafka Consumer Fleet (EC2 ASG: MSK 1 -> MSK 2 -> Druid) =====================
module "kafka_consumer_fleet" {
  source                            = "../../modules/kafka_consumer_fleet"
  environment                       = var.environment
  vpc_id                            = module.vpc.vpc_id
  subnet_ids                        = module.vpc.private_subnet_ids
  instance_type                     = "r8g.large"
  desired_capacity                  = 3
  min_size                          = 2
  max_size                          = 8
  kafka_primary_security_group_id   = module.kafka.security_group_id
  kafka_secondary_security_group_id = module.kafka_secondary.security_group_id
  kafka_primary_cluster_arn         = module.kafka.cluster_arn
  kafka_secondary_cluster_arn       = module.kafka_secondary.cluster_arn
  kafka_primary_bootstrap_servers   = module.kafka.bootstrap_brokers_iam
  kafka_secondary_bootstrap_servers = module.kafka_secondary.bootstrap_brokers_iam
  tags                              = local.tags
}

# ===================== Flink (ECS) =====================
module "flink" {
  source                  = "../../modules/flink"
  environment             = var.environment
  vpc_id                  = module.vpc.vpc_id
  subnet_ids              = module.vpc.private_subnet_ids
  kafka_security_group_id = module.kafka.security_group_id
  s3_checkpoints_bucket   = module.s3.checkpoints_bucket_name
  s3_output_bucket        = module.s3.raw_bucket_name
  tags                    = local.tags
}

# ===================== EMR (Spark) =====================
module "emr" {
  source               = "../../modules/emr"
  environment          = var.environment
  vpc_id               = module.vpc.vpc_id
  subnet_id            = module.vpc.private_subnet_ids[0]
  s3_log_bucket        = module.s3.airflow_logs_bucket_name
  s3_output_bucket     = module.s3.raw_bucket_name
  master_instance_type = var.emr_master_instance_type
  core_instance_type   = var.emr_core_instance_type
  core_instance_count  = var.emr_core_instance_count
  tags                 = local.tags
}

# ===================== Athena (serverless, replacing self-hosted Trino on ECS) =====================
# Glue Data Catalog replaces the self-hosted Hive Metastore.
module "trino" {
  source      = "../../modules/athena"
  environment = var.environment
  vpc_id      = module.vpc.vpc_id
  subnet_ids  = module.vpc.private_subnet_ids
  tags        = local.tags
}

# ===================== Druid (Real-time OLAP) =====================
module "druid" {
  source                 = "../../modules/druid"
  environment            = var.environment
  vpc_id                 = module.vpc.vpc_id
  subnet_ids             = module.vpc.private_subnet_ids
  s3_deep_storage_bucket = module.s3.raw_bucket_name
  tags                   = local.tags
}

# ===================== ECS (Debezium / Services) =====================
module "ecs" {
  source      = "../../modules/ecs"
  environment = var.environment
  vpc_id      = module.vpc.vpc_id
  subnet_ids  = module.vpc.private_subnet_ids
  tags        = local.tags
}

# ===================== Airflow (MWAA) =====================
module "airflow" {
  source         = "../../modules/airflow"
  environment    = var.environment
  vpc_id         = module.vpc.vpc_id
  subnet_ids     = module.vpc.private_subnet_ids
  s3_dags_bucket = module.s3.airflow_logs_bucket_name
  tags           = local.tags
}

# ===================== Monitoring =====================
module "monitoring" {
  source      = "../../modules/monitoring"
  environment = var.environment
  tags        = local.tags
}
