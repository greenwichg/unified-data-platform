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
