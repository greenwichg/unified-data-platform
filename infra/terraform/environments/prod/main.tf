###############################################################################
# Production Environment - Zomato Data Platform
# 2M+ orders/day, 450M Kafka msgs/min, 20B events/week
###############################################################################

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.30"
    }
  }

  backend "s3" {
    bucket         = "zomato-terraform-state"
    key            = "prod/terraform.tfstate"
    region         = "ap-south-1"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "zomato-data-platform"
      Environment = "prod"
      ManagedBy   = "terraform"
    }
  }
}

variable "aws_region" {
  type    = string
  default = "ap-south-1"
}

variable "environment" {
  type    = string
  default = "prod"
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
  vpc_cidr           = "10.0.0.0/16"
  availability_zones = ["ap-south-1a", "ap-south-1b", "ap-south-1c"]
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
  instance_class = "db.r6g.4xlarge"
  instance_count = 3
  tags           = local.tags
}

# ===================== DynamoDB =====================
module "dynamodb" {
  source      = "../../modules/dynamodb"
  environment = var.environment
  tags        = local.tags
}

# ===================== Amazon MSK (replacing self-hosted Kafka on EC2) - Production Scale =====================
module "kafka" {
  source          = "../../modules/kafka"
  environment     = var.environment
  vpc_id          = module.vpc.vpc_id
  subnet_ids      = module.vpc.private_subnet_ids
  instance_type   = "kafka.r8g.4xlarge"
  broker_count    = 9
  ebs_volume_size = 2000
  tags            = local.tags
}

# ===================== Amazon MSK Secondary (Pipeline 4 -> Druid ingestion) =====================
module "kafka_secondary" {
  source              = "../../modules/kafka"
  project_name        = "${local.project_name}-realtime"
  environment         = var.environment
  vpc_id              = module.vpc.vpc_id
  subnet_ids          = module.vpc.private_subnet_ids
  instance_type       = "kafka.r8g.2xlarge"
  number_of_brokers   = 6
  ebs_volume_size     = 1000
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
  instance_type                     = "r8g.xlarge"
  desired_capacity                  = 6
  min_size                          = 3
  max_size                          = 24
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
  master_instance_type = "r8g.2xlarge"
  core_instance_type   = "r8g.4xlarge"
  core_instance_count  = 5
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

# ===================== Superset (BI Dashboards) =====================
module "superset" {
  source              = "../../modules/superset"
  environment         = var.environment
  vpc_id              = module.vpc.vpc_id
  private_subnet_ids  = module.vpc.private_subnet_ids
  ecs_cluster_id      = module.ecs.cluster_id
  ecs_cluster_name    = module.ecs.cluster_name
  superset_database_url = var.superset_database_url
  secret_key_arn      = var.superset_secret_key_arn
  certificate_arn     = var.certificate_arn
  tags                = local.tags
}

# ===================== Redash (Ad-hoc Analytics) =====================
module "redash" {
  source              = "../../modules/redash"
  environment         = var.environment
  vpc_id              = module.vpc.vpc_id
  private_subnet_ids  = module.vpc.private_subnet_ids
  ecs_cluster_id      = module.ecs.cluster_id
  ecs_cluster_name    = module.ecs.cluster_name
  redash_database_url = var.redash_database_url
  cookie_secret_arn   = var.redash_cookie_secret_arn
  certificate_arn     = var.certificate_arn
  tags                = local.tags
}

# ===================== JupyterHub (Data Science Notebooks) =====================
module "jupyter" {
  source             = "../../modules/jupyter"
  environment        = var.environment
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  ecs_cluster_id     = module.ecs.cluster_id
  s3_data_bucket     = module.s3.raw_bucket_name
  certificate_arn    = var.certificate_arn
  tags               = local.tags
}

# ===================== Monitoring =====================
module "monitoring" {
  source      = "../../modules/monitoring"
  environment = var.environment
  tags        = local.tags
}

# ===================== Outputs =====================
output "vpc_id" {
  value = module.vpc.vpc_id
}

output "aurora_endpoint" {
  value = module.aurora.cluster_endpoint
}

output "s3_raw_bucket" {
  value = module.s3.raw_bucket_name
}

output "s3_processed_bucket" {
  value = module.s3.processed_bucket_name
}

output "airflow_url" {
  value = module.airflow.webserver_url
}

output "emr_cluster_id" {
  value = module.emr.cluster_id
}
