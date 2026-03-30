###############################################################################
# ECS Module - Container orchestration for Debezium connectors and services
###############################################################################

# ---------- ECS Cluster ----------
resource "aws_ecs_cluster" "main" {
  name = "${var.project_name}-${var.environment}-services"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-ecs-services"
  })
}

# ---------- Fargate Spot Capacity Provider (up to 70% savings) ----------
resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name = aws_ecs_cluster.main.name

  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 4
    base              = 1
  }

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
  }
}

# ---------- Security Group ----------
resource "aws_security_group" "ecs" {
  name_prefix = "${var.project_name}-${var.environment}-ecs-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 8083
    to_port     = 8083
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Kafka Connect REST API"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-ecs-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- Debezium Connector Task Definition ----------
resource "aws_ecs_task_definition" "debezium" {
  family                   = "${var.project_name}-${var.environment}-debezium"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 2048
  memory                   = 8192
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn

  container_definitions = jsonencode([{
    name  = "debezium-connect"
    image = "debezium/connect:2.5"

    portMappings = [{
      containerPort = 8083
      protocol      = "tcp"
    }]

    environment = [
      { name = "GROUP_ID",                  value = "zomato-cdc-${var.environment}" },
      { name = "CONFIG_STORAGE_TOPIC",      value = "debezium-configs" },
      { name = "OFFSET_STORAGE_TOPIC",      value = "debezium-offsets" },
      { name = "STATUS_STORAGE_TOPIC",      value = "debezium-status" },
      { name = "BOOTSTRAP_SERVERS",         value = var.kafka_bootstrap_servers },
      # MSK IAM authentication
      { name = "CONNECT_SECURITY_PROTOCOL",                        value = "SASL_SSL" },
      { name = "CONNECT_SASL_MECHANISM",                           value = "AWS_MSK_IAM" },
      { name = "CONNECT_SASL_JAAS_CONFIG",                         value = "software.amazon.msk.auth.iam.IAMLoginModule required;" },
      { name = "CONNECT_SASL_CLIENT_CALLBACK_HANDLER_CLASS",       value = "software.amazon.msk.auth.iam.IAMClientCallbackHandler" },
      # Producer/consumer IAM auth (for internal Connect topics)
      { name = "CONNECT_PRODUCER_SECURITY_PROTOCOL",               value = "SASL_SSL" },
      { name = "CONNECT_PRODUCER_SASL_MECHANISM",                  value = "AWS_MSK_IAM" },
      { name = "CONNECT_PRODUCER_SASL_JAAS_CONFIG",                value = "software.amazon.msk.auth.iam.IAMLoginModule required;" },
      { name = "CONNECT_PRODUCER_SASL_CLIENT_CALLBACK_HANDLER_CLASS", value = "software.amazon.msk.auth.iam.IAMClientCallbackHandler" },
      { name = "CONNECT_CONSUMER_SECURITY_PROTOCOL",               value = "SASL_SSL" },
      { name = "CONNECT_CONSUMER_SASL_MECHANISM",                  value = "AWS_MSK_IAM" },
      { name = "CONNECT_CONSUMER_SASL_JAAS_CONFIG",                value = "software.amazon.msk.auth.iam.IAMLoginModule required;" },
      { name = "CONNECT_CONSUMER_SASL_CLIENT_CALLBACK_HANDLER_CLASS", value = "software.amazon.msk.auth.iam.IAMClientCallbackHandler" },
      # AWS Glue Schema Registry converter (replaces Confluent Schema Registry)
      { name = "CONNECT_KEY_CONVERTER",   value = "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter" },
      { name = "CONNECT_VALUE_CONVERTER", value = "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter" },
      { name = "CONNECT_KEY_CONVERTER_REGION",          value = var.aws_region },
      { name = "CONNECT_VALUE_CONVERTER_REGION",        value = var.aws_region },
      { name = "CONNECT_KEY_CONVERTER_REGISTRY_NAME",   value = var.glue_registry_name },
      { name = "CONNECT_VALUE_CONVERTER_REGISTRY_NAME", value = var.glue_registry_name },
      { name = "CONNECT_KEY_CONVERTER_SCHEMA_AUTO_REGISTRATION_ENABLED",   value = "true" },
      { name = "CONNECT_VALUE_CONVERTER_SCHEMA_AUTO_REGISTRATION_ENABLED", value = "true" }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/${var.project_name}-${var.environment}/debezium"
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "debezium"
      }
    }
  }])

  tags = var.tags
}

# ---------- Debezium Service ----------
resource "aws_ecs_service" "debezium" {
  name            = "debezium-connect"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.debezium.arn
  desired_count   = 3

  # Use capacity provider strategy instead of launch_type for Spot savings
  capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
    base              = 1  # 1 guaranteed On-Demand for stability
  }

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 4  # 80% Spot for remaining tasks
  }

  network_configuration {
    subnets         = var.subnet_ids
    security_groups = [aws_security_group.ecs.id]
  }

  tags = var.tags
}

# ---------- CloudWatch Log Group ----------
resource "aws_cloudwatch_log_group" "debezium" {
  name              = "/ecs/${var.project_name}-${var.environment}/debezium"
  retention_in_days = 30
  tags              = var.tags
}

data "aws_region" "current" {}

