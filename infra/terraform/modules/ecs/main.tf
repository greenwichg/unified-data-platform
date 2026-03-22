###############################################################################
# ECS Module - Container orchestration for Debezium connectors and services
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

variable "subnet_ids" {
  type = list(string)
}

variable "tags" {
  type    = map(string)
  default = {}
}

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

# ---------- IAM Roles ----------
resource "aws_iam_role" "ecs_task_execution" {
  name = "${var.project_name}-${var.environment}-ecs-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
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
      { name = "GROUP_ID", value = "zomato-cdc-${var.environment}" },
      { name = "CONFIG_STORAGE_TOPIC", value = "debezium-configs" },
      { name = "OFFSET_STORAGE_TOPIC", value = "debezium-offsets" },
      { name = "STATUS_STORAGE_TOPIC", value = "debezium-status" },
      { name = "CONNECT_KEY_CONVERTER", value = "io.confluent.connect.avro.AvroConverter" },
      { name = "CONNECT_VALUE_CONVERTER", value = "io.confluent.connect.avro.AvroConverter" }
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

# ---------- Outputs ----------
output "cluster_id" {
  value = aws_ecs_cluster.main.id
}

output "cluster_arn" {
  value = aws_ecs_cluster.main.arn
}

output "cluster_name" {
  value = aws_ecs_cluster.main.name
}

output "security_group_id" {
  value = aws_security_group.ecs.id
}
