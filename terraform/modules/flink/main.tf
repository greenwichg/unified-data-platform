###############################################################################
# Flink Module - Stream Processing on Amazon Managed Flink / ECS
# Used by Pipeline 2 (CDC) and Pipeline 4 (Real-time Events)
# Complex event processing with feedback loops
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

variable "kafka_security_group_id" {
  type = string
}

variable "s3_checkpoints_bucket" {
  type = string
}

variable "s3_output_bucket" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}

# ---------- Security Group ----------
resource "aws_security_group" "flink" {
  name_prefix = "${var.project_name}-${var.environment}-flink-"
  vpc_id      = var.vpc_id

  # Flink TaskManager RPC
  ingress {
    from_port   = 6121
    to_port     = 6125
    protocol    = "tcp"
    self        = true
    description = "Flink internal communication"
  }

  # Flink Web UI
  ingress {
    from_port   = 8081
    to_port     = 8081
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Flink Web UI"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-flink-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# Allow Flink to connect to Kafka
resource "aws_security_group_rule" "kafka_from_flink" {
  type                     = "ingress"
  from_port                = 9092
  to_port                  = 9093
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.flink.id
  security_group_id        = var.kafka_security_group_id
  description              = "Kafka access from Flink"
}

# ---------- IAM Role ----------
resource "aws_iam_role" "flink" {
  name = "${var.project_name}-${var.environment}-flink-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = ["ecs-tasks.amazonaws.com", "kinesisanalytics.amazonaws.com"]
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "flink_s3" {
  name = "flink-s3-access"
  role = aws_iam_role.flink.id

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
          "arn:aws:s3:::${var.s3_checkpoints_bucket}",
          "arn:aws:s3:::${var.s3_checkpoints_bucket}/*",
          "arn:aws:s3:::${var.s3_output_bucket}",
          "arn:aws:s3:::${var.s3_output_bucket}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

# ---------- ECS Cluster for Flink ----------
resource "aws_ecs_cluster" "flink" {
  name = "${var.project_name}-${var.environment}-flink"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-flink-cluster"
  })
}

# ---------- Flink JobManager Task Definition ----------
resource "aws_ecs_task_definition" "flink_jobmanager" {
  family                   = "${var.project_name}-${var.environment}-flink-jobmanager"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 4096
  memory                   = 16384
  execution_role_arn       = aws_iam_role.flink.arn
  task_role_arn            = aws_iam_role.flink.arn

  container_definitions = jsonencode([{
    name  = "flink-jobmanager"
    image = "flink:1.18-java17"
    command = ["jobmanager"]

    portMappings = [
      { containerPort = 6123, protocol = "tcp" },
      { containerPort = 8081, protocol = "tcp" }
    ]

    environment = [
      { name = "FLINK_PROPERTIES", value = join("\n", [
        "jobmanager.rpc.address: localhost",
        "state.backend: rocksdb",
        "state.checkpoints.dir: s3://${var.s3_checkpoints_bucket}/flink/checkpoints",
        "state.savepoints.dir: s3://${var.s3_checkpoints_bucket}/flink/savepoints",
        "execution.checkpointing.interval: 60000",
        "execution.checkpointing.min-pause: 30000",
        "taskmanager.memory.process.size: 14gb",
        "parallelism.default: 32"
      ])}
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/${var.project_name}-${var.environment}/flink-jobmanager"
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "flink"
      }
    }
  }])

  tags = var.tags
}

# ---------- Flink TaskManager Task Definition ----------
resource "aws_ecs_task_definition" "flink_taskmanager" {
  family                   = "${var.project_name}-${var.environment}-flink-taskmanager"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 8192
  memory                   = 30720
  execution_role_arn       = aws_iam_role.flink.arn
  task_role_arn            = aws_iam_role.flink.arn

  container_definitions = jsonencode([{
    name  = "flink-taskmanager"
    image = "flink:1.18-java17"
    command = ["taskmanager"]

    environment = [
      { name = "FLINK_PROPERTIES", value = join("\n", [
        "jobmanager.rpc.address: flink-jobmanager.${var.environment}.internal",
        "state.backend: rocksdb",
        "state.checkpoints.dir: s3://${var.s3_checkpoints_bucket}/flink/checkpoints",
        "taskmanager.numberOfTaskSlots: 8",
        "taskmanager.memory.process.size: 28gb",
        "taskmanager.memory.managed.fraction: 0.4"
      ])}
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/${var.project_name}-${var.environment}/flink-taskmanager"
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "flink"
      }
    }
  }])

  tags = var.tags
}

# ---------- Flink TaskManager Service ----------
resource "aws_ecs_service" "flink_taskmanager" {
  name            = "flink-taskmanager"
  cluster         = aws_ecs_cluster.flink.id
  task_definition = aws_ecs_task_definition.flink_taskmanager.arn
  desired_count   = 8
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.subnet_ids
    security_groups = [aws_security_group.flink.id]
  }

  tags = var.tags
}

# ---------- CloudWatch Log Groups ----------
resource "aws_cloudwatch_log_group" "flink_jobmanager" {
  name              = "/ecs/${var.project_name}-${var.environment}/flink-jobmanager"
  retention_in_days = 30
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "flink_taskmanager" {
  name              = "/ecs/${var.project_name}-${var.environment}/flink-taskmanager"
  retention_in_days = 30
  tags              = var.tags
}

data "aws_region" "current" {}

# ---------- Outputs ----------
output "cluster_id" {
  value = aws_ecs_cluster.flink.id
}

output "security_group_id" {
  value = aws_security_group.flink.id
}

output "jobmanager_task_definition_arn" {
  value = aws_ecs_task_definition.flink_jobmanager.arn
}

output "taskmanager_task_definition_arn" {
  value = aws_ecs_task_definition.flink_taskmanager.arn
}
