###############################################################################
# Trino Module - Query Engine with 3 Cluster Types
# Adhoc, ETL (Airflow), and Reporting clusters
# 250K+ queries/week, 2PB scanned
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

# ---------- Security Group ----------
resource "aws_security_group" "trino" {
  name_prefix = "${var.project_name}-${var.environment}-trino-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Trino HTTP"
  }

  ingress {
    from_port   = 8443
    to_port     = 8443
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Trino HTTPS"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-trino-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- IAM Role ----------
resource "aws_iam_role" "trino" {
  name = "${var.project_name}-${var.environment}-trino-role"

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

resource "aws_iam_role_policy" "trino_s3" {
  name = "trino-data-access"
  role = aws_iam_role.trino.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:*"]
        Resource = ["*"]
      }
    ]
  })
}

# ---------- ECS Cluster ----------
resource "aws_ecs_cluster" "trino" {
  name = "${var.project_name}-${var.environment}-trino"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-trino-cluster"
  })
}

# ---------- Trino Cluster Definitions (Adhoc, ETL, Reporting) ----------
locals {
  trino_clusters = {
    adhoc = {
      coordinator_cpu    = 4096
      coordinator_memory = 16384
      worker_cpu         = 8192
      worker_memory      = 30720
      worker_count       = 10
      description        = "Ad-hoc queries from analysts"
    }
    etl = {
      coordinator_cpu    = 4096
      coordinator_memory = 16384
      worker_cpu         = 16384
      worker_memory      = 61440
      worker_count       = 20
      description        = "ETL queries from Airflow"
    }
    reporting = {
      coordinator_cpu    = 4096
      coordinator_memory = 16384
      worker_cpu         = 8192
      worker_memory      = 30720
      worker_count       = 8
      description        = "Dashboard and reporting queries"
    }
  }
}

# ---------- Coordinator Task Definitions ----------
resource "aws_ecs_task_definition" "trino_coordinator" {
  for_each                 = local.trino_clusters
  family                   = "${var.project_name}-${var.environment}-trino-${each.key}-coordinator"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value.coordinator_cpu
  memory                   = each.value.coordinator_memory
  execution_role_arn       = aws_iam_role.trino.arn
  task_role_arn            = aws_iam_role.trino.arn

  container_definitions = jsonencode([{
    name  = "trino-coordinator"
    image = "trinodb/trino:435"

    portMappings = [
      { containerPort = 8080, protocol = "tcp" }
    ]

    environment = [
      { name = "TRINO_ENVIRONMENT", value = each.key },
      { name = "COORDINATOR", value = "true" }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/${var.project_name}-${var.environment}/trino-${each.key}"
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "coordinator"
      }
    }
  }])

  tags = merge(var.tags, { ClusterType = each.key })
}

# ---------- Worker Task Definitions ----------
resource "aws_ecs_task_definition" "trino_worker" {
  for_each                 = local.trino_clusters
  family                   = "${var.project_name}-${var.environment}-trino-${each.key}-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value.worker_cpu
  memory                   = each.value.worker_memory
  execution_role_arn       = aws_iam_role.trino.arn
  task_role_arn            = aws_iam_role.trino.arn

  container_definitions = jsonencode([{
    name  = "trino-worker"
    image = "trinodb/trino:435"

    environment = [
      { name = "TRINO_ENVIRONMENT", value = each.key },
      { name = "COORDINATOR", value = "false" }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/${var.project_name}-${var.environment}/trino-${each.key}"
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "worker"
      }
    }
  }])

  tags = merge(var.tags, { ClusterType = each.key })
}

# ---------- Worker Services ----------
resource "aws_ecs_service" "trino_worker" {
  for_each        = local.trino_clusters
  name            = "trino-${each.key}-worker"
  cluster         = aws_ecs_cluster.trino.id
  task_definition = aws_ecs_task_definition.trino_worker[each.key].arn
  desired_count   = each.value.worker_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.subnet_ids
    security_groups = [aws_security_group.trino.id]
  }

  tags = merge(var.tags, { ClusterType = each.key })
}

# ---------- Auto Scaling for Workers ----------
resource "aws_appautoscaling_target" "trino_worker" {
  for_each           = local.trino_clusters
  max_capacity       = each.value.worker_count * 3
  min_capacity       = each.value.worker_count
  resource_id        = "service/${aws_ecs_cluster.trino.name}/${aws_ecs_service.trino_worker[each.key].name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "trino_worker_cpu" {
  for_each           = local.trino_clusters
  name               = "trino-${each.key}-cpu-scaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.trino_worker[each.key].resource_id
  scalable_dimension = aws_appautoscaling_target.trino_worker[each.key].scalable_dimension
  service_namespace  = aws_appautoscaling_target.trino_worker[each.key].service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
    target_value = 70.0
  }
}

# ---------- CloudWatch Log Groups ----------
resource "aws_cloudwatch_log_group" "trino" {
  for_each          = local.trino_clusters
  name              = "/ecs/${var.project_name}-${var.environment}/trino-${each.key}"
  retention_in_days = 30
  tags              = var.tags
}

data "aws_region" "current" {}

# ---------- Outputs ----------
output "cluster_id" {
  value = aws_ecs_cluster.trino.id
}

output "security_group_id" {
  value = aws_security_group.trino.id
}
