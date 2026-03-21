###############################################################################
# Redash Module - BI visualization platform on ECS Fargate
# Internal analytics tool for ad-hoc queries against Trino, Druid, and Aurora
###############################################################################

# ---------- Security Group ----------
resource "aws_security_group" "redash" {
  name_prefix = "${var.project_name}-${var.environment}-redash-"
  vpc_id      = var.vpc_id

  # ALB health checks and traffic
  ingress {
    from_port       = var.redash_port
    to_port         = var.redash_port
    protocol        = "tcp"
    security_groups = [aws_security_group.redash_alb.id]
    description     = "Redash from ALB"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-redash-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "redash_alb" {
  name_prefix = "${var.project_name}-${var.environment}-redash-alb-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
    description = "HTTPS from allowed networks"
  }

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
    description = "HTTP redirect"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-redash-alb-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- Security Group for Redash Redis ----------
resource "aws_security_group" "redash_redis" {
  name_prefix = "${var.project_name}-${var.environment}-redash-redis-"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [aws_security_group.redash.id]
    description     = "Redis from Redash tasks"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-redash-redis-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- ElastiCache Redis for Redash ----------
resource "aws_elasticache_subnet_group" "redash" {
  name       = "${var.project_name}-${var.environment}-redash-redis"
  subnet_ids = var.private_subnet_ids
}

resource "aws_elasticache_replication_group" "redash" {
  replication_group_id = "${var.project_name}-${var.environment}-redash"
  description          = "Redis for Redash query result caching and celery broker"
  node_type            = var.redis_node_type
  num_cache_clusters   = 2
  engine_version       = "7.0"
  port                 = 6379
  subnet_group_name    = aws_elasticache_subnet_group.redash.name
  security_group_ids   = [aws_security_group.redash_redis.id]
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  automatic_failover_enabled = true

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-redash-redis"
  })
}

# ---------- CloudWatch Log Group ----------
resource "aws_cloudwatch_log_group" "redash" {
  name              = "/ecs/${var.project_name}-${var.environment}/redash"
  retention_in_days = 30
  tags              = var.tags
}

# ---------- ECS Task Definition - Redash Server ----------
resource "aws_ecs_task_definition" "redash_server" {
  family                   = "${var.project_name}-${var.environment}-redash-server"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.server_cpu
  memory                   = var.server_memory
  execution_role_arn       = aws_iam_role.redash_execution.arn
  task_role_arn            = aws_iam_role.redash_task.arn

  container_definitions = jsonencode([{
    name  = "redash-server"
    image = var.redash_image
    command = ["server"]

    portMappings = [{
      containerPort = var.redash_port
      protocol      = "tcp"
    }]

    environment = [
      { name = "REDASH_LOG_LEVEL",             value = "INFO" },
      { name = "REDASH_REDIS_URL",             value = "redis://${aws_elasticache_replication_group.redash.primary_endpoint_address}:6379/0" },
      { name = "REDASH_DATABASE_URL",          value = var.redash_database_url },
      { name = "REDASH_COOKIE_SECRET",         value = "REPLACE_VIA_SECRETS_MANAGER" },
      { name = "REDASH_WEB_WORKERS",           value = "4" },
      { name = "REDASH_QUERY_RESULTS_CLEANUP_ENABLED", value = "true" },
      { name = "REDASH_QUERY_RESULTS_CLEANUP_MAX_AGE", value = "30" },
    ]

    secrets = [
      {
        name      = "REDASH_COOKIE_SECRET"
        valueFrom = var.cookie_secret_arn
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.redash.name
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "server"
      }
    }

    healthCheck = {
      command     = ["CMD-SHELL", "curl -f http://localhost:${var.redash_port}/ping || exit 1"]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = 60
    }
  }])

  tags = var.tags
}

# ---------- ECS Task Definition - Redash Worker ----------
resource "aws_ecs_task_definition" "redash_worker" {
  family                   = "${var.project_name}-${var.environment}-redash-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.worker_cpu
  memory                   = var.worker_memory
  execution_role_arn       = aws_iam_role.redash_execution.arn
  task_role_arn            = aws_iam_role.redash_task.arn

  container_definitions = jsonencode([{
    name  = "redash-worker"
    image = var.redash_image
    command = ["worker"]

    environment = [
      { name = "REDASH_LOG_LEVEL",                value = "INFO" },
      { name = "REDASH_REDIS_URL",                value = "redis://${aws_elasticache_replication_group.redash.primary_endpoint_address}:6379/0" },
      { name = "REDASH_DATABASE_URL",             value = var.redash_database_url },
      { name = "QUEUES",                          value = "queries,scheduled_queries,celery,schemas" },
      { name = "WORKERS_COUNT",                   value = "2" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.redash.name
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "worker"
      }
    }
  }])

  tags = var.tags
}

# ---------- ECS Task Definition - Redash Scheduler ----------
resource "aws_ecs_task_definition" "redash_scheduler" {
  family                   = "${var.project_name}-${var.environment}-redash-scheduler"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.redash_execution.arn
  task_role_arn            = aws_iam_role.redash_task.arn

  container_definitions = jsonencode([{
    name  = "redash-scheduler"
    image = var.redash_image
    command = ["scheduler"]

    environment = [
      { name = "REDASH_LOG_LEVEL",    value = "INFO" },
      { name = "REDASH_REDIS_URL",    value = "redis://${aws_elasticache_replication_group.redash.primary_endpoint_address}:6379/0" },
      { name = "REDASH_DATABASE_URL", value = var.redash_database_url },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.redash.name
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "scheduler"
      }
    }
  }])

  tags = var.tags
}

# ---------- ALB ----------
resource "aws_lb" "redash" {
  name               = "${var.project_name}-${var.environment}-redash"
  internal           = true
  load_balancer_type = "application"
  security_groups    = [aws_security_group.redash_alb.id]
  subnets            = var.private_subnet_ids

  enable_deletion_protection = true

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-redash-alb"
  })
}

resource "aws_lb_target_group" "redash" {
  name        = "${var.project_name}-${var.environment}-redash"
  port        = var.redash_port
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    enabled             = true
    path                = "/ping"
    port                = "traffic-port"
    healthy_threshold   = 3
    unhealthy_threshold = 3
    timeout             = 5
    interval            = 30
    matcher             = "200"
  }

  tags = var.tags
}

resource "aws_lb_listener" "redash_https" {
  load_balancer_arn = aws_lb.redash.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.redash.arn
  }
}

resource "aws_lb_listener" "redash_http_redirect" {
  load_balancer_arn = aws_lb.redash.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "redirect"
    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}

# ---------- ECS Services ----------
resource "aws_ecs_service" "redash_server" {
  name            = "redash-server"
  cluster         = var.ecs_cluster_id
  task_definition = aws_ecs_task_definition.redash_server.arn
  desired_count   = var.server_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.private_subnet_ids
    security_groups = [aws_security_group.redash.id]
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.redash.arn
    container_name   = "redash-server"
    container_port   = var.redash_port
  }

  depends_on = [aws_lb_listener.redash_https]

  tags = var.tags
}

resource "aws_ecs_service" "redash_worker" {
  name            = "redash-worker"
  cluster         = var.ecs_cluster_id
  task_definition = aws_ecs_task_definition.redash_worker.arn
  desired_count   = var.worker_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.private_subnet_ids
    security_groups = [aws_security_group.redash.id]
  }

  tags = var.tags
}

resource "aws_ecs_service" "redash_scheduler" {
  name            = "redash-scheduler"
  cluster         = var.ecs_cluster_id
  task_definition = aws_ecs_task_definition.redash_scheduler.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.private_subnet_ids
    security_groups = [aws_security_group.redash.id]
  }

  tags = var.tags
}

# ---------- Auto Scaling for Server ----------
resource "aws_appautoscaling_target" "redash_server" {
  max_capacity       = var.server_max_count
  min_capacity       = var.server_desired_count
  resource_id        = "service/${var.ecs_cluster_name}/${aws_ecs_service.redash_server.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "redash_server_cpu" {
  name               = "${var.project_name}-${var.environment}-redash-server-cpu"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.redash_server.resource_id
  scalable_dimension = aws_appautoscaling_target.redash_server.scalable_dimension
  service_namespace  = aws_appautoscaling_target.redash_server.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
    target_value       = 70.0
    scale_in_cooldown  = 300
    scale_out_cooldown = 60
  }
}

data "aws_region" "current" {}
