###############################################################################
# Superset Module - BI/Visualization platform on ECS Fargate
# Primary dashboarding tool for Zomato analytics; connects to Trino and Druid
###############################################################################

# ---------- Security Group ----------
resource "aws_security_group" "superset" {
  name_prefix = "${var.project_name}-${var.environment}-superset-"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = var.superset_port
    to_port         = var.superset_port
    protocol        = "tcp"
    security_groups = [aws_security_group.superset_alb.id]
    description     = "Superset from ALB"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-superset-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "superset_alb" {
  name_prefix = "${var.project_name}-${var.environment}-superset-alb-"
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
    Name = "${var.project_name}-${var.environment}-superset-alb-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- Security Group for Superset Redis ----------
resource "aws_security_group" "superset_redis" {
  name_prefix = "${var.project_name}-${var.environment}-superset-redis-"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [aws_security_group.superset.id]
    description     = "Redis from Superset tasks"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-superset-redis-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- ElastiCache Redis for Superset ----------
resource "aws_elasticache_subnet_group" "superset" {
  name       = "${var.project_name}-${var.environment}-superset-redis"
  subnet_ids = var.private_subnet_ids
}

resource "aws_elasticache_replication_group" "superset" {
  replication_group_id = "${var.project_name}-${var.environment}-superset"
  description          = "Redis for Superset cache and Celery broker"
  node_type            = var.redis_node_type
  num_cache_clusters   = 2
  engine_version       = "7.0"
  port                 = 6379
  subnet_group_name    = aws_elasticache_subnet_group.superset.name
  security_group_ids   = [aws_security_group.superset_redis.id]
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  automatic_failover_enabled = true

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-superset-redis"
  })
}

# ---------- CloudWatch Log Group ----------
resource "aws_cloudwatch_log_group" "superset" {
  name              = "/ecs/${var.project_name}-${var.environment}/superset"
  retention_in_days = 30
  tags              = var.tags
}

# ---------- ECS Task Definition - Superset Web ----------
resource "aws_ecs_task_definition" "superset_web" {
  family                   = "${var.project_name}-${var.environment}-superset-web"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.web_cpu
  memory                   = var.web_memory
  execution_role_arn       = aws_iam_role.superset_execution.arn
  task_role_arn            = aws_iam_role.superset_task.arn

  container_definitions = jsonencode([{
    name  = "superset-web"
    image = var.superset_image
    command = [
      "gunicorn",
      "--bind", "0.0.0.0:${var.superset_port}",
      "--workers", "4",
      "--worker-class", "gevent",
      "--timeout", "120",
      "--limit-request-line", "0",
      "--limit-request-field_size", "0",
      "superset.app:create_app()"
    ]

    portMappings = [{
      containerPort = var.superset_port
      protocol      = "tcp"
    }]

    environment = [
      { name = "SUPERSET_ENV",                    value = "production" },
      { name = "SUPERSET_PORT",                   value = tostring(var.superset_port) },
      { name = "REDIS_HOST",                      value = aws_elasticache_replication_group.superset.primary_endpoint_address },
      { name = "REDIS_PORT",                      value = "6379" },
      { name = "SQLALCHEMY_DATABASE_URI",         value = var.superset_database_url },
      { name = "SUPERSET_WEBSERVER_TIMEOUT",      value = "120" },
      { name = "MAPBOX_API_KEY",                  value = "" },
      { name = "ENABLE_PROXY_FIX",                value = "True" },
      { name = "FEATURE_FLAGS",                   value = "{\"DASHBOARD_NATIVE_FILTERS\": true, \"DASHBOARD_CROSS_FILTERS\": true, \"ENABLE_TEMPLATE_PROCESSING\": true}" },
    ]

    secrets = [
      {
        name      = "SUPERSET_SECRET_KEY"
        valueFrom = var.secret_key_arn
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.superset.name
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "web"
      }
    }

    healthCheck = {
      command     = ["CMD-SHELL", "curl -f http://localhost:${var.superset_port}/health || exit 1"]
      interval    = 30
      timeout     = 10
      retries     = 3
      startPeriod = 120
    }
  }])

  tags = var.tags
}

# ---------- ECS Task Definition - Superset Celery Worker ----------
resource "aws_ecs_task_definition" "superset_worker" {
  family                   = "${var.project_name}-${var.environment}-superset-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.worker_cpu
  memory                   = var.worker_memory
  execution_role_arn       = aws_iam_role.superset_execution.arn
  task_role_arn            = aws_iam_role.superset_task.arn

  container_definitions = jsonencode([{
    name  = "superset-worker"
    image = var.superset_image
    command = [
      "celery",
      "--app=superset.tasks.celery_app:app",
      "worker",
      "--pool=prefork",
      "--concurrency=4",
      "--max-tasks-per-child=128",
      "-Ofair",
      "--loglevel=INFO"
    ]

    environment = [
      { name = "SUPERSET_ENV",            value = "production" },
      { name = "REDIS_HOST",              value = aws_elasticache_replication_group.superset.primary_endpoint_address },
      { name = "REDIS_PORT",              value = "6379" },
      { name = "SQLALCHEMY_DATABASE_URI", value = var.superset_database_url },
    ]

    secrets = [
      {
        name      = "SUPERSET_SECRET_KEY"
        valueFrom = var.secret_key_arn
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.superset.name
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "worker"
      }
    }
  }])

  tags = var.tags
}

# ---------- ECS Task Definition - Superset Celery Beat ----------
resource "aws_ecs_task_definition" "superset_beat" {
  family                   = "${var.project_name}-${var.environment}-superset-beat"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.superset_execution.arn
  task_role_arn            = aws_iam_role.superset_task.arn

  container_definitions = jsonencode([{
    name  = "superset-beat"
    image = var.superset_image
    command = [
      "celery",
      "--app=superset.tasks.celery_app:app",
      "beat",
      "--pidfile=/tmp/celerybeat.pid",
      "--schedule=/tmp/celerybeat-schedule",
      "--loglevel=INFO"
    ]

    environment = [
      { name = "SUPERSET_ENV",            value = "production" },
      { name = "REDIS_HOST",              value = aws_elasticache_replication_group.superset.primary_endpoint_address },
      { name = "REDIS_PORT",              value = "6379" },
      { name = "SQLALCHEMY_DATABASE_URI", value = var.superset_database_url },
    ]

    secrets = [
      {
        name      = "SUPERSET_SECRET_KEY"
        valueFrom = var.secret_key_arn
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.superset.name
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "beat"
      }
    }
  }])

  tags = var.tags
}

# ---------- ALB ----------
resource "aws_lb" "superset" {
  name               = "${var.project_name}-${var.environment}-superset"
  internal           = true
  load_balancer_type = "application"
  security_groups    = [aws_security_group.superset_alb.id]
  subnets            = var.private_subnet_ids

  enable_deletion_protection = true

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-superset-alb"
  })
}

resource "aws_lb_target_group" "superset" {
  name        = "${var.project_name}-${var.environment}-superset"
  port        = var.superset_port
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    enabled             = true
    path                = "/health"
    port                = "traffic-port"
    healthy_threshold   = 3
    unhealthy_threshold = 3
    timeout             = 10
    interval            = 30
    matcher             = "200"
  }

  tags = var.tags
}

resource "aws_lb_listener" "superset_https" {
  load_balancer_arn = aws_lb.superset.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.superset.arn
  }
}

resource "aws_lb_listener" "superset_http_redirect" {
  load_balancer_arn = aws_lb.superset.arn
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
resource "aws_ecs_service" "superset_web" {
  name            = "superset-web"
  cluster         = var.ecs_cluster_id
  task_definition = aws_ecs_task_definition.superset_web.arn
  desired_count   = var.web_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.private_subnet_ids
    security_groups = [aws_security_group.superset.id]
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.superset.arn
    container_name   = "superset-web"
    container_port   = var.superset_port
  }

  depends_on = [aws_lb_listener.superset_https]

  tags = var.tags
}

resource "aws_ecs_service" "superset_worker" {
  name            = "superset-worker"
  cluster         = var.ecs_cluster_id
  task_definition = aws_ecs_task_definition.superset_worker.arn
  desired_count   = var.worker_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.private_subnet_ids
    security_groups = [aws_security_group.superset.id]
  }

  tags = var.tags
}

resource "aws_ecs_service" "superset_beat" {
  name            = "superset-beat"
  cluster         = var.ecs_cluster_id
  task_definition = aws_ecs_task_definition.superset_beat.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.private_subnet_ids
    security_groups = [aws_security_group.superset.id]
  }

  tags = var.tags
}

# ---------- Auto Scaling for Web ----------
resource "aws_appautoscaling_target" "superset_web" {
  max_capacity       = var.web_max_count
  min_capacity       = var.web_desired_count
  resource_id        = "service/${var.ecs_cluster_name}/${aws_ecs_service.superset_web.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "superset_web_cpu" {
  name               = "${var.project_name}-${var.environment}-superset-web-cpu"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.superset_web.resource_id
  scalable_dimension = aws_appautoscaling_target.superset_web.scalable_dimension
  service_namespace  = aws_appautoscaling_target.superset_web.service_namespace

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
