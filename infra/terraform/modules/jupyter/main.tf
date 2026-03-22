###############################################################################
# JupyterHub Module - Data Science Notebook platform on ECS Fargate
# Connects to Athena, Druid, and S3 data lake for interactive analytics
###############################################################################

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# ---------- Security Group ----------
resource "aws_security_group" "jupyter" {
  name_prefix = "${var.project_name}-${var.environment}-jupyter-"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = var.jupyter_port
    to_port         = var.jupyter_port
    protocol        = "tcp"
    security_groups = [aws_security_group.jupyter_alb.id]
    description     = "JupyterHub from ALB"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-jupyter-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "jupyter_alb" {
  name_prefix = "${var.project_name}-${var.environment}-jupyter-alb-"
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
    Name = "${var.project_name}-${var.environment}-jupyter-alb-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- IAM Roles ----------
resource "aws_iam_role" "jupyter_execution" {
  name = "${var.project_name}-${var.environment}-jupyter-exec"

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

resource "aws_iam_role_policy_attachment" "jupyter_execution" {
  role       = aws_iam_role.jupyter_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "jupyter_task" {
  name = "${var.project_name}-${var.environment}-jupyter-task"

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

# S3 data lake access for notebooks
resource "aws_iam_role_policy" "jupyter_s3" {
  name = "jupyter-s3-datalake-access"
  role = aws_iam_role.jupyter_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:PutObject"
      ]
      Resource = [
        "arn:aws:s3:::${var.s3_data_bucket}",
        "arn:aws:s3:::${var.s3_data_bucket}/*"
      ]
    }]
  })
}

# Athena query access for notebooks
resource "aws_iam_role_policy" "jupyter_athena" {
  name = "jupyter-athena-access"
  role = aws_iam_role.jupyter_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:StopQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetWorkGroup",
          "athena:ListWorkGroups"
        ]
        Resource = "arn:aws:athena:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:workgroup/*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions"
        ]
        Resource = "*"
      }
    ]
  })
}

# ---------- CloudWatch Log Group ----------
resource "aws_cloudwatch_log_group" "jupyter" {
  name              = "/ecs/${var.project_name}-${var.environment}/jupyter"
  retention_in_days = 30
  tags              = var.tags
}

# ---------- ECS Task Definition ----------
resource "aws_ecs_task_definition" "jupyter" {
  family                   = "${var.project_name}-${var.environment}-jupyter"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = aws_iam_role.jupyter_execution.arn
  task_role_arn            = aws_iam_role.jupyter_task.arn

  container_definitions = jsonencode([{
    name  = "jupyterhub"
    image = var.jupyter_image

    portMappings = [{
      containerPort = var.jupyter_port
      protocol      = "tcp"
    }]

    environment = [
      { name = "JUPYTER_ENABLE_LAB",  value = "yes" },
      { name = "AWS_DEFAULT_REGION",  value = data.aws_region.current.name },
      { name = "S3_DATA_BUCKET",      value = var.s3_data_bucket },
      { name = "ATHENA_WORKGROUP",    value = "adhoc" },
      { name = "DRUID_BROKER_URL",    value = var.druid_broker_url },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.jupyter.name
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "jupyter"
      }
    }

    healthCheck = {
      command     = ["CMD-SHELL", "curl -f http://localhost:${var.jupyter_port}/hub/health || exit 1"]
      interval    = 30
      timeout     = 10
      retries     = 3
      startPeriod = 120
    }
  }])

  tags = var.tags
}

# ---------- ALB ----------
resource "aws_lb" "jupyter" {
  name               = "${var.project_name}-${var.environment}-jupyter"
  internal           = true
  load_balancer_type = "application"
  security_groups    = [aws_security_group.jupyter_alb.id]
  subnets            = var.private_subnet_ids

  enable_deletion_protection = true

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-jupyter-alb"
  })
}

resource "aws_lb_target_group" "jupyter" {
  name        = "${var.project_name}-${var.environment}-jupyter"
  port        = var.jupyter_port
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    enabled             = true
    path                = "/hub/health"
    port                = "traffic-port"
    healthy_threshold   = 3
    unhealthy_threshold = 3
    timeout             = 10
    interval            = 30
    matcher             = "200"
  }

  tags = var.tags
}

resource "aws_lb_listener" "jupyter_https" {
  load_balancer_arn = aws_lb.jupyter.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.jupyter.arn
  }
}

resource "aws_lb_listener" "jupyter_http_redirect" {
  load_balancer_arn = aws_lb.jupyter.arn
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

# ---------- ECS Service ----------
resource "aws_ecs_service" "jupyter" {
  name            = "jupyterhub"
  cluster         = var.ecs_cluster_id
  task_definition = aws_ecs_task_definition.jupyter.arn
  desired_count   = var.desired_count
  # Inherits cluster default capacity provider (FARGATE_SPOT primary, FARGATE fallback)

  network_configuration {
    subnets         = var.private_subnet_ids
    security_groups = [aws_security_group.jupyter.id]
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.jupyter.arn
    container_name   = "jupyterhub"
    container_port   = var.jupyter_port
  }

  depends_on = [aws_lb_listener.jupyter_https]

  tags = var.tags
}
