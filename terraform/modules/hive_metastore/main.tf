###############################################################################
# Hive Metastore Module - RDS MySQL + ECS Fargate Thrift Server
# Centralized metadata catalog for Trino, Flink, and Spark
###############################################################################

# ---------- DB Subnet Group ----------
resource "aws_db_subnet_group" "hive_metastore" {
  name       = "${var.project_name}-${var.environment}-hive-metastore"
  subnet_ids = var.data_subnet_ids

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-hive-metastore-subnet-group"
  })
}

# ---------- Security Group for RDS ----------
resource "aws_security_group" "hive_metastore_db" {
  name_prefix = "${var.project_name}-${var.environment}-hive-metastore-db-"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 3306
    to_port         = 3306
    protocol        = "tcp"
    security_groups = [aws_security_group.hive_metastore.id]
    description     = "MySQL from Hive Metastore service"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-hive-metastore-db-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- Security Group for Hive Metastore Thrift ----------
resource "aws_security_group" "hive_metastore" {
  name_prefix = "${var.project_name}-${var.environment}-hive-metastore-"
  vpc_id      = var.vpc_id

  # Hive Metastore Thrift port
  ingress {
    from_port   = 9083
    to_port     = 9083
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Hive Metastore Thrift"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-hive-metastore-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- RDS MySQL for Hive Metastore Schema ----------
resource "aws_rds_cluster" "hive_metastore" {
  cluster_identifier     = "${var.project_name}-${var.environment}-hive-metastore"
  engine                 = "aurora-mysql"
  engine_version         = "8.0.mysql_aurora.3.05.2"
  database_name          = "hive_metastore"
  master_username        = "hive"
  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.hive_metastore.name
  vpc_security_group_ids = [aws_security_group.hive_metastore_db.id]

  backup_retention_period = 7
  preferred_backup_window = "04:00-05:00"
  storage_encrypted       = true
  deletion_protection     = true

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-hive-metastore-db"
  })
}

resource "aws_rds_cluster_instance" "hive_metastore" {
  count              = var.db_instance_count
  identifier         = "${var.project_name}-${var.environment}-hive-metastore-${count.index}"
  cluster_identifier = aws_rds_cluster.hive_metastore.id
  instance_class     = var.db_instance_class
  engine             = aws_rds_cluster.hive_metastore.engine
  engine_version     = aws_rds_cluster.hive_metastore.engine_version

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-hive-metastore-${count.index}"
  })
}

# ---------- IAM Role for ECS Task ----------
resource "aws_iam_role" "hive_metastore" {
  name = "${var.project_name}-${var.environment}-hive-metastore-role"

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

resource "aws_iam_role_policy" "hive_metastore" {
  name = "hive-metastore-policy"
  role = aws_iam_role.hive_metastore.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

# ---------- ECS Cluster ----------
resource "aws_ecs_cluster" "hive_metastore" {
  name = "${var.project_name}-${var.environment}-hive-metastore"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-hive-metastore-cluster"
  })
}

# ---------- Hive Metastore Task Definition ----------
resource "aws_ecs_task_definition" "hive_metastore" {
  family                   = "${var.project_name}-${var.environment}-hive-metastore"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.hive_metastore.arn
  task_role_arn            = aws_iam_role.hive_metastore.arn

  container_definitions = jsonencode([{
    name  = "hive-metastore"
    image = "apache/hive:3.1.3"
    command = ["/opt/hive/bin/hive", "--service", "metastore"]

    portMappings = [{
      containerPort = 9083
      protocol      = "tcp"
    }]

    environment = [
      {
        name  = "SERVICE_NAME"
        value = "metastore"
      },
      {
        name  = "DB_DRIVER"
        value = "mysql"
      },
      {
        name  = "JAVAX_JDO_OPTION_CONNECTIONURL"
        value = "jdbc:mysql://${aws_rds_cluster.hive_metastore.endpoint}:3306/hive_metastore?createDatabaseIfNotExist=true&useSSL=true"
      },
      {
        name  = "JAVAX_JDO_OPTION_CONNECTIONDRIVERNAME"
        value = "com.mysql.cj.jdbc.Driver"
      },
      {
        name  = "JAVAX_JDO_OPTION_CONNECTIONUSERNAME"
        value = "hive"
      },
      {
        name  = "HIVE_METASTORE_WAREHOUSE_DIR"
        value = "s3://${var.s3_warehouse_bucket}/warehouse/"
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/${var.project_name}-${var.environment}/hive-metastore"
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "hive-metastore"
      }
    }

    healthCheck = {
      command     = ["CMD-SHELL", "nc -z localhost 9083 || exit 1"]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = 60
    }
  }])

  tags = var.tags
}

# ---------- Hive Metastore ECS Service ----------
resource "aws_ecs_service" "hive_metastore" {
  name            = "hive-metastore"
  cluster         = aws_ecs_cluster.hive_metastore.id
  task_definition = aws_ecs_task_definition.hive_metastore.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.private_subnet_ids
    security_groups = [aws_security_group.hive_metastore.id]
  }

  service_registries {
    registry_arn = aws_service_discovery_service.hive_metastore.arn
  }

  tags = var.tags
}

# ---------- Service Discovery ----------
resource "aws_service_discovery_private_dns_namespace" "hive_metastore" {
  name = "${var.environment}.internal"
  vpc  = var.vpc_id

  tags = var.tags
}

resource "aws_service_discovery_service" "hive_metastore" {
  name = "hive-metastore"

  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.hive_metastore.id

    dns_records {
      ttl  = 10
      type = "A"
    }

    routing_policy = "MULTIVALUE"
  }

  health_check_custom_config {
    failure_threshold = 1
  }

  tags = var.tags
}

# ---------- CloudWatch Log Group ----------
resource "aws_cloudwatch_log_group" "hive_metastore" {
  name              = "/ecs/${var.project_name}-${var.environment}/hive-metastore"
  retention_in_days = 30
  tags              = var.tags
}

data "aws_region" "current" {}
