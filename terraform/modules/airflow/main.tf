###############################################################################
# Airflow Module - MWAA (Managed Workflows for Apache Airflow)
# Orchestrates ETL jobs, Athena queries, and pipeline scheduling
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

variable "s3_dags_bucket" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}

# ---------- Security Group ----------
resource "aws_security_group" "airflow" {
  name_prefix = "${var.project_name}-${var.environment}-airflow-"
  vpc_id      = var.vpc_id

  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "HTTPS for Airflow Web UI"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-airflow-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- IAM Role ----------
resource "aws_iam_role" "airflow" {
  name = "${var.project_name}-${var.environment}-airflow-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "airflow" {
  name = "airflow-execution-policy"
  role = aws_iam_role.airflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:*"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_dags_bucket}",
          "arn:aws:s3:::${var.s3_dags_bucket}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups"
        ]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "emr:*",
          "ecs:*",
          "ec2:Describe*"
        ]
        Resource = ["*"]
      }
    ]
  })
}

# ---------- MWAA Environment ----------
resource "aws_mwaa_environment" "main" {
  name               = "${var.project_name}-${var.environment}-airflow"
  airflow_version    = "2.8.1"
  environment_class  = "mw1.large"
  execution_role_arn = aws_iam_role.airflow.arn

  source_bucket_arn = "arn:aws:s3:::${var.s3_dags_bucket}"
  dag_s3_path       = "dags/"

  network_configuration {
    security_group_ids = [aws_security_group.airflow.id]
    subnet_ids         = slice(var.subnet_ids, 0, 2)
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "WARNING"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  max_workers = 25
  min_workers = 5

  airflow_configuration_options = {
    "core.default_timezone"           = "Asia/Kolkata"
    "core.parallelism"                = "64"
    "core.max_active_runs_per_dag"    = "16"
    "celery.worker_concurrency"       = "16"
    "webserver.default_ui_timezone"   = "Asia/Kolkata"
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-airflow"
  })
}

# ---------- Outputs ----------
output "airflow_arn" {
  value = aws_mwaa_environment.main.arn
}

output "webserver_url" {
  value = aws_mwaa_environment.main.webserver_url
}
