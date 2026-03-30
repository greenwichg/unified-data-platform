###############################################################################
# Airflow Module - MWAA (Managed Workflows for Apache Airflow)
# Orchestrates ETL jobs, Athena queries, and pipeline scheduling
###############################################################################

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

# ---------- MWAA Environment ----------
resource "aws_mwaa_environment" "main" {
  name               = "${var.project_name}-${var.environment}-airflow"
  airflow_version    = "2.8.1"
  environment_class  = var.environment_class
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

  max_workers = var.max_workers
  min_workers = var.min_workers

  airflow_configuration_options = {
    "core.default_timezone"         = "Asia/Kolkata"
    "core.parallelism"              = "64"
    "core.max_active_runs_per_dag"  = "16"
    "celery.worker_concurrency"     = "16"
    "webserver.default_ui_timezone" = "Asia/Kolkata"
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-airflow"
  })
}

