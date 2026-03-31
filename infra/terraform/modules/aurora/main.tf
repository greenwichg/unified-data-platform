###############################################################################
# Aurora MySQL Module - Primary transactional database
# Source for Pipeline 1 (Spark JDBC) and Pipeline 2 (Debezium CDC)
###############################################################################

# ---------- DB Subnet Group ----------
resource "aws_db_subnet_group" "aurora" {
  name       = "${var.project_name}-${var.environment}-aurora"
  subnet_ids = var.subnet_ids

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-aurora-subnet-group"
  })
}

# ---------- Security Group ----------
resource "aws_security_group" "aurora" {
  name_prefix = "${var.project_name}-${var.environment}-aurora-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 3306
    to_port     = 3306
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "MySQL from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-aurora-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- Aurora Cluster ----------
resource "aws_rds_cluster" "main" {
  cluster_identifier          = "${var.project_name}-${var.environment}-aurora"
  engine                      = "aurora-mysql"
  engine_version              = "8.0.mysql_aurora.3.07.1"
  database_name               = "zomato"
  master_username             = "admin"
  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.aurora.name
  vpc_security_group_ids = [aws_security_group.aurora.id]

  # Enable binary logging for Debezium CDC (Pipeline 2)
  enabled_cloudwatch_logs_exports = ["audit", "error", "slowquery"]

  backup_retention_period = 7
  preferred_backup_window = "03:00-04:00"
  storage_encrypted       = true
  deletion_protection     = true

  db_cluster_parameter_group_name = aws_rds_cluster_parameter_group.main.name

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-aurora"
  })
}

# ---------- Parameter Group (binlog for CDC) ----------
resource "aws_rds_cluster_parameter_group" "main" {
  name   = "${var.project_name}-${var.environment}-aurora-params"
  family = "aurora-mysql8.0"

  # Enable binlog for Debezium CDC (static params require pending-reboot)
  parameter {
    name         = "binlog_format"
    value        = "ROW"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "binlog_row_image"
    value        = "FULL"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "log_bin_trust_function_creators"
    value        = "1"
    apply_method = "pending-reboot"
  }

  tags = var.tags
}

# ---------- Aurora Instances ----------
resource "aws_rds_cluster_instance" "main" {
  count              = var.instance_count
  identifier         = "${var.project_name}-${var.environment}-aurora-${count.index}"
  cluster_identifier = aws_rds_cluster.main.id
  instance_class     = var.instance_class
  engine             = aws_rds_cluster.main.engine
  engine_version     = aws_rds_cluster.main.engine_version

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-aurora-${count.index}"
  })
}

