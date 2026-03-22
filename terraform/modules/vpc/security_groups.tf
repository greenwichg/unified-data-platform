###############################################################################
# VPC Module - Security Group Rules for Inter-Service Communication
###############################################################################

# ---------- Kafka Security Group ----------
resource "aws_security_group" "kafka" {
  name_prefix = "${var.project_name}-${var.environment}-kafka-"
  vpc_id      = aws_vpc.main.id
  description = "Security group for Kafka brokers"

  # Kafka broker plaintext
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Kafka broker plaintext"
  }

  # Kafka broker SSL
  ingress {
    from_port   = 9093
    to_port     = 9093
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Kafka broker SSL"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-kafka-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- ZooKeeper Security Group ----------
resource "aws_security_group" "zookeeper" {
  name_prefix = "${var.project_name}-${var.environment}-zookeeper-"
  vpc_id      = aws_vpc.main.id
  description = "Security group for ZooKeeper ensemble"

  # ZooKeeper client port
  ingress {
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "ZooKeeper client port"
  }

  # ZooKeeper leader election
  ingress {
    from_port   = 2888
    to_port     = 2888
    protocol    = "tcp"
    self        = true
    description = "ZooKeeper peer port"
  }

  # ZooKeeper leader election
  ingress {
    from_port   = 3888
    to_port     = 3888
    protocol    = "tcp"
    self        = true
    description = "ZooKeeper leader election port"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-zookeeper-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- Flink Security Group ----------
resource "aws_security_group" "flink" {
  name_prefix = "${var.project_name}-${var.environment}-flink-"
  vpc_id      = aws_vpc.main.id
  description = "Security group for Flink cluster"

  # Flink RPC / TaskManager
  ingress {
    from_port   = 6121
    to_port     = 6125
    protocol    = "tcp"
    self        = true
    description = "Flink internal RPC"
  }

  # Flink JobManager RPC
  ingress {
    from_port   = 6123
    to_port     = 6123
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Flink JobManager RPC"
  }

  # Flink Web UI
  ingress {
    from_port   = 8081
    to_port     = 8081
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
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

# ---------- Trino Security Group ----------
# DEPRECATED: Trino on ECS has been replaced by serverless Amazon Athena.
# Athena is serverless and does not require VPC security group rules.
# This resource is retained temporarily for teardown; remove after migration is complete.
# resource "aws_security_group" "trino" {
#   name_prefix = "${var.project_name}-${var.environment}-trino-"
#   vpc_id      = aws_vpc.main.id
#   description = "DEPRECATED - Trino replaced by Athena (serverless)"
# }

# ---------- Druid Security Group ----------
resource "aws_security_group" "druid" {
  name_prefix = "${var.project_name}-${var.environment}-druid-"
  vpc_id      = aws_vpc.main.id
  description = "Security group for Druid cluster"

  # Druid Router
  ingress {
    from_port   = 8888
    to_port     = 8888
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Druid Router"
  }

  # Druid Broker
  ingress {
    from_port   = 8082
    to_port     = 8082
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Druid Broker"
  }

  # Druid Coordinator
  ingress {
    from_port   = 8081
    to_port     = 8081
    protocol    = "tcp"
    self        = true
    description = "Druid Coordinator"
  }

  # Druid Historical
  ingress {
    from_port   = 8083
    to_port     = 8083
    protocol    = "tcp"
    self        = true
    description = "Druid Historical"
  }

  # Druid MiddleManager
  ingress {
    from_port   = 8091
    to_port     = 8091
    protocol    = "tcp"
    self        = true
    description = "Druid MiddleManager"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-druid-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- Hive Metastore Security Group ----------
# DEPRECATED: Self-hosted Hive Metastore has been replaced by AWS Glue Data Catalog.
# Glue is a managed service and does not require VPC security group rules.
# This resource is retained temporarily for teardown; remove after migration is complete.
# resource "aws_security_group" "hive_metastore" {
#   name_prefix = "${var.project_name}-${var.environment}-hive-metastore-"
#   vpc_id      = aws_vpc.main.id
#   description = "DEPRECATED - Hive Metastore replaced by AWS Glue Data Catalog"
# }

# ---------- Cross-Service Rules ----------

# Allow Flink to access Kafka
resource "aws_security_group_rule" "flink_to_kafka" {
  type                     = "ingress"
  from_port                = 9092
  to_port                  = 9093
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.flink.id
  security_group_id        = aws_security_group.kafka.id
  description              = "Flink to Kafka brokers"
}

# Allow Druid to access Kafka
resource "aws_security_group_rule" "druid_to_kafka" {
  type                     = "ingress"
  from_port                = 9092
  to_port                  = 9093
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.druid.id
  security_group_id        = aws_security_group.kafka.id
  description              = "Druid to Kafka brokers"
}

# Allow Flink to access ZooKeeper
resource "aws_security_group_rule" "flink_to_zookeeper" {
  type                     = "ingress"
  from_port                = 2181
  to_port                  = 2181
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.flink.id
  security_group_id        = aws_security_group.zookeeper.id
  description              = "Flink to ZooKeeper"
}

# Allow Kafka to access ZooKeeper
resource "aws_security_group_rule" "kafka_to_zookeeper" {
  type                     = "ingress"
  from_port                = 2181
  to_port                  = 2181
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.kafka.id
  security_group_id        = aws_security_group.zookeeper.id
  description              = "Kafka to ZooKeeper"
}

# Allow Druid to access ZooKeeper
resource "aws_security_group_rule" "druid_to_zookeeper" {
  type                     = "ingress"
  from_port                = 2181
  to_port                  = 2181
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.druid.id
  security_group_id        = aws_security_group.zookeeper.id
  description              = "Druid to ZooKeeper"
}

# DEPRECATED: Trino-to-Hive-Metastore and Flink/Druid-to-Hive-Metastore rules removed.
# Trino replaced by serverless Athena; Hive Metastore replaced by AWS Glue Data Catalog.
# Flink and Druid now access Glue via IAM (no network-level SG rules required).
