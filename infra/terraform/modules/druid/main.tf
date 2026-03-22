###############################################################################
# Druid Module - Real-time OLAP for Pipeline 4
# 20B events/week, 8M queries/week, millisecond response times
# Deployed on R8g instances for performance
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

variable "s3_deep_storage_bucket" {
  type = string
}

variable "kafka_secondary_security_group_id" {
  description = "Security group ID of the secondary MSK cluster for Druid ingestion"
  type        = string
  default     = ""
}

variable "tags" {
  type    = map(string)
  default = {}
}

# ---------- Security Group ----------
resource "aws_security_group" "druid" {
  name_prefix = "${var.project_name}-${var.environment}-druid-"
  vpc_id      = var.vpc_id

  # Druid Router
  ingress {
    from_port   = 8888
    to_port     = 8888
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Druid Router"
  }

  # Druid Broker
  ingress {
    from_port   = 8082
    to_port     = 8082
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
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

  # Kafka secondary (Druid ingestion) - MSK ports
  ingress {
    from_port       = 9092
    to_port         = 9098
    protocol        = "tcp"
    cidr_blocks     = ["10.0.0.0/16"]
    description     = "MSK secondary cluster access for Druid ingestion"
  }

  # Internal communication
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
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

# ---------- IAM Role ----------
resource "aws_iam_role" "druid" {
  name = "${var.project_name}-${var.environment}-druid-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "druid_s3" {
  name = "druid-s3-access"
  role = aws_iam_role.druid.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = [
        "arn:aws:s3:::${var.s3_deep_storage_bucket}",
        "arn:aws:s3:::${var.s3_deep_storage_bucket}/*"
      ]
    }]
  })
}

resource "aws_iam_instance_profile" "druid" {
  name = "${var.project_name}-${var.environment}-druid-profile"
  role = aws_iam_role.druid.name
}

# ---------- Launch Templates for each Druid node type ----------
locals {
  druid_nodes = {
    coordinator = {
      instance_type = "r8g.2xlarge"
      count         = 2
      volume_size   = 100
    }
    broker = {
      instance_type = "r8g.4xlarge"
      count         = 4
      volume_size   = 200
    }
    historical = {
      instance_type = "r8g.8xlarge"
      count         = 8
      volume_size   = 2000
    }
    middlemanager = {
      instance_type = "r8g.4xlarge"
      count         = 6
      volume_size   = 500
    }
    router = {
      instance_type = "r8g.xlarge"
      count         = 2
      volume_size   = 50
    }
  }
}

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-arm64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_launch_template" "druid" {
  for_each      = local.druid_nodes
  name_prefix   = "${var.project_name}-${var.environment}-druid-${each.key}-"
  image_id      = data.aws_ami.amazon_linux.id
  instance_type = each.value.instance_type

  iam_instance_profile {
    arn = aws_iam_instance_profile.druid.arn
  }

  vpc_security_group_ids = [aws_security_group.druid.id]

  block_device_mappings {
    device_name = "/dev/xvdf"
    ebs {
      volume_size           = each.value.volume_size
      volume_type           = "gp3"
      encrypted             = true
      delete_on_termination = true
    }
  }

  tag_specifications {
    resource_type = "instance"
    tags = merge(var.tags, {
      Name      = "${var.project_name}-${var.environment}-druid-${each.key}"
      Component = "druid"
      NodeType  = each.key
    })
  }

  metadata_options {
    http_tokens = "required"
  }

  tags = var.tags
}

resource "aws_autoscaling_group" "druid" {
  for_each            = local.druid_nodes
  name                = "${var.project_name}-${var.environment}-druid-${each.key}"
  desired_capacity    = each.value.count
  max_size            = each.value.count * 2
  min_size            = each.value.count
  vpc_zone_identifier = var.subnet_ids

  launch_template {
    id      = aws_launch_template.druid[each.key].id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "${var.project_name}-${var.environment}-druid-${each.key}"
    propagate_at_launch = true
  }
}

# ---------- Outputs ----------
output "security_group_id" {
  value = aws_security_group.druid.id
}

output "broker_endpoint" {
  description = "Internal endpoint for Druid Broker queries"
  value       = "${var.project_name}-${var.environment}-druid-broker.internal:8082"
}

output "coordinator_endpoint" {
  description = "Internal endpoint for Druid Coordinator"
  value       = "${var.project_name}-${var.environment}-druid-coordinator.internal:8081"
}
