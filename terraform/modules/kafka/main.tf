###############################################################################
# Kafka Module - Self-Hosted Kafka Cluster on EC2
# 450M+ messages per minute across all pipelines
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
  description = "Private subnet IDs for Kafka brokers"
  type        = list(string)
}

variable "instance_type" {
  description = "EC2 instance type for Kafka brokers"
  type        = string
  default     = "r8g.4xlarge"
}

variable "broker_count" {
  description = "Number of Kafka brokers"
  type        = number
  default     = 9
}

variable "ebs_volume_size" {
  description = "EBS volume size in GB per broker"
  type        = number
  default     = 2000
}

variable "kafka_version" {
  type    = string
  default = "3.6.1"
}

variable "tags" {
  type    = map(string)
  default = {}
}

# ---------- Security Group ----------
resource "aws_security_group" "kafka" {
  name_prefix = "${var.project_name}-${var.environment}-kafka-"
  vpc_id      = var.vpc_id

  # Kafka broker port
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    self        = true
    description = "Kafka broker communication"
  }

  # Kafka SSL port
  ingress {
    from_port   = 9093
    to_port     = 9093
    protocol    = "tcp"
    self        = true
    description = "Kafka SSL communication"
  }

  # ZooKeeper ports
  ingress {
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    self        = true
    description = "ZooKeeper client"
  }

  ingress {
    from_port   = 2888
    to_port     = 3888
    protocol    = "tcp"
    self        = true
    description = "ZooKeeper leader election"
  }

  # JMX monitoring
  ingress {
    from_port   = 9999
    to_port     = 9999
    protocol    = "tcp"
    self        = true
    description = "JMX monitoring"
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

# ---------- IAM Role for Kafka EC2 Instances ----------
resource "aws_iam_role" "kafka" {
  name = "${var.project_name}-${var.environment}-kafka-role"

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

resource "aws_iam_role_policy" "kafka_s3" {
  name = "kafka-s3-access"
  role = aws_iam_role.kafka.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ]
      Resource = ["*"]
    }]
  })
}

resource "aws_iam_instance_profile" "kafka" {
  name = "${var.project_name}-${var.environment}-kafka-profile"
  role = aws_iam_role.kafka.name
}

# ---------- Launch Template ----------
resource "aws_launch_template" "kafka" {
  name_prefix   = "${var.project_name}-${var.environment}-kafka-"
  image_id      = data.aws_ami.amazon_linux.id
  instance_type = var.instance_type

  iam_instance_profile {
    arn = aws_iam_instance_profile.kafka.arn
  }

  vpc_security_group_ids = [aws_security_group.kafka.id]

  block_device_mappings {
    device_name = "/dev/xvdf"
    ebs {
      volume_size           = var.ebs_volume_size
      volume_type           = "gp3"
      iops                  = 16000
      throughput            = 1000
      encrypted             = true
      delete_on_termination = false
    }
  }

  user_data = base64encode(templatefile("${path.module}/userdata.sh.tpl", {
    kafka_version   = var.kafka_version
    broker_count    = var.broker_count
    environment     = var.environment
  }))

  tag_specifications {
    resource_type = "instance"
    tags = merge(var.tags, {
      Name      = "${var.project_name}-${var.environment}-kafka-broker"
      Component = "kafka"
    })
  }

  metadata_options {
    http_tokens = "required"
  }

  tags = var.tags
}

# ---------- Auto Scaling Group ----------
resource "aws_autoscaling_group" "kafka" {
  name                = "${var.project_name}-${var.environment}-kafka-asg"
  desired_capacity    = var.broker_count
  max_size            = var.broker_count + 3
  min_size            = var.broker_count
  vpc_zone_identifier = var.subnet_ids

  launch_template {
    id      = aws_launch_template.kafka.id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "${var.project_name}-${var.environment}-kafka-broker"
    propagate_at_launch = true
  }

  tag {
    key                 = "Component"
    value               = "kafka"
    propagate_at_launch = true
  }
}

# ---------- AMI Lookup ----------
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

# ---------- Outputs ----------
output "security_group_id" {
  value = aws_security_group.kafka.id
}

output "broker_asg_name" {
  value = aws_autoscaling_group.kafka.name
}
