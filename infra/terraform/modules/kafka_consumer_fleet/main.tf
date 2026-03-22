###############################################################################
# Kafka Consumer Fleet Module - EC2 Auto-Scaling Group
# Pipeline 4: Consumes from primary MSK, produces to secondary MSK for Druid
# Architecture: MSK Cluster 1 -> [EC2 ASG Consumer Fleet] -> MSK Cluster 2 -> Druid
###############################################################################

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# ---------- Security Group ----------
resource "aws_security_group" "consumer_fleet" {
  name_prefix = "${var.project_name}-${var.environment}-consumer-fleet-"
  vpc_id      = var.vpc_id

  # Outbound to MSK clusters (TLS + IAM auth ports)
  egress {
    from_port   = 9092
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "MSK broker access (TLS + IAM)"
  }

  # Outbound HTTPS for CloudWatch, S3, SSM
  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS for AWS APIs"
  }

  # Outbound DNS
  egress {
    from_port   = 53
    to_port     = 53
    protocol    = "udp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "DNS"
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-consumer-fleet-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# Allow consumer fleet to connect to both MSK clusters
resource "aws_security_group_rule" "kafka_primary_from_consumers" {
  type                     = "ingress"
  from_port                = 9092
  to_port                  = 9098
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.consumer_fleet.id
  security_group_id        = var.kafka_primary_security_group_id
  description              = "MSK access from consumer fleet"
}

resource "aws_security_group_rule" "kafka_secondary_from_consumers" {
  type                     = "ingress"
  from_port                = 9092
  to_port                  = 9098
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.consumer_fleet.id
  security_group_id        = var.kafka_secondary_security_group_id
  description              = "MSK access from consumer fleet"
}

# ---------- IAM Role ----------
resource "aws_iam_role" "consumer_fleet" {
  name = "${var.project_name}-${var.environment}-consumer-fleet-role"

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

resource "aws_iam_role_policy" "consumer_fleet_msk" {
  name = "consumer-fleet-msk-access"
  role = aws_iam_role.consumer_fleet.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeCluster",
        "kafka-cluster:DescribeClusterV2",
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:ReadData",
        "kafka-cluster:WriteData",
        "kafka-cluster:DescribeGroup",
        "kafka-cluster:AlterGroup"
      ]
      Resource = [
        var.kafka_primary_cluster_arn,
        "${var.kafka_primary_cluster_arn}/*",
        var.kafka_secondary_cluster_arn,
        "${var.kafka_secondary_cluster_arn}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "consumer_fleet_cloudwatch" {
  name = "consumer-fleet-cloudwatch"
  role = aws_iam_role.consumer_fleet.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "cloudwatch:PutMetricData",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy" "consumer_fleet_ssm" {
  name = "consumer-fleet-ssm"
  role = aws_iam_role.consumer_fleet.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "ssm:GetParameter",
        "ssm:GetParameters",
        "ssm:GetParametersByPath"
      ]
      Resource = "arn:aws:ssm:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:parameter/${var.project_name}/*"
    }]
  })
}

resource "aws_iam_instance_profile" "consumer_fleet" {
  name = "${var.project_name}-${var.environment}-consumer-fleet-profile"
  role = aws_iam_role.consumer_fleet.name
}

# ---------- Launch Template ----------
data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_launch_template" "consumer_fleet" {
  name_prefix   = "${var.project_name}-${var.environment}-consumer-fleet-"
  image_id      = data.aws_ami.amazon_linux.id
  instance_type = var.instance_type

  iam_instance_profile {
    arn = aws_iam_instance_profile.consumer_fleet.arn
  }

  vpc_security_group_ids = [aws_security_group.consumer_fleet.id]

  user_data = base64encode(templatefile("${path.module}/userdata.sh.tpl", {
    project_name                   = var.project_name
    environment                    = var.environment
    kafka_primary_bootstrap        = var.kafka_primary_bootstrap_servers
    kafka_secondary_bootstrap      = var.kafka_secondary_bootstrap_servers
    source_topics                  = join(",", var.source_topics)
    destination_topic              = var.destination_topic
    consumer_group_id              = var.consumer_group_id
    cloudwatch_namespace           = "${var.project_name}/${var.environment}/kafka-consumer-fleet"
  }))

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = 50
      volume_type           = "gp3"
      encrypted             = true
      delete_on_termination = true
    }
  }

  tag_specifications {
    resource_type = "instance"
    tags = merge(var.tags, {
      Name      = "${var.project_name}-${var.environment}-consumer-fleet"
      Component = "kafka-consumer-fleet"
      Pipeline  = "pipeline4-realtime"
    })
  }

  metadata_options {
    http_tokens = "required"
  }

  tags = var.tags
}

# ---------- Auto-Scaling Group ----------
resource "aws_autoscaling_group" "consumer_fleet" {
  name                = "${var.project_name}-${var.environment}-consumer-fleet"
  desired_capacity    = var.desired_capacity
  min_size            = var.min_size
  max_size            = var.max_size
  vpc_zone_identifier = var.subnet_ids

  launch_template {
    id      = aws_launch_template.consumer_fleet.id
    version = "$Latest"
  }

  health_check_type         = "EC2"
  health_check_grace_period = 300

  tag {
    key                 = "Name"
    value               = "${var.project_name}-${var.environment}-consumer-fleet"
    propagate_at_launch = true
  }

  tag {
    key                 = "Component"
    value               = "kafka-consumer-fleet"
    propagate_at_launch = true
  }
}

# ---------- Scaling Policies (driven by Kafka consumer lag) ----------
resource "aws_autoscaling_policy" "scale_out" {
  name                   = "${var.project_name}-${var.environment}-consumer-fleet-scale-out"
  autoscaling_group_name = aws_autoscaling_group.consumer_fleet.name
  policy_type            = "StepScaling"
  adjustment_type        = "ChangeInCapacity"

  step_adjustment {
    scaling_adjustment          = 2
    metric_interval_lower_bound = 0
    metric_interval_upper_bound = 100000
  }

  step_adjustment {
    scaling_adjustment          = 4
    metric_interval_lower_bound = 100000
  }
}

resource "aws_autoscaling_policy" "scale_in" {
  name                   = "${var.project_name}-${var.environment}-consumer-fleet-scale-in"
  autoscaling_group_name = aws_autoscaling_group.consumer_fleet.name
  policy_type            = "StepScaling"
  adjustment_type        = "ChangeInCapacity"

  step_adjustment {
    scaling_adjustment          = -1
    metric_interval_upper_bound = 0
  }
}

# ---------- CloudWatch Alarms for Scaling ----------
resource "aws_cloudwatch_metric_alarm" "consumer_lag_high" {
  alarm_name          = "${var.project_name}-${var.environment}-consumer-lag-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "TotalConsumerLag"
  namespace           = "${var.project_name}/${var.environment}/kafka-consumer-fleet"
  period              = 60
  statistic           = "Average"
  threshold           = var.scale_out_lag_threshold
  alarm_description   = "Scale out consumer fleet when Kafka consumer lag is high"
  alarm_actions       = [aws_autoscaling_policy.scale_out.arn]

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "consumer_lag_low" {
  alarm_name          = "${var.project_name}-${var.environment}-consumer-lag-low"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 5
  metric_name         = "TotalConsumerLag"
  namespace           = "${var.project_name}/${var.environment}/kafka-consumer-fleet"
  period              = 60
  statistic           = "Average"
  threshold           = var.scale_in_lag_threshold
  alarm_description   = "Scale in consumer fleet when Kafka consumer lag is low"
  alarm_actions       = [aws_autoscaling_policy.scale_in.arn]

  tags = var.tags
}
