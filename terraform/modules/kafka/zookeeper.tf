###############################################################################
# Kafka Module - ZooKeeper Ensemble (3-node with auto-discovery)
###############################################################################

# ---------- ZooKeeper Security Group ----------
resource "aws_security_group" "zookeeper" {
  name_prefix = "${var.project_name}-${var.environment}-zookeeper-"
  vpc_id      = var.vpc_id

  # Client port
  ingress {
    from_port       = 2181
    to_port         = 2181
    protocol        = "tcp"
    security_groups = [aws_security_group.kafka.id]
    description     = "ZooKeeper client from Kafka"
  }

  ingress {
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    self        = true
    description = "ZooKeeper client from peers"
  }

  # Peer communication
  ingress {
    from_port   = 2888
    to_port     = 2888
    protocol    = "tcp"
    self        = true
    description = "ZooKeeper peer port"
  }

  # Leader election
  ingress {
    from_port   = 3888
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
    Name = "${var.project_name}-${var.environment}-zookeeper-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- ZooKeeper IAM Role ----------
resource "aws_iam_role" "zookeeper" {
  name = "${var.project_name}-${var.environment}-zookeeper-role"

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

resource "aws_iam_role_policy" "zookeeper_discovery" {
  name = "zookeeper-auto-discovery"
  role = aws_iam_role.zookeeper.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ec2:DescribeInstances",
          "ec2:DescribeTags",
          "autoscaling:DescribeAutoScalingGroups"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "zookeeper" {
  name = "${var.project_name}-${var.environment}-zookeeper-profile"
  role = aws_iam_role.zookeeper.name
}

# ---------- ZooKeeper Launch Template ----------
resource "aws_launch_template" "zookeeper" {
  name_prefix   = "${var.project_name}-${var.environment}-zookeeper-"
  image_id      = data.aws_ami.amazon_linux.id
  instance_type = "r6g.xlarge"

  iam_instance_profile {
    arn = aws_iam_instance_profile.zookeeper.arn
  }

  vpc_security_group_ids = [aws_security_group.zookeeper.id]

  block_device_mappings {
    device_name = "/dev/xvdf"
    ebs {
      volume_size           = 100
      volume_type           = "gp3"
      iops                  = 3000
      throughput            = 125
      encrypted             = true
      delete_on_termination = false
    }
  }

  user_data = base64encode(<<-USERDATA
#!/bin/bash
set -euo pipefail

# Install dependencies
yum install -y java-17-amazon-corretto aws-cli jq

# ZooKeeper version
ZK_VERSION="3.8.4"
ZK_HOME="/opt/zookeeper"
ZK_DATA="/data/zookeeper"

# Download and install ZooKeeper
cd /opt
wget "https://downloads.apache.org/zookeeper/zookeeper-$${ZK_VERSION}/apache-zookeeper-$${ZK_VERSION}-bin.tar.gz"
tar xzf "apache-zookeeper-$${ZK_VERSION}-bin.tar.gz"
mv "apache-zookeeper-$${ZK_VERSION}-bin" "$${ZK_HOME}"

# Create data directory
mkdir -p "$${ZK_DATA}"

# Auto-discover ZooKeeper peers using EC2 tags
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)

# Get all ZooKeeper instances in the ASG
ZK_INSTANCES=$(aws ec2 describe-instances \
  --region "$${REGION}" \
  --filters "Name=tag:Component,Values=zookeeper" \
            "Name=tag:Name,Values=${var.project_name}-${var.environment}-zookeeper" \
            "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].{Id:InstanceId,Ip:PrivateIpAddress}' \
  --output json)

# Determine this node's myid based on sorted instance IDs
SORTED_IDS=$(echo "$${ZK_INSTANCES}" | jq -r '.[].Id' | sort)
MY_ID=1
for id in $${SORTED_IDS}; do
  if [ "$${id}" == "$${INSTANCE_ID}" ]; then
    break
  fi
  MY_ID=$((MY_ID + 1))
done

echo "$${MY_ID}" > "$${ZK_DATA}/myid"

# Generate zoo.cfg
cat > "$${ZK_HOME}/conf/zoo.cfg" <<EOF
tickTime=2000
initLimit=10
syncLimit=5
dataDir=$${ZK_DATA}
clientPort=2181
maxClientCnxns=60
autopurge.snapRetainCount=3
autopurge.purgeInterval=1
4lw.commands.whitelist=*
EOF

# Add server entries for each peer
SERVER_ID=1
for id in $${SORTED_IDS}; do
  IP=$(echo "$${ZK_INSTANCES}" | jq -r ".[] | select(.Id==\"$${id}\") | .Ip")
  echo "server.$${SERVER_ID}=$${IP}:2888:3888" >> "$${ZK_HOME}/conf/zoo.cfg"
  SERVER_ID=$((SERVER_ID + 1))
done

# Start ZooKeeper
$${ZK_HOME}/bin/zkServer.sh start
USERDATA
  )

  tag_specifications {
    resource_type = "instance"
    tags = merge(var.tags, {
      Name      = "${var.project_name}-${var.environment}-zookeeper"
      Component = "zookeeper"
    })
  }

  metadata_options {
    http_tokens = "required"
  }

  tags = var.tags
}

# ---------- ZooKeeper Auto Scaling Group (3-node ensemble) ----------
resource "aws_autoscaling_group" "zookeeper" {
  name                = "${var.project_name}-${var.environment}-zookeeper-asg"
  desired_capacity    = 3
  max_size            = 3
  min_size            = 3
  vpc_zone_identifier = var.subnet_ids

  launch_template {
    id      = aws_launch_template.zookeeper.id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "${var.project_name}-${var.environment}-zookeeper"
    propagate_at_launch = true
  }

  tag {
    key                 = "Component"
    value               = "zookeeper"
    propagate_at_launch = true
  }
}

# ---------- CloudWatch Log Group ----------
resource "aws_cloudwatch_log_group" "zookeeper" {
  name              = "/ec2/${var.project_name}-${var.environment}/zookeeper"
  retention_in_days = 30
  tags              = var.tags
}
