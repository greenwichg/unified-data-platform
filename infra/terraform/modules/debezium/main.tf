###############################################################################
# Debezium Module - Kafka Connect workers for CDC (Pipeline 2)
# Captures change events from Aurora MySQL and streams to Kafka/MSK
# Runs on ECS Fargate with distributed Kafka Connect mode
###############################################################################

# ---------- Security Group ----------
resource "aws_security_group" "debezium" {
  name_prefix = "${var.project_name}-${var.environment}-debezium-"
  vpc_id      = var.vpc_id

  # Kafka Connect REST API
  ingress {
    from_port   = 8083
    to_port     = 8083
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Kafka Connect REST API"
  }

  # Prometheus JMX exporter
  ingress {
    from_port   = 9404
    to_port     = 9404
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "JMX Prometheus metrics"
  }

  # Internal cluster communication
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
    Name = "${var.project_name}-${var.environment}-debezium-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------- CloudWatch Log Group ----------
resource "aws_cloudwatch_log_group" "debezium" {
  name              = "/ecs/${var.project_name}-${var.environment}/debezium"
  retention_in_days = 30
  tags              = var.tags
}

# ---------- ECS Task Definition - Debezium Connect Worker ----------
resource "aws_ecs_task_definition" "debezium" {
  family                   = "${var.project_name}-${var.environment}-debezium"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.debezium_execution.arn
  task_role_arn            = aws_iam_role.debezium_task.arn

  container_definitions = jsonencode([{
    name  = "debezium-connect"
    image = var.debezium_image

    portMappings = [
      {
        containerPort = 8083
        protocol      = "tcp"
      },
      {
        containerPort = 9404
        protocol      = "tcp"
      }
    ]

    environment = [
      { name = "GROUP_ID", value = "zomato-cdc-${var.environment}" },
      { name = "BOOTSTRAP_SERVERS", value = var.kafka_bootstrap_servers },
      { name = "CONFIG_STORAGE_TOPIC", value = "debezium-configs" },
      { name = "OFFSET_STORAGE_TOPIC", value = "debezium-offsets" },
      { name = "STATUS_STORAGE_TOPIC", value = "debezium-status" },
      { name = "CONFIG_STORAGE_REPLICATION_FACTOR", value = "3" },
      { name = "OFFSET_STORAGE_REPLICATION_FACTOR", value = "3" },
      { name = "STATUS_STORAGE_REPLICATION_FACTOR", value = "3" },
      { name = "CONNECT_KEY_CONVERTER", value = "io.confluent.connect.avro.AvroConverter" },
      { name = "CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL", value = var.schema_registry_url },
      { name = "CONNECT_VALUE_CONVERTER", value = "io.confluent.connect.avro.AvroConverter" },
      { name = "CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", value = var.schema_registry_url },
      { name = "CONNECT_PRODUCER_ACKS", value = "all" },
      { name = "CONNECT_PRODUCER_RETRIES", value = "2147483647" },
      { name = "CONNECT_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION", value = "1" },
      { name = "CONNECT_CONSUMER_AUTO_OFFSET_RESET", value = "earliest" },
      { name = "CONNECT_REST_ADVERTISED_HOST_NAME", value = "debezium-connect" },
      { name = "CONNECT_REST_PORT", value = "8083" },
      { name = "JMXPORT", value = "9404" },
      { name = "KAFKA_OPTS", value = "-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9404:/opt/jmx-exporter/config.yml" },
      { name = "HEAP_OPTS", value = "-Xms${var.jvm_heap_mb}m -Xmx${var.jvm_heap_mb}m" },
    ]

    secrets = [
      {
        name      = "CONNECT_SASL_JAAS_CONFIG"
        valueFrom = var.kafka_sasl_secret_arn
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.debezium.name
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "debezium"
      }
    }

    healthCheck = {
      command     = ["CMD-SHELL", "curl -f http://localhost:8083/connectors || exit 1"]
      interval    = 30
      timeout     = 10
      retries     = 5
      startPeriod = 120
    }
  }])

  tags = var.tags
}

# ---------- ECS Service ----------
resource "aws_ecs_service" "debezium" {
  name            = "debezium-connect"
  cluster         = var.ecs_cluster_id
  task_definition = aws_ecs_task_definition.debezium.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.private_subnet_ids
    security_groups = [aws_security_group.debezium.id]
  }

  service_registries {
    registry_arn = aws_service_discovery_service.debezium.arn
  }

  deployment_configuration {
    minimum_healthy_percent = 50
    maximum_percent         = 200
  }

  tags = var.tags
}

# ---------- Service Discovery ----------
resource "aws_service_discovery_service" "debezium" {
  name = "debezium-connect"

  dns_config {
    namespace_id = var.service_discovery_namespace_id

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

# ---------- Auto Scaling ----------
resource "aws_appautoscaling_target" "debezium" {
  max_capacity       = var.max_count
  min_capacity       = var.desired_count
  resource_id        = "service/${var.ecs_cluster_name}/${aws_ecs_service.debezium.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "debezium_cpu" {
  name               = "${var.project_name}-${var.environment}-debezium-cpu"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.debezium.resource_id
  scalable_dimension = aws_appautoscaling_target.debezium.scalable_dimension
  service_namespace  = aws_appautoscaling_target.debezium.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
    target_value       = 65.0
    scale_in_cooldown  = 600
    scale_out_cooldown = 120
  }
}

# ---------- CloudWatch Alarms ----------
resource "aws_cloudwatch_metric_alarm" "debezium_cpu_high" {
  alarm_name          = "${var.project_name}-${var.environment}-debezium-cpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "CPUUtilization"
  namespace           = "AWS/ECS"
  period              = 300
  statistic           = "Average"
  threshold           = 85
  alarm_description   = "Debezium Connect CPU utilization exceeds 85%"
  alarm_actions       = var.alarm_sns_topic_arns

  dimensions = {
    ClusterName = var.ecs_cluster_name
    ServiceName = aws_ecs_service.debezium.name
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "debezium_memory_high" {
  alarm_name          = "${var.project_name}-${var.environment}-debezium-memory-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "MemoryUtilization"
  namespace           = "AWS/ECS"
  period              = 300
  statistic           = "Average"
  threshold           = 85
  alarm_description   = "Debezium Connect memory utilization exceeds 85%"
  alarm_actions       = var.alarm_sns_topic_arns

  dimensions = {
    ClusterName = var.ecs_cluster_name
    ServiceName = aws_ecs_service.debezium.name
  }

  tags = var.tags
}

data "aws_region" "current" {}
