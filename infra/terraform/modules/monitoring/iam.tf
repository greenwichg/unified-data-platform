###############################################################################
# Monitoring Module - IAM Roles and Policies
###############################################################################

# ---------- CloudWatch Alarm Actions Role ----------
resource "aws_iam_role" "monitoring_alarm_actions" {
  name = "${var.project_name}-${var.environment}-monitoring-alarm-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "cloudwatch.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "monitoring_alarm_actions" {
  name = "monitoring-alarm-actions"
  role = aws_iam_role.monitoring_alarm_actions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = [aws_sns_topic.alerts.arn]
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

# ---------- Grafana CloudWatch Role (for external monitoring) ----------
resource "aws_iam_role" "grafana_cloudwatch" {
  name = "${var.project_name}-${var.environment}-grafana-cloudwatch-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "grafana.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "grafana_cloudwatch" {
  name = "grafana-cloudwatch-readonly"
  role = aws_iam_role.grafana_cloudwatch.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "cloudwatch:DescribeAlarms",
        "cloudwatch:DescribeAlarmsForMetric",
        "cloudwatch:DescribeAnomalyDetectors",
        "cloudwatch:GetDashboard",
        "cloudwatch:GetMetricData",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:ListDashboards",
        "cloudwatch:ListMetrics",
        "logs:DescribeLogGroups",
        "logs:GetLogEvents",
        "logs:GetQueryResults",
        "logs:StartQuery",
        "logs:StopQuery"
      ]
      Resource = "*"
    }]
  })
}
