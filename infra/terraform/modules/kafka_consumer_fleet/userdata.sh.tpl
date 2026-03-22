#!/bin/bash
set -euo pipefail

# ============================================================================
# Kafka Consumer Fleet - EC2 Userdata
# Pipeline 4: MSK Cluster 1 -> Consumer Fleet -> MSK Cluster 2 -> Druid
# ============================================================================

echo ">>> Starting Kafka Consumer Fleet setup for ${project_name} (${environment})"

# Install dependencies
dnf install -y python3.11 python3.11-pip jq

# Create application directory
mkdir -p /opt/kafka-consumer-fleet
cd /opt/kafka-consumer-fleet

# Install Python dependencies
python3.11 -m pip install \
  confluent-kafka==2.3.0 \
  boto3==1.34.0 \
  --quiet

# Write configuration file
cat > /opt/kafka-consumer-fleet/config.json <<'CFGEOF'
{
  "primary_bootstrap_servers": "${kafka_primary_bootstrap}",
  "secondary_bootstrap_servers": "${kafka_secondary_bootstrap}",
  "source_topics": "${source_topics}",
  "destination_topic": "${destination_topic}",
  "consumer_group_id": "${consumer_group_id}",
  "cloudwatch_namespace": "${cloudwatch_namespace}",
  "environment": "${environment}"
}
CFGEOF

# Create systemd service
cat > /etc/systemd/system/kafka-consumer-fleet.service <<'SVCEOF'
[Unit]
Description=Kafka Consumer Fleet - Druid Feeder
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/opt/kafka-consumer-fleet
ExecStart=/usr/bin/python3.11 /opt/kafka-consumer-fleet/kafka_consumer_autoscaling.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

# Graceful shutdown (SIGTERM)
KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
SVCEOF

# Download application code from S3 (deployed by CI/CD)
aws s3 cp "s3://${project_name}-${environment}-artifacts/consumer-fleet/kafka_consumer_autoscaling.py" \
  /opt/kafka-consumer-fleet/kafka_consumer_autoscaling.py || echo "WARN: S3 download failed, code must be deployed separately"

# Start the service
systemctl daemon-reload
systemctl enable kafka-consumer-fleet
systemctl start kafka-consumer-fleet || echo "WARN: Service start deferred until code is deployed"

echo ">>> Kafka Consumer Fleet setup complete"
