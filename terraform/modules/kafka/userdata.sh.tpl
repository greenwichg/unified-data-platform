#!/bin/bash
set -euo pipefail

# Install Java
dnf install -y java-17-amazon-corretto

# Create kafka user
useradd -r -s /sbin/nologin kafka

# Download and install Kafka
KAFKA_VERSION="${kafka_version}"
cd /opt
curl -sL "https://downloads.apache.org/kafka/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.tgz" | tar xz
ln -s "kafka_2.13-$KAFKA_VERSION" kafka
chown -R kafka:kafka /opt/kafka*

# Format and mount data volume
mkfs.xfs /dev/xvdf
mkdir -p /data/kafka
mount /dev/xvdf /data/kafka
echo "/dev/xvdf /data/kafka xfs defaults,nofail 0 2" >> /etc/fstab
chown kafka:kafka /data/kafka

# Get instance metadata
TOKEN=$(curl -sX PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
INSTANCE_ID=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
PRIVATE_IP=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)

# Derive broker ID from instance ID hash (1-based)
BROKER_ID=$(echo "$INSTANCE_ID" | md5sum | tr -d 'a-f' | head -c 4 | sed 's/^0*//' | head -c 3)
BROKER_ID=$${BROKER_ID:-1}

# Configure Kafka
cat > /opt/kafka/config/server.properties << CONF
broker.id=$BROKER_ID
listeners=PLAINTEXT://$PRIVATE_IP:9092
advertised.listeners=PLAINTEXT://$PRIVATE_IP:9092
log.dirs=/data/kafka

# High-throughput settings for 450M msgs/min
num.network.threads=16
num.io.threads=16
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
socket.request.max.bytes=104857600

# Log settings
num.partitions=64
default.replication.factor=3
min.insync.replicas=2
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

# ZooKeeper (will be replaced with KRaft in future)
zookeeper.connect=zookeeper.${environment}.internal:2181
zookeeper.connection.timeout.ms=18000

# Performance tuning
replica.fetch.max.bytes=10485760
message.max.bytes=10485760
compression.type=lz4
log.cleaner.enable=true
auto.create.topics.enable=false
CONF

# Create systemd service
cat > /etc/systemd/system/kafka.service << SVC
[Unit]
Description=Apache Kafka
After=network.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment="KAFKA_HEAP_OPTS=-Xmx12g -Xms12g"
Environment="KAFKA_JVM_PERFORMANCE_OPTS=-XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
SVC

systemctl daemon-reload
systemctl enable kafka
systemctl start kafka
