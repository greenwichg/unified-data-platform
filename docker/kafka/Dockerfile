# DEPRECATED: Self-hosted Kafka replaced by Amazon MSK. Kept for local dev only.
# =============================================================================
# Zomato Data Platform - Kafka (Local Development)
# Single-broker Kafka for local testing of CDC and event pipelines
#
# PRODUCTION NOTE: In production, Amazon MSK with IAM authentication is used
# instead of self-hosted Kafka on EC2. This Dockerfile is retained only for
# local development and testing.
# =============================================================================
FROM confluentinc/cp-kafka:7.6.0

LABEL maintainer="data-engineering@zomato.com"
LABEL description="Kafka broker for Zomato data platform local development"

# Environment defaults for single-broker local development
ENV KAFKA_BROKER_ID=1 \
    KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
    KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka-1:29092,PLAINTEXT_HOST://localhost:9092 \
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
    KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    KAFKA_NUM_PARTITIONS=6 \
    KAFKA_DEFAULT_REPLICATION_FACTOR=1 \
    KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    KAFKA_LOG_RETENTION_HOURS=24 \
    KAFKA_LOG_RETENTION_BYTES=1073741824 \
    KAFKA_COMPRESSION_TYPE=lz4 \
    KAFKA_MESSAGE_MAX_BYTES=10485760 \
    KAFKA_LOG_SEGMENT_BYTES=536870912 \
    KAFKA_LOG_CLEANUP_POLICY=delete \
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE=false \
    KAFKA_JMX_PORT=9999 \
    KAFKA_JMX_HOSTNAME=localhost \
    KAFKA_HEAP_OPTS="-Xmx512m -Xms512m"

EXPOSE 9092 29092 9999

HEALTHCHECK --interval=15s --timeout=10s --retries=5 --start-period=30s \
    CMD kafka-broker-api-versions --bootstrap-server localhost:9092 || exit 1
