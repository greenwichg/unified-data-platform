# =============================================================================
# Zomato Data Platform - Flink with Custom JARs
# Flink 1.18 with Kafka, Iceberg, and Avro connectors for CDC and event processing
# =============================================================================
FROM flink:1.18-java11

LABEL maintainer="data-engineering@zomato.com"
LABEL description="Apache Flink with Kafka, Iceberg, and Avro connectors"

# Connector versions matching the Flink 1.18 compatibility matrix
ENV FLINK_VERSION=1.18.1 \
    ICEBERG_VERSION=1.4.3 \
    KAFKA_CONNECTOR_VERSION=3.0.2-1.18 \
    AVRO_VERSION=1.18.1

# Download Kafka connector
RUN wget -q -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"

# Download Iceberg Flink runtime
RUN wget -q -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/${ICEBERG_VERSION}/iceberg-flink-runtime-1.18-${ICEBERG_VERSION}.jar"

# Download Avro format
RUN wget -q -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/${AVRO_VERSION}/flink-sql-avro-confluent-registry-${AVRO_VERSION}.jar"

# Download AWS S3 filesystem plugin for checkpoints and state backend
RUN wget -q -P /opt/flink/plugins/s3-fs-hadoop/ \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}/flink-s3-fs-hadoop-${FLINK_VERSION}.jar"

# Download Hadoop AWS for S3A access
RUN wget -q -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar" && \
    wget -q -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.600/aws-java-sdk-bundle-1.12.600.jar"

# Copy custom Flink configuration
COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml 2>/dev/null || true

# Set Flink defaults for local development
ENV FLINK_PROPERTIES="\
jobmanager.rpc.address: flink-jobmanager\n\
state.backend: hashmap\n\
state.checkpoints.dir: s3://zomato-data-platform-dev-checkpoints/flink/checkpoints\n\
state.savepoints.dir: s3://zomato-data-platform-dev-checkpoints/flink/savepoints\n\
execution.checkpointing.interval: 60000\n\
execution.checkpointing.min-pause: 30000\n\
execution.checkpointing.timeout: 600000\n\
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION\n\
restart-strategy: fixed-delay\n\
restart-strategy.fixed-delay.attempts: 3\n\
restart-strategy.fixed-delay.delay: 30s\n\
taskmanager.numberOfTaskSlots: 4\n\
taskmanager.memory.process.size: 4096m\n\
parallelism.default: 2\n\
"

EXPOSE 6123 8081

HEALTHCHECK --interval=15s --timeout=10s --retries=5 --start-period=45s \
    CMD curl -f http://localhost:8081/overview || exit 1
