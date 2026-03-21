# =============================================================================
# Zomato Data Platform - Debezium Connect (Local Development)
# Debezium Kafka Connect with Avro converter and MySQL connector for CDC
# =============================================================================
FROM debezium/connect:2.4

LABEL maintainer="data-engineering@zomato.com"
LABEL description="Debezium Kafka Connect with Avro converter for CDC pipelines"

USER root

# Install Confluent Avro converter and Schema Registry client
ENV CONFLUENT_VERSION=7.6.0
RUN mkdir -p /kafka/connect/avro-converter && \
    cd /tmp && \
    curl -sL "https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/${CONFLUENT_VERSION}/kafka-connect-avro-converter-${CONFLUENT_VERSION}.jar" \
        -o /kafka/connect/avro-converter/kafka-connect-avro-converter-${CONFLUENT_VERSION}.jar && \
    curl -sL "https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/${CONFLUENT_VERSION}/kafka-schema-registry-client-${CONFLUENT_VERSION}.jar" \
        -o /kafka/connect/avro-converter/kafka-schema-registry-client-${CONFLUENT_VERSION}.jar && \
    curl -sL "https://packages.confluent.io/maven/io/confluent/common-config/${CONFLUENT_VERSION}/common-config-${CONFLUENT_VERSION}.jar" \
        -o /kafka/connect/avro-converter/common-config-${CONFLUENT_VERSION}.jar && \
    curl -sL "https://packages.confluent.io/maven/io/confluent/common-utils/${CONFLUENT_VERSION}/common-utils-${CONFLUENT_VERSION}.jar" \
        -o /kafka/connect/avro-converter/common-utils-${CONFLUENT_VERSION}.jar && \
    curl -sL "https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar" \
        -o /kafka/connect/avro-converter/avro-1.11.3.jar

# Install SMT (Single Message Transforms) for data enrichment
RUN mkdir -p /kafka/connect/transforms

# Environment defaults for local development
ENV GROUP_ID=zomato-connect \
    BOOTSTRAP_SERVERS=kafka-1:29092 \
    CONFIG_STORAGE_TOPIC=debezium-configs \
    OFFSET_STORAGE_TOPIC=debezium-offsets \
    STATUS_STORAGE_TOPIC=debezium-status \
    CONFIG_STORAGE_REPLICATION_FACTOR=1 \
    OFFSET_STORAGE_REPLICATION_FACTOR=1 \
    STATUS_STORAGE_REPLICATION_FACTOR=1 \
    KEY_CONVERTER=io.confluent.connect.avro.AvroConverter \
    VALUE_CONVERTER=io.confluent.connect.avro.AvroConverter \
    CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
    CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
    CONNECT_INTERNAL_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
    CONNECT_INTERNAL_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
    KAFKA_HEAP_OPTS="-Xmx512m -Xms256m" \
    JMXPORT=9012

USER kafka

EXPOSE 8083 9012

HEALTHCHECK --interval=15s --timeout=10s --retries=10 --start-period=60s \
    CMD curl -f http://localhost:8083/connectors || exit 1
