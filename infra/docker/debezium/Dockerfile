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

# Install AWS MSK IAM Auth library for MSK connectivity
RUN mkdir -p /kafka/connect/msk-iam-auth && \
    curl -sL "https://github.com/aws/aws-msk-iam-auth/releases/download/v2.0.3/aws-msk-iam-auth-2.0.3-all.jar" \
        -o /kafka/connect/msk-iam-auth/aws-msk-iam-auth-2.0.3-all.jar

# Install AWS Glue Schema Registry libraries for Glue Schema Registry integration
RUN mkdir -p /kafka/connect/glue-schema-registry && \
    curl -sL "https://repo1.maven.org/maven2/software/amazon/glue/schema-registry-kafkaconnect-converter/1.1.19/schema-registry-kafkaconnect-converter-1.1.19.jar" \
        -o /kafka/connect/glue-schema-registry/schema-registry-kafkaconnect-converter-1.1.19.jar

# Install SMT (Single Message Transforms) for data enrichment
RUN mkdir -p /kafka/connect/transforms

# Environment defaults for local development
ENV GROUP_ID=zomato-connect \
    BOOTSTRAP_SERVERS=b-1.zomato-msk.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-2.zomato-msk.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-3.zomato-msk.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098 \
    CONFIG_STORAGE_TOPIC=debezium-configs \
    OFFSET_STORAGE_TOPIC=debezium-offsets \
    STATUS_STORAGE_TOPIC=debezium-status \
    CONFIG_STORAGE_REPLICATION_FACTOR=3 \
    OFFSET_STORAGE_REPLICATION_FACTOR=3 \
    STATUS_STORAGE_REPLICATION_FACTOR=3 \
    CONNECT_SECURITY_PROTOCOL=SASL_SSL \
    CONNECT_SASL_MECHANISM=AWS_MSK_IAM \
    CONNECT_SASL_JAAS_CONFIG="software.amazon.msk.auth.iam.IAMLoginModule required;" \
    CONNECT_SASL_CLIENT_CALLBACK_HANDLER_CLASS=software.amazon.msk.auth.iam.IAMClientCallbackHandler \
    KEY_CONVERTER=com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter \
    VALUE_CONVERTER=com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter \
    CONNECT_INTERNAL_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
    CONNECT_INTERNAL_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
    KAFKA_HEAP_OPTS="-Xmx512m -Xms256m" \
    JMXPORT=9012

USER kafka

EXPOSE 8083 9012

HEALTHCHECK --interval=15s --timeout=10s --retries=10 --start-period=60s \
    CMD curl -f http://localhost:8083/connectors || exit 1
