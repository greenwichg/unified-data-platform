# DEPRECATED: Trino replaced by Amazon Athena (serverless). Kept for local dev only.
# =============================================================================
# Zomato Data Platform - Trino Single-Node (Local Development)
# Trino with Iceberg, MySQL, and Hive Metastore catalogs configured
#
# PRODUCTION NOTE: In production, Amazon Athena (serverless) is used instead
# of self-hosted Trino on ECS. This Dockerfile is retained only for local
# development and testing.
# =============================================================================
FROM trinodb/trino:latest

LABEL maintainer="data-engineering@zomato.com"
LABEL description="Trino single-node for local development with Iceberg catalog"

USER root

# Copy catalog configuration files
COPY catalog/ /etc/trino/catalog/
COPY config.properties /etc/trino/config.properties

# Create JVM config optimized for local development
RUN echo "-server" > /etc/trino/jvm.config && \
    echo "-Xmx2G" >> /etc/trino/jvm.config && \
    echo "-XX:InitialRAMPercentage=40" >> /etc/trino/jvm.config && \
    echo "-XX:MaxRAMPercentage=80" >> /etc/trino/jvm.config && \
    echo "-XX:+UseG1GC" >> /etc/trino/jvm.config && \
    echo "-XX:G1HeapRegionSize=32M" >> /etc/trino/jvm.config && \
    echo "-XX:+ExplicitGCInvokesConcurrent" >> /etc/trino/jvm.config && \
    echo "-XX:+HeapDumpOnOutOfMemoryError" >> /etc/trino/jvm.config && \
    echo "-XX:HeapDumpPath=/tmp/trino-heap-dump.hprof" >> /etc/trino/jvm.config && \
    echo "-Djdk.attach.allowAttachSelf=true" >> /etc/trino/jvm.config

# Node properties
RUN echo "node.environment=local-dev" > /etc/trino/node.properties && \
    echo "node.id=trino-local-1" >> /etc/trino/node.properties && \
    echo "node.data-dir=/data/trino" >> /etc/trino/node.properties

# Log levels for development
RUN echo "io.trino=INFO" > /etc/trino/log.properties && \
    echo "io.trino.plugin.iceberg=DEBUG" >> /etc/trino/log.properties

# Create data directories
RUN mkdir -p /data/trino && chown -R trino:trino /data/trino

USER trino

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --retries=10 --start-period=30s \
    CMD curl -f http://localhost:8080/v1/info | grep -q '"starting":false' || exit 1
