# =============================================================================
# Zomato Data Platform - Apache Druid (Local Development)
# Micro-quickstart configuration for local real-time OLAP testing
# =============================================================================
FROM apache/druid:29.0.0

LABEL maintainer="data-engineering@zomato.com"
LABEL description="Apache Druid micro-quickstart for local development"

USER root

# Install additional extensions for Kafka ingestion and S3 deep storage
ENV DRUID_EXTENSIONS='["druid-kafka-indexing-service", "druid-s3-extensions", "druid-avro-extensions", "druid-datasketches", "druid-lookups-cached-global", "druid-histogram", "druid-bloom-filter"]'

# Common runtime properties for local development
ENV druid_metadata_storage_type=derby \
    druid_metadata_storage_connector_connectURI=jdbc:derby://localhost:1527/var/druid/metadata.db;create=true \
    druid_storage_type=local \
    druid_storage_storageDirectory=/opt/druid/var/druid/segments \
    druid_indexer_logs_type=file \
    druid_indexer_logs_directory=/opt/druid/var/druid/indexing-logs \
    druid_processing_numThreads=2 \
    druid_processing_numMergeBuffers=2 \
    druid_processing_buffer_sizeBytes=100000000 \
    druid_emitter=noop \
    druid_extensions_loadList=${DRUID_EXTENSIONS}

# Create required directories
RUN mkdir -p /opt/druid/var/druid/segments \
             /opt/druid/var/druid/indexing-logs \
             /opt/druid/var/druid/task \
             /opt/druid/var/druid/metadata.db && \
    chown -R druid:druid /opt/druid/var

USER druid

EXPOSE 8081 8082 8083 8888 8091 8100

HEALTHCHECK --interval=15s --timeout=10s --retries=10 --start-period=60s \
    CMD curl -f http://localhost:8888/status/health || exit 1
