# DEPRECATED: Hive Metastore replaced by AWS Glue Data Catalog. Kept for local dev only.
# =============================================================================
# Zomato Data Platform - Hive Metastore (Local Development)
# Standalone Hive Metastore Thrift server for Iceberg catalog metadata
# Used by Trino, Flink, and Spark to share table metadata
#
# PRODUCTION NOTE: In production, AWS Glue Data Catalog is used instead of
# this self-hosted Hive Metastore. This Dockerfile is retained only for
# local development and testing.
# =============================================================================
FROM apache/hive:4.0.0

LABEL maintainer="data-engineering@zomato.com"
LABEL description="Hive Metastore Thrift server for Iceberg table catalog"

USER root

# Download AWS SDK JARs for S3-compatible (MinIO) access
RUN wget -q -P /opt/hive/lib/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar" && \
    wget -q -P /opt/hive/lib/ \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.600/aws-java-sdk-bundle-1.12.600.jar"

# Hive Metastore configuration for local development
RUN cat > /opt/hive/conf/metastore-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <!-- Metastore Thrift server -->
    <property>
        <name>metastore.thrift.uris</name>
        <value>thrift://0.0.0.0:9083</value>
    </property>

    <!-- Use embedded Derby for local dev (no external RDBMS needed) -->
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:derby:;databaseName=/opt/hive/data/metastore_db;create=true</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>org.apache.derby.jdbc.EmbeddedDriver</value>
    </property>

    <!-- S3A (MinIO) filesystem configuration -->
    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://minio:9000</value>
    </property>
    <property>
        <name>fs.s3a.access.key</name>
        <value>minioadmin</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>minioadmin</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>

    <!-- Warehouse directory in MinIO -->
    <property>
        <name>metastore.warehouse.dir</name>
        <value>s3a://zomato-data-platform-dev-curated-data-lake/warehouse</value>
    </property>

    <!-- Schema initialization -->
    <property>
        <name>datanucleus.schema.autoCreateAll</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.metastore.schema.verification</name>
        <value>false</value>
    </property>
</configuration>
EOF

# Create data directories
RUN mkdir -p /opt/hive/data && chown -R hive:hive /opt/hive/data

USER hive

EXPOSE 9083

HEALTHCHECK --interval=15s --timeout=10s --retries=10 --start-period=45s \
    CMD nc -z localhost 9083 || exit 1

CMD ["bin/start-metastore"]
