# =============================================================================
# Zomato Data Platform - PySpark (Local Development)
# Spark with Iceberg, ORC, and S3 support for Pipeline 3 (DynamoDB -> ORC)
# =============================================================================
FROM bitnami/spark:3.5

LABEL maintainer="data-engineering@zomato.com"
LABEL description="PySpark with Iceberg, ORC, and S3A support"

USER root

# Install Python dependencies for pipeline scripts
RUN pip install --no-cache-dir \
    boto3>=1.34.0 \
    pyyaml>=6.0 \
    pyarrow>=14.0.0 \
    requests>=2.31.0

# Download Iceberg Spark runtime JAR
ENV ICEBERG_VERSION=1.4.3
RUN wget -q -P /opt/bitnami/spark/jars/ \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"

# Download Hadoop AWS and AWS SDK for S3A
RUN wget -q -P /opt/bitnami/spark/jars/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar" && \
    wget -q -P /opt/bitnami/spark/jars/ \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.600/aws-java-sdk-bundle-1.12.600.jar"

# Spark defaults for local development
ENV SPARK_MODE=master \
    SPARK_MASTER_HOST=spark \
    SPARK_RPC_AUTHENTICATION_ENABLED=no \
    SPARK_RPC_ENCRYPTION_ENABLED=no \
    SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no \
    SPARK_SSL_ENABLED=no

# Copy Spark defaults configuration
RUN echo "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
        >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog" \
        >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.iceberg.type=hive" \
        >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083" \
        >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.sql.orc.enabled=true" \
        >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
        >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.access.key=minioadmin" \
        >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.secret.key=minioadmin" \
        >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.path.style.access=true" \
        >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
        >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.sql.adaptive.enabled=true" \
        >> /opt/bitnami/spark/conf/spark-defaults.conf

USER 1001

EXPOSE 7077 8080 4040
