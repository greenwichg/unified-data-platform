#!/usr/bin/env bash
#
# Pipeline 1 - Spark JDBC Batch ETL
# Imports tables from Aurora MySQL to Iceberg/ORC on S3 via Spark.
# Uses Spark JDBC for partitioned reads from Aurora MySQL.
#
# Usage:
#   ./spark_submit.sh [--table <table_name>] [--full-load] [--no-iceberg]
#
# Environment variables:
#   AURORA_JDBC_URL    - JDBC connection string for Aurora MySQL
#   S3_BUCKET          - Target S3 bucket for data lake
#   EMR_CLUSTER_ID     - (optional) EMR cluster for remote submit
#   SPARK_MASTER       - Spark master URL (default: yarn)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR="$(dirname "${SCRIPT_DIR}")"
LOG_PREFIX="[spark-jdbc-etl]"

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------
AURORA_JDBC_URL="${AURORA_JDBC_URL:-jdbc:mysql://aurora-cluster.cluster-xxxxx.ap-south-1.rds.amazonaws.com:3306/zomato}"
S3_BUCKET="${S3_BUCKET:-zomato-data-platform-prod-raw-data-lake}"
SPARK_MASTER="${SPARK_MASTER:-yarn}"

# Spark resource settings
DRIVER_MEMORY="${DRIVER_MEMORY:-4g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-8g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
NUM_EXECUTORS="${NUM_EXECUTORS:-8}"

# JDBC driver
MYSQL_CONNECTOR_JAR="${MYSQL_CONNECTOR_JAR:-/usr/share/java/mysql-connector-java.jar}"
ICEBERG_SPARK_JAR="${ICEBERG_SPARK_JAR:-/usr/lib/iceberg/lib/iceberg-spark-runtime.jar}"

# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --table)
            EXTRA_ARGS+=(--table "$2")
            shift 2
            ;;
        --full-load)
            EXTRA_ARGS+=(--full-load)
            shift
            ;;
        --no-iceberg)
            EXTRA_ARGS+=(--no-iceberg)
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--table <table_name>] [--full-load] [--no-iceberg]"
            exit 0
            ;;
        *)
            echo "${LOG_PREFIX} Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log_info()  { echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') ${LOG_PREFIX} INFO  $*"; }
log_error() { echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') ${LOG_PREFIX} ERROR $*" >&2; }

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
if ! command -v spark-submit &>/dev/null; then
    log_error "spark-submit not found in PATH"
    exit 1
fi

if [[ ! -f "${PIPELINE_DIR}/src/batch_etl.py" ]]; then
    log_error "batch_etl.py not found at ${PIPELINE_DIR}/src/batch_etl.py"
    exit 1
fi

# ---------------------------------------------------------------------------
# Spark Submit
# ---------------------------------------------------------------------------
log_info "Starting Spark JDBC Batch ETL"
log_info "JDBC URL: ${AURORA_JDBC_URL}"
log_info "S3 Bucket: ${S3_BUCKET}"
log_info "Spark Master: ${SPARK_MASTER}"
log_info "Resources: ${NUM_EXECUTORS} executors x ${EXECUTOR_CORES} cores x ${EXECUTOR_MEMORY}"

START_TS=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

spark-submit \
    --master "${SPARK_MASTER}" \
    --deploy-mode cluster \
    --name "Pipeline1-BatchETL" \
    --driver-memory "${DRIVER_MEMORY}" \
    --executor-memory "${EXECUTOR_MEMORY}" \
    --executor-cores "${EXECUTOR_CORES}" \
    --num-executors "${NUM_EXECUTORS}" \
    --jars "${MYSQL_CONNECTOR_JAR},${ICEBERG_SPARK_JAR}" \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.orc.impl=native \
    --conf spark.sql.orc.enableVectorizedReader=true \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.iceberg.warehouse=s3://zomato-data-platform-prod-raw-data-lake/iceberg \
    --conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    "${PIPELINE_DIR}/src/batch_etl.py" \
    --jdbc-url "${AURORA_JDBC_URL}" \
    --s3-bucket "${S3_BUCKET}" \
    "${EXTRA_ARGS[@]}"

RC=$?
END_TS=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

if [[ ${RC} -eq 0 ]]; then
    log_info "Spark JDBC ETL completed successfully (started=${START_TS} ended=${END_TS})"
else
    log_error "Spark JDBC ETL failed with exit code ${RC} (started=${START_TS} ended=${END_TS})"
fi

exit ${RC}
