# =============================================================================
# Zomato Data Platform - Apache Airflow with Providers
# Airflow 2.8 with all required provider packages for orchestrating
# the data platform pipelines
#
# PRODUCTION NOTE: In production, Athena replaces Trino for query execution,
# Amazon MSK (with IAM auth) replaces self-hosted Kafka, and AWS Glue Data
# Catalog replaces Hive Metastore. The apache-airflow-providers-amazon package
# includes Athena and MSK operators.
# =============================================================================
FROM apache/airflow:2.8-python3.11

LABEL maintainer="data-engineering@zomato.com"
LABEL description="Apache Airflow with AWS, Athena, Trino (local dev), Slack, and Celery providers"

USER root

# Install system dependencies for providers
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        default-libmysqlclient-dev \
        pkg-config \
        curl \
        jq && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Copy and install Python requirements
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy DAGs and plugins
COPY --chown=airflow:root ../../platform/airflow/dags/ /opt/airflow/dags/
COPY --chown=airflow:root ../../platform/airflow/plugins/ /opt/airflow/plugins/

# Copy entrypoint script
COPY entrypoint.sh /opt/airflow/entrypoint.sh

USER root
RUN chmod +x /opt/airflow/entrypoint.sh
USER airflow

# Environment defaults
ENV AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
    AIRFLOW__CORE__LOAD_EXAMPLES=false \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true \
    AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=Asia/Kolkata \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=false \
    AIRFLOW__CELERY__WORKER_CONCURRENCY=16 \
    AIRFLOW__CORE__PARALLELISM=32 \
    AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16 \
    AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=4 \
    AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30

EXPOSE 8080 5555 8793

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]
