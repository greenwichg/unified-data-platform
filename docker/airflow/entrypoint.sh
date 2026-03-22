#!/usr/bin/env bash
# =============================================================================
# Zomato Data Platform - Airflow Entrypoint
# Initializes the database, creates admin user, and starts the webserver.
# =============================================================================
set -euo pipefail

AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"

# Wait for the metadata database to be available
wait_for_db() {
    local max_retries=30
    local retry_interval=2
    local attempt=0

    echo "[entrypoint] Waiting for metadata database..."

    while [ $attempt -lt $max_retries ]; do
        if airflow db check 2>/dev/null; then
            echo "[entrypoint] Database is available."
            return 0
        fi
        attempt=$((attempt + 1))
        echo "[entrypoint] Database not ready (attempt $attempt/$max_retries). Retrying in ${retry_interval}s..."
        sleep $retry_interval
    done

    echo "[entrypoint] ERROR: Database not available after $max_retries attempts."
    exit 1
}

# Initialize or migrate the Airflow metadata database
init_db() {
    echo "[entrypoint] Initializing Airflow metadata database..."
    airflow db migrate
    echo "[entrypoint] Database initialization complete."
}

# Create the admin user if it doesn't exist
create_admin_user() {
    local username="${AIRFLOW_ADMIN_USER:-admin}"
    local password="${AIRFLOW_ADMIN_PASSWORD:-admin}"
    local firstname="${AIRFLOW_ADMIN_FIRSTNAME:-Admin}"
    local lastname="${AIRFLOW_ADMIN_LASTNAME:-User}"
    local email="${AIRFLOW_ADMIN_EMAIL:-admin@zomato.com}"

    echo "[entrypoint] Creating admin user '${username}'..."
    airflow users create \
        --username "${username}" \
        --password "${password}" \
        --firstname "${firstname}" \
        --lastname "${lastname}" \
        --role Admin \
        --email "${email}" \
        2>/dev/null || echo "[entrypoint] Admin user already exists."
}

# Set up Airflow Variables for pipeline configuration
setup_variables() {
    echo "[entrypoint] Setting up default Airflow Variables..."

    # Only set variables that aren't already configured
    # NOTE: In production, Athena replaces Trino and Amazon MSK replaces self-hosted Kafka.
    # Trino variables are kept for local dev; production DAGs use Athena workgroup settings.
    airflow variables set trino_etl_host "${TRINO_ETL_HOST:-trino}" 2>/dev/null || true
    airflow variables set trino_adhoc_host "${TRINO_ADHOC_HOST:-trino}" 2>/dev/null || true
    airflow variables set athena_workgroup "${ATHENA_WORKGROUP:-zomato-etl}" 2>/dev/null || true
    airflow variables set athena_output_location "${ATHENA_OUTPUT_LOCATION:-s3://zomato-data-platform-athena-results/}" 2>/dev/null || true
    # NOTE: In production, use MSK bootstrap servers with IAM auth.
    airflow variables set kafka_bootstrap "${KAFKA_BOOTSTRAP:-kafka-1:29092}" 2>/dev/null || true
    airflow variables set msk_bootstrap "${MSK_BOOTSTRAP:-}" 2>/dev/null || true
    airflow variables set emr_cluster_id "${EMR_CLUSTER_ID:-}" 2>/dev/null || true
    airflow variables set s3_raw_bucket "${S3_RAW_BUCKET:-zomato-data-platform-dev-raw-data-lake}" 2>/dev/null || true
    airflow variables set s3_processed_bucket "${S3_PROCESSED_BUCKET:-zomato-data-platform-dev-processed-data-lake}" 2>/dev/null || true

    echo "[entrypoint] Variables configured."
}

# Set up Airflow Connections
setup_connections() {
    echo "[entrypoint] Setting up default Airflow Connections..."

    airflow connections add aws_default \
        --conn-type aws \
        --conn-extra '{"region_name": "us-east-1"}' \
        2>/dev/null || true

    # NOTE: In production, use athena_default instead of trino_default.
    # Trino connection is kept for local development only.
    airflow connections add trino_default \
        --conn-type trino \
        --conn-host "${TRINO_ETL_HOST:-trino}" \
        --conn-port 8080 \
        --conn-schema zomato \
        --conn-extra '{"catalog": "iceberg"}' \
        2>/dev/null || true

    airflow connections add athena_default \
        --conn-type aws \
        --conn-extra "{\"region_name\": \"${AWS_REGION:-us-east-1}\", \"work_group\": \"${ATHENA_WORKGROUP:-zomato-etl}\", \"output_location\": \"${ATHENA_OUTPUT_LOCATION:-s3://zomato-data-platform-athena-results/}\"}" \
        2>/dev/null || true

    echo "[entrypoint] Connections configured."
}

# Main entrypoint logic
main() {
    echo "=============================================="
    echo "Zomato Data Platform - Airflow"
    echo "=============================================="

    wait_for_db
    init_db
    create_admin_user
    setup_variables
    setup_connections

    # Determine which component to start based on the first argument
    case "${1:-webserver}" in
        webserver)
            echo "[entrypoint] Starting Airflow Webserver..."
            exec airflow webserver
            ;;
        scheduler)
            echo "[entrypoint] Starting Airflow Scheduler..."
            exec airflow scheduler
            ;;
        worker)
            echo "[entrypoint] Starting Airflow Celery Worker..."
            exec airflow celery worker
            ;;
        flower)
            echo "[entrypoint] Starting Airflow Celery Flower..."
            exec airflow celery flower
            ;;
        triggerer)
            echo "[entrypoint] Starting Airflow Triggerer..."
            exec airflow triggerer
            ;;
        *)
            echo "[entrypoint] Running custom command: $*"
            exec "$@"
            ;;
    esac
}

main "$@"
