#!/usr/bin/env bash
# =============================================================================
# Zomato Data Platform - MinIO Bucket Initialization
# Creates the four data lake buckets and sets lifecycle/notification policies
# =============================================================================
set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
ALIAS="local"

# Wait for MinIO to become available
echo "[init-buckets] Waiting for MinIO to be ready at ${MINIO_ENDPOINT}..."
max_retries=30
attempt=0
while [ $attempt -lt $max_retries ]; do
    if mc alias set "${ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null; then
        echo "[init-buckets] MinIO is ready."
        break
    fi
    attempt=$((attempt + 1))
    echo "[init-buckets] MinIO not ready (attempt $attempt/$max_retries). Retrying..."
    sleep 2
done

if [ $attempt -eq $max_retries ]; then
    echo "[init-buckets] ERROR: MinIO not available after $max_retries attempts."
    exit 1
fi

# Data lake buckets
BUCKETS=(
    "zomato-data-platform-dev-raw-data-lake"
    "zomato-data-platform-dev-staging-data-lake"
    "zomato-data-platform-dev-curated-data-lake"
    "zomato-data-platform-dev-archive-data-lake"
)

# Create each bucket
for bucket in "${BUCKETS[@]}"; do
    echo "[init-buckets] Creating bucket: ${bucket}"
    mc mb "${ALIAS}/${bucket}" --ignore-existing
done

# Create supporting buckets
echo "[init-buckets] Creating supporting buckets..."
mc mb "${ALIAS}/zomato-data-platform-dev-checkpoints" --ignore-existing
mc mb "${ALIAS}/zomato-data-platform-dev-airflow-logs" --ignore-existing
mc mb "${ALIAS}/zomato-data-platform-dev-flink-savepoints" --ignore-existing
mc mb "${ALIAS}/zomato-data-platform-dev-druid-deep-storage" --ignore-existing

# Set versioning on critical buckets
echo "[init-buckets] Enabling versioning on curated bucket..."
mc version enable "${ALIAS}/zomato-data-platform-dev-curated-data-lake" 2>/dev/null || true

# Set lifecycle policy on archive bucket (expire after 90 days in local dev)
echo "[init-buckets] Setting lifecycle policy on archive bucket..."
cat > /tmp/archive-lifecycle.json <<'EOF'
{
    "Rules": [
        {
            "ID": "expire-old-archives",
            "Status": "Enabled",
            "Expiration": {
                "Days": 90
            }
        }
    ]
}
EOF
mc ilm import "${ALIAS}/zomato-data-platform-dev-archive-data-lake" < /tmp/archive-lifecycle.json 2>/dev/null || true

# Create directory structure in raw bucket for pipelines
echo "[init-buckets] Creating directory structure..."
for pipeline in pipeline1-batch-etl pipeline2-cdc pipeline3-dynamodb-streams pipeline4-realtime-events; do
    echo "" | mc pipe "${ALIAS}/zomato-data-platform-dev-raw-data-lake/${pipeline}/.keep" 2>/dev/null || true
done

echo ""
echo "[init-buckets] All buckets created successfully!"
echo ""
echo "[init-buckets] Listing all buckets:"
mc ls "${ALIAS}/"
echo ""
echo "[init-buckets] Bucket initialization complete."
