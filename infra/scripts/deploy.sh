#!/bin/bash
###############################################################################
# Deployment Script for Zomato Data Platform
# Deploys infrastructure and pipeline components
###############################################################################

set -euo pipefail

ENVIRONMENT="${1:-dev}"
ACTION="${2:-plan}"
REGION="${AWS_REGION:-us-east-1}"

echo "=============================================="
echo "Zomato Data Platform Deployment"
echo "Environment: $ENVIRONMENT"
echo "Action:      $ACTION"
echo "Region:      $REGION"
echo "=============================================="

TERRAFORM_DIR="terraform/environments/$ENVIRONMENT"
SCRIPTS_DIR="scripts"

# Validate environment
if [[ ! -d "$TERRAFORM_DIR" ]]; then
    echo "ERROR: Environment '$ENVIRONMENT' not found at $TERRAFORM_DIR"
    exit 1
fi

case "$ACTION" in
    plan)
        echo ""
        echo ">>> Running Terraform Plan..."
        cd "$TERRAFORM_DIR"
        terraform init -upgrade
        terraform plan -out=tfplan
        echo "Plan saved to tfplan. Run with 'apply' to execute."
        ;;

    apply)
        echo ""
        echo ">>> Running Terraform Apply..."
        cd "$TERRAFORM_DIR"
        terraform init -upgrade
        if [[ "$ENVIRONMENT" == "prod" ]]; then
            echo ">>> PRODUCTION deployment requires explicit confirmation"
            terraform apply
        else
            terraform apply -auto-approve
        fi
        echo ""
        echo ">>> Infrastructure deployed!"
        echo ""
        echo ">>> Creating Kafka topics (MSK)..."
        MSK_ENABLED=true bash "../../$SCRIPTS_DIR/create-kafka-topics.sh"
        echo ""
        echo ">>> Deploying Debezium connectors..."
        python3 "../../platform/pipelines/pipeline2_cdc/src/debezium_connector.py"
        echo ""
        echo "Deployment complete!"
        ;;

    destroy)
        echo ""
        echo "=============================================="
        echo "  DESTROY: $ENVIRONMENT"
        echo "=============================================="
        echo ""
        echo "WARNING: This will permanently destroy ALL infrastructure"
        echo "in the '$ENVIRONMENT' environment, including:"
        echo ""
        echo "  - Aurora MySQL cluster and all data"
        echo "  - MSK Kafka clusters (both)"
        echo "  - Amazon Managed Flink applications"
        echo "  - EMR Spark cluster"
        echo "  - Druid EC2 Auto Scaling Groups"
        echo "  - ECS cluster and Debezium tasks"
        echo "  - S3 buckets (data lake, checkpoints, logs)"
        echo "  - MWAA Airflow environment"
        echo "  - VPC and all networking"
        echo "  - CloudWatch dashboards and alarms"
        echo ""

        if [[ "$ENVIRONMENT" == "prod" ]]; then
            echo "  !!! YOU ARE DESTROYING PRODUCTION !!!"
            echo ""
            echo "  Recommended pre-destroy checklist:"
            echo "    1. Take a final Aurora snapshot:  make ops-aurora-snapshot"
            echo "    2. Export S3 data if needed"
            echo "    3. Notify the team"
            echo ""
            read -r -p "Type 'yes-destroy-prod' to confirm: " confirm
            if [[ "$confirm" != "yes-destroy-prod" ]]; then
                echo "Aborted."
                exit 1
            fi
        else
            read -r -p "Type 'yes-destroy-$ENVIRONMENT' to confirm: " confirm
            if [[ "$confirm" != "yes-destroy-$ENVIRONMENT" ]]; then
                echo "Aborted."
                exit 1
            fi
        fi

        echo ""
        echo ">>> Taking pre-destroy Aurora snapshot..."
        if aws rds describe-db-clusters \
            --query "DBClusters[?contains(DBClusterIdentifier, '$ENVIRONMENT')].DBClusterIdentifier" \
            --output text 2>/dev/null | grep -q "$ENVIRONMENT"; then
            CLUSTER_ID=$(aws rds describe-db-clusters \
                --query "DBClusters[?contains(DBClusterIdentifier, '$ENVIRONMENT')].DBClusterIdentifier" \
                --output text)
            SNAPSHOT_ID="pre-destroy-${ENVIRONMENT}-$(date +%Y%m%d%H%M%S)"
            aws rds create-db-cluster-snapshot \
                --db-cluster-identifier "$CLUSTER_ID" \
                --db-cluster-snapshot-identifier "$SNAPSHOT_ID" \
                --region "$REGION" && \
                echo "Snapshot created: $SNAPSHOT_ID" || \
                echo "Snapshot failed or skipped (non-fatal)."
        else
            echo "No Aurora cluster found for '$ENVIRONMENT' — skipping snapshot."
        fi

        echo ""
        echo ">>> Emptying S3 buckets before destroy..."
        for bucket in $(aws s3api list-buckets \
            --query "Buckets[?contains(Name, '$ENVIRONMENT')].Name" \
            --output text 2>/dev/null); do
            echo "  Emptying: $bucket"
            aws s3 rm "s3://$bucket" --recursive --quiet || true
            # Remove versioned objects if versioning is enabled
            aws s3api list-object-versions --bucket "$bucket" \
                --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
                --output json 2>/dev/null | \
                jq 'select(.Objects != null)' | \
                aws s3api delete-objects --bucket "$bucket" --delete file:///dev/stdin \
                --quiet 2>/dev/null || true
        done

        echo ""
        echo ">>> Running Terraform Destroy..."
        cd "$TERRAFORM_DIR"
        terraform init -upgrade

        if [[ "$ENVIRONMENT" == "prod" ]]; then
            terraform destroy
        else
            terraform destroy -auto-approve
        fi

        echo ""
        echo "Destroy complete for environment: $ENVIRONMENT"
        ;;

    local-up)
        echo ""
        echo ">>> Starting local development environment..."
        docker-compose up -d
        echo ""
        echo "Services starting up. Access:"
        echo "  Kafka (local):   localhost:9092  (prod: Amazon MSK)"
        echo "  Schema Registry: localhost:8081  (prod: AWS Glue Schema Registry)"
        echo "  Kafka Connect:   localhost:8083"
        echo "  Flink UI:        localhost:8084"
        echo "  Trino (local):   localhost:8085  (prod: Amazon Athena)"
        echo "  Druid:           localhost:8888"
        echo "  Airflow:         localhost:8080 (admin/admin)"
        echo "  MinIO Console:   localhost:9001 (minioadmin/minioadmin)"
        echo "  MySQL:           localhost:3306"
        ;;

    local-down)
        echo ""
        echo ">>> Stopping local development environment..."
        docker-compose down
        ;;

    *)
        echo "Usage: $0 <environment> <action>"
        echo ""
        echo "Environments: dev, staging, prod"
        echo "Actions:      plan, apply, destroy, local-up, local-down"
        exit 1
        ;;
esac
