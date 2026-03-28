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
        echo "WARNING: This will destroy all infrastructure in $ENVIRONMENT!"
        read -r -p "Are you sure? (yes/no): " confirm
        if [[ "$confirm" == "yes" ]]; then
            cd "$TERRAFORM_DIR"
            terraform destroy -auto-approve
        else
            echo "Aborted."
        fi
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
