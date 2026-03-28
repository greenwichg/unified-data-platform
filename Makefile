# ==============================================================================
# Zomato Data Platform - Makefile
#
# Targets:
#   build          - Install Python dependencies
#   test           - Run full test suite
#   test-unit      - Run unit tests only
#   test-int       - Run integration tests only
#   test-e2e       - Run end-to-end tests only
#   lint           - Run linters (ruff, mypy)
#   format         - Auto-format code
#   deploy-dev     - Deploy to dev environment
#   deploy-staging - Deploy to staging environment
#   deploy-prod    - Deploy to production environment
#   docker-build   - Build all Docker images
#   docker-up      - Start local Docker Compose stack
#   docker-down    - Stop local Docker Compose stack
#   clean          - Remove build artifacts, caches, and temp files
#   help           - Show this help message
#   seed           - One-time bulk seed (MySQL + DynamoDB + Kafka)
#   produce        - Continuous producer, all targets, 5 events/sec
#   produce-fast   - Continuous producer, all targets, 23 events/sec (~2M orders/day)
#   produce-timed  - Continuous producer, 10/sec for 10 minutes
#   produce-kafka  - Kafka only, 5 events/sec
#   produce-docker - Run producer in a Docker container (docker-compose profile)
#   dev-setup      - Start local stack and seed data in one command
# ==============================================================================

.PHONY: build test test-unit test-int test-e2e lint format \
        deploy-dev deploy-staging deploy-prod \
        docker-build docker-up docker-down clean help \
        migrate-athena msk-topics ops-athena-health \
        seed seed-mysql seed-dynamodb seed-kafka \
        produce produce-mysql produce-dynamodb produce-kafka \
        produce-fast produce-kafka-fast produce-timed \
        produce-docker produce-docker-fast produce-docker-kafka produce-docker-stop \
        dev-setup \
        cdc-register cdc-status cdc-restart cdc-test

PYTHON ?= python3
PIP ?= pip3
PYTEST ?= pytest
DOCKER_COMPOSE ?= docker compose
AWS_REGION ?= us-east-1

# Project paths
PROJECT_ROOT := $(shell pwd)
PIPELINES_DIR := $(PROJECT_ROOT)/platform/pipelines
AIRFLOW_DIR := $(PROJECT_ROOT)/platform/airflow
TESTS_DIR := $(PROJECT_ROOT)/tests
DOCKER_DIR := $(PROJECT_ROOT)/infra/docker
TERRAFORM_DIR := $(PROJECT_ROOT)/infra/terraform

# Docker image settings
DOCKER_REGISTRY ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
DOCKER_TAG ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "latest")

# ==============================================================================
# Build
# ==============================================================================

build:  ## Install Python dependencies
	$(PIP) install -r requirements.txt
	@echo "Dependencies installed."

build-dev: build  ## Install dev/test dependencies
	$(PIP) install pytest pytest-cov pytest-xdist ruff mypy moto[s3] boto3-stubs
	@echo "Dev dependencies installed."

# ==============================================================================
# Testing
# ==============================================================================

test: lint  ## Run full test suite with coverage
	$(PYTEST) $(TESTS_DIR) \
		--tb=short \
		-v \
		--cov=$(PIPELINES_DIR) \
		--cov-report=term-missing \
		--cov-report=html:htmlcov \
		-x

test-unit:  ## Run unit tests only
	$(PYTEST) $(TESTS_DIR)/unit \
		--tb=short \
		-v \
		-x

test-int:  ## Run integration tests only
	$(PYTEST) $(TESTS_DIR)/integration \
		--tb=short \
		-v \
		-x

test-e2e:  ## Run end-to-end tests only
	$(PYTEST) $(TESTS_DIR)/e2e \
		--tb=short \
		-v \
		-x

test-parallel:  ## Run tests in parallel (requires pytest-xdist)
	$(PYTEST) $(TESTS_DIR) \
		--tb=short \
		-v \
		-n auto \
		--cov=$(PIPELINES_DIR)

test-pipeline1:  ## Run Pipeline 1 (Batch ETL) tests only
	$(PYTEST) $(TESTS_DIR)/unit/pipelines/test_batch_etl.py -v

test-pipeline2:  ## Run Pipeline 2 (CDC) tests only
	$(PYTEST) $(TESTS_DIR)/unit/pipelines/test_cdc_processor.py -v

test-pipeline3:  ## Run Pipeline 3 (DynamoDB) tests only
	$(PYTEST) $(TESTS_DIR)/unit/pipelines/test_dynamodb_processor.py -v

test-pipeline4:  ## Run Pipeline 4 (Realtime) tests only
	$(PYTEST) $(TESTS_DIR)/unit/pipelines/test_realtime_events.py -v

# ==============================================================================
# Linting & Formatting
# ==============================================================================

lint:  ## Run all linters
	@echo "Running ruff..."
	ruff check $(PIPELINES_DIR) $(TESTS_DIR) infra/scripts/ $(AIRFLOW_DIR)/ --fix
	@echo "Running ruff format check..."
	ruff format --check $(PIPELINES_DIR) $(TESTS_DIR)
	@echo "Linting passed."

format:  ## Auto-format code with ruff
	ruff format $(PIPELINES_DIR) $(TESTS_DIR) infra/scripts/ $(AIRFLOW_DIR)/
	ruff check --fix $(PIPELINES_DIR) $(TESTS_DIR) infra/scripts/ $(AIRFLOW_DIR)/
	@echo "Formatting complete."

typecheck:  ## Run mypy type checking
	mypy $(PIPELINES_DIR) --ignore-missing-imports --no-error-summary

# ==============================================================================
# Deployment
# ==============================================================================

deploy-dev:  ## Deploy to dev environment
	@echo "Deploying to DEV..."
	bash scripts/deploy.sh dev apply

deploy-staging:  ## Deploy to staging environment
	@echo "Deploying to STAGING..."
	bash scripts/deploy.sh staging apply

deploy-prod:  ## Deploy to production environment
	@echo "========================================="
	@echo "WARNING: Deploying to PRODUCTION"
	@echo "========================================="
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ] || exit 1
	bash scripts/deploy.sh prod apply

deploy-plan-%:  ## Show Terraform plan for an environment (e.g., make deploy-plan-dev)
	bash scripts/deploy.sh $* plan

# ==============================================================================
# Docker
# ==============================================================================

docker-build:  ## Build all Docker images
	@echo "Building Docker images (tag: $(DOCKER_TAG))..."
	$(DOCKER_COMPOSE) build
	@echo "Docker images built."

docker-build-push: docker-build  ## Build and push Docker images to ECR
	@echo "Pushing images to $(DOCKER_REGISTRY)..."
	@echo "NOTE: kafka and trino images are for local dev only (prod uses MSK and Athena)"
	aws ecr get-login-password --region $(AWS_REGION) | \
		docker login --username AWS --password-stdin $(DOCKER_REGISTRY)
	@for service in airflow flink spark trino druid kafka; do \
		docker tag zomato-$$service:latest $(DOCKER_REGISTRY)/zomato-$$service:$(DOCKER_TAG); \
		docker push $(DOCKER_REGISTRY)/zomato-$$service:$(DOCKER_TAG); \
	done
	@echo "Push complete."

docker-up:  ## Start local Docker Compose stack
	$(DOCKER_COMPOSE) up -d
	@echo ""
	@echo "Services starting. Access points:"
	@echo "  Kafka (local):   localhost:9092  (prod: Amazon MSK)"
	@echo "  Schema Registry: localhost:8081  (prod: AWS Glue Schema Registry)"
	@echo "  Flink UI:        localhost:8084"
	@echo "  Trino (local):   localhost:8085  (prod: Amazon Athena)"
	@echo "  Druid:           localhost:8888"
	@echo "  Airflow:         localhost:8080"
	@echo "  MinIO Console:   localhost:9001"

docker-down:  ## Stop local Docker Compose stack
	$(DOCKER_COMPOSE) down

docker-logs:  ## Tail logs from all containers
	$(DOCKER_COMPOSE) logs -f --tail=100

docker-ps:  ## Show running containers
	$(DOCKER_COMPOSE) ps

# ==============================================================================
# Schema & Data Operations
# ==============================================================================

migrate-aurora:  ## Run Aurora MySQL schema migrations
	$(PYTHON) scripts/migration/schema_migration.py migrate --target aurora --env $(ENV)

migrate-athena:  ## Run Athena/Iceberg schema migrations (production)
	$(PYTHON) scripts/migration/schema_migration.py migrate --target athena --env $(ENV)

migrate-trino:  ## Run Trino/Iceberg schema migrations (local dev only)
	$(PYTHON) scripts/migration/schema_migration.py migrate --target trino --env $(ENV)

migrate-status:  ## Show migration status for all targets
	$(PYTHON) scripts/migration/schema_migration.py status --target aurora --env $(ENV)
	$(PYTHON) scripts/migration/schema_migration.py status --target trino --env $(ENV)

seed:  ## Seed all data sources (MySQL + DynamoDB + Kafka)
	$(PYTHON) infra/scripts/seed_data.py --target all

seed-mysql:  ## Seed Aurora MySQL only
	$(PYTHON) infra/scripts/seed_data.py --target mysql

seed-dynamodb:  ## Seed DynamoDB only
	$(PYTHON) infra/scripts/seed_data.py --target dynamodb

seed-kafka:  ## Seed Kafka topics only
	$(PYTHON) infra/scripts/seed_data.py --target kafka

produce:  ## Produce to all targets at 5 events/sec (runs forever)
	$(PYTHON) infra/scripts/produce_realtime.py --target all --rate 5

produce-fast:  ## Produce to all targets at 23 events/sec (~2M orders/day)
	$(PYTHON) infra/scripts/produce_realtime.py --target all --rate 23

produce-timed:  ## Produce to all targets at 10 events/sec for 10 minutes
	$(PYTHON) infra/scripts/produce_realtime.py --target all --rate 10 --duration 600

produce-mysql:  ## Produce real-time events to MySQL only (5/sec)
	$(PYTHON) infra/scripts/produce_realtime.py --target mysql --rate 5

produce-dynamodb:  ## Produce real-time events to DynamoDB only (5/sec)
	$(PYTHON) infra/scripts/produce_realtime.py --target dynamodb --rate 5

produce-kafka:  ## Produce real-time events to Kafka only (5/sec)
	$(PYTHON) infra/scripts/produce_realtime.py --target kafka --rate 5

produce-kafka-fast:  ## Produce to Kafka only at 23 events/sec (~2M orders/day)
	$(PYTHON) infra/scripts/produce_realtime.py --target kafka --rate 23

# docker-compose profile shortcuts (runs producer inside a container)
produce-docker:  ## Start producer container via docker-compose (5/sec, all targets)
	$(DOCKER_COMPOSE) --profile produce up producer

produce-docker-fast:  ## Start producer container at 23 events/sec (~2M orders/day)
	PRODUCE_RATE=23 $(DOCKER_COMPOSE) --profile produce up producer

produce-docker-kafka:  ## Start producer container targeting Kafka only
	PRODUCE_TARGET=kafka $(DOCKER_COMPOSE) --profile produce up producer

produce-docker-stop:  ## Stop the producer container
	$(DOCKER_COMPOSE) --profile produce stop producer

dev-setup: docker-up  ## Start local stack, seed data, and register CDC connectors
	@echo "Waiting for services to be ready..."
	@sleep 15
	@$(MAKE) seed
	@$(MAKE) cdc-register

# ==============================================================================
# CDC (Pipeline 2) Testing
# ==============================================================================

cdc-register:  ## Register Debezium CDC connectors (auto-run by connect-init service)
	bash infra/scripts/register-connectors.sh

cdc-status:  ## Show status of all Debezium connectors
	@curl -s http://localhost:8083/connectors?expand=status | \
		python3 -c "import sys,json; [print(f'  {n}: {i[\"status\"][\"connector\"][\"state\"]}') for n,i in json.load(sys.stdin).items()]"

cdc-restart:  ## Restart all CDC connectors
	@for c in zomato-orders-cdc zomato-users-cdc zomato-menu-cdc zomato-promo-cdc; do \
		echo "Restarting $$c..."; \
		curl -sf -X POST http://localhost:8083/connectors/$$c/restart; \
	done

cdc-test:  ## Insert a test row into MySQL and verify it appears in Kafka
	@echo "Inserting test order into MySQL..."
	@docker exec $$(docker compose ps -q mysql) mysql -uroot -prootpass zomato -e \
		"INSERT INTO orders (order_id,user_id,restaurant_id,status,subtotal,tax,delivery_fee,total_amount,payment_method,city,pincode,estimated_delivery_mins,created_at,updated_at) \
		 SELECT 'cdc-test-001', user_id, restaurant_id, 'PLACED', 299.00, 14.95, 30.00, 343.95, 'UPI', city, '560001', 35, NOW(), NOW() \
		 FROM orders LIMIT 1 \
		 ON DUPLICATE KEY UPDATE status='PLACED';" 2>/dev/null
	@echo "Waiting 5s for CDC to capture event..."
	@sleep 5
	@echo "Checking Kafka 'orders' topic for CDC event..."
	@docker exec $$(docker compose ps -q kafka-1) kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic orders \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms 5000 2>/dev/null && echo "✓ CDC event found in Kafka!" || echo "✗ No event found — check cdc-status"

kafka-topics:  ## Create Kafka topics (local dev)
	bash scripts/create-kafka-topics.sh

msk-topics:  ## Create Kafka topics on MSK (production)
	MSK_ENABLED=true bash scripts/create-kafka-topics.sh

# ==============================================================================
# Operations
# ==============================================================================

ops-kafka-rebalance:  ## Trigger Kafka partition rebalance - dry-run (local dev; MSK handles this automatically)
	bash scripts/ops/kafka_rebalance.sh --dry-run

ops-athena-health:  ## Check Athena workgroup health (replaces Trino health check)
	bash scripts/ops/trino_cluster_health.sh

ops-trino-health: ops-athena-health  ## DEPRECATED: Alias for ops-athena-health

ops-druid-compact:  ## Trigger Druid segment compaction
	bash scripts/ops/druid_compaction_trigger.sh

ops-aurora-snapshot:  ## Create Aurora MySQL snapshot
	bash scripts/backup/aurora_snapshot.sh

# ==============================================================================
# Cleanup
# ==============================================================================

clean:  ## Remove build artifacts, caches, and temp files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name htmlcov -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	rm -rf build/ dist/ *.egg-info/
	@echo "Clean complete."

# ==============================================================================
# Help
# ==============================================================================

help:  ## Show this help message
	@grep -E '^[a-zA-Z_%-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
