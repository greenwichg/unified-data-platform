# Zomato Data Platform Architecture on AWS

A production-grade data platform processing 2M+ orders/day, 450M MSK messages/min,
20B events/week, and petabytes of data queried daily.

## Architecture Overview

> **Full architecture diagram and details**: [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md)

### Four Data Pipelines

| Pipeline | Source | Processing | Sink | Format |
|----------|--------|-----------|------|--------|
| Pipeline 1 - Batch ETL | Aurora MySQL | Spark JDBC → S3 | Iceberg + ORC | ORC |
| Pipeline 2 - CDC | Aurora MySQL | Debezium → MSK | Flink → S3 (Iceberg) | Avro/ORC |
| Pipeline 3 - DynamoDB Streams | DynamoDB | Streams → S3 JSON | Spark (EMR) → S3 | ORC |
| Pipeline 4 - Real-time Events | Microservices/Web/Mobile | Custom Producer → MSK (2 clusters) | Flink → S3 + Druid | ORC |

### Query Layer
- **Amazon Athena** (Trino-based, serverless): 250K+ queries/week, 2PB scanned, 3 Athena workgroups (Adhoc, ETL, Reporting)
- **Druid**: 20B events/week, 8M queries/week, millisecond response times

## Project Structure

```
├── platform/                              # Application code
│   ├── pipelines/                         # Pipeline source code
│   │   ├── pipeline1_batch_etl/           # Spark JDBC batch ETL (Aurora → S3 Iceberg/ORC)
│   │   ├── pipeline2_cdc/                 # Debezium CDC → MSK → Flink → S3 Iceberg
│   │   ├── pipeline3_dynamodb_streams/    # DynamoDB Streams → EMR Spark → S3 ORC
│   │   └── pipeline4_realtime_events/     # Events → MSK → Flink → S3 ORC + Druid
│   ├── airflow/                           # MWAA DAGs, plugins, requirements
│   ├── schemas/                           # Avro and Iceberg schema definitions
│   │   ├── avro/                          # Avro schemas for Kafka topics
│   │   └── iceberg/                       # Iceberg table schemas
│   ├── sql/                               # SQL scripts
│   │   ├── ddl/                           # Table DDL
│   │   ├── druid/                         # Druid ingestion specs
│   │   └── queries/                       # Named queries
│   └── config/                            # Application configuration (application.yml)
│
├── local/                                 # Local development only
│   ├── docker-compose.yml                 # Full local stack (all services)
│   ├── .env.example                       # Environment variable template
│   ├── configs/
│   │   └── mysql-init/                    # MySQL schema init scripts
│   ├── scripts/
│   │   ├── create-kafka-topics.sh         # Create topics on local Kafka
│   │   └── register-connectors.sh         # Register Debezium connectors locally
│   └── tools/
│       ├── seed_data.py                   # Seed MySQL, DynamoDB, Kafka
│       ├── seed_helpers.py                # Seed utility functions
│       └── produce_realtime.py            # Continuous real-time event producer
│
├── infra/                                 # Production infrastructure
│   ├── terraform/
│   │   ├── modules/                       # Reusable Terraform modules
│   │   │   ├── vpc/                       # VPC, subnets, NAT, security groups
│   │   │   ├── aurora/                    # Amazon Aurora MySQL cluster
│   │   │   ├── kafka/                     # Amazon MSK (both clusters)
│   │   │   ├── kafka_consumer_fleet/      # EC2 ASG consumer fleet (MSK → Druid)
│   │   │   ├── ecs/                       # ECS cluster (Fargate)
│   │   │   ├── debezium/                  # Debezium on ECS Fargate
│   │   │   ├── flink/                     # Amazon Managed Flink (3 applications)
│   │   │   ├── emr/                       # EMR Spark cluster
│   │   │   ├── dynamodb/                  # DynamoDB tables with streams
│   │   │   ├── druid/                     # Druid EC2 cluster (5 node types + ASG)
│   │   │   ├── athena/                    # Amazon Athena + Glue Data Catalog
│   │   │   ├── s3/                        # S3 data lake buckets + lifecycle
│   │   │   ├── airflow/                   # Amazon MWAA
│   │   │   └── monitoring/                # CloudWatch dashboards + SNS alarms
│   │   └── environments/                  # Per-environment Terraform roots
│   │       ├── dev/
│   │       ├── staging/
│   │       └── prod/
│   ├── docker/                            # Dockerfiles for production images
│   │   ├── airflow/                       # Custom Airflow image
│   │   ├── athena/                        # Trino/Athena local config
│   │   ├── debezium/                      # Debezium Connect image
│   │   ├── druid/                         # Druid image
│   │   ├── flink/                         # Flink image
│   │   └── spark/                         # Spark image
│   ├── configs/                           # Production service configs
│   │   ├── msk/                           # MSK broker, connector, schema registry configs
│   │   └── trino/                         # Trino coordinator/worker configs (reference)
│   └── scripts/                           # Deployment and ops scripts
│       ├── deploy.sh                      # Terraform deploy (dev/staging/prod)
│       ├── ops/                           # Day-2 operations
│       │   ├── msk-create-topics.sh       # Create MSK topics
│       │   ├── consumer-group-monitor.sh  # Kafka consumer lag monitoring
│       │   ├── kafka_rebalance.sh         # Kafka partition rebalancing
│       │   ├── reassign-partitions.sh     # Partition reassignment tool
│       │   ├── druid_compaction_trigger.sh # Druid segment compaction
│       │   └── trino_cluster_health.sh    # Athena workgroup health check
│       ├── backup/
│       │   ├── aurora_snapshot.sh         # Aurora snapshot automation
│       │   └── s3_lifecycle_policies.json # S3 tiering and expiry rules
│       └── migration/
│           └── schema_migration.py        # Versioned schema migrations (Aurora/Athena)
│
├── observability/                         # Monitoring and alerting (local dev)
│   ├── prometheus/                        # Prometheus config and alert rules
│   ├── grafana/                           # Grafana dashboards and provisioning
│   ├── alertmanager/                      # Alertmanager routing config
│   ├── kafka/                             # Kafka JMX exporter config
│   └── docker-compose.monitoring.yml      # Observability stack compose file
│
├── tests/                                 # Test suite
│   ├── unit/                              # Unit tests
│   │   ├── pipelines/                     # Pipeline logic tests
│   │   ├── airflow/                       # DAG integrity tests
│   │   ├── athena/                        # Query tests
│   │   ├── flink/                         # Flink job tests
│   │   ├── kafka/                         # Schema registry tests
│   │   └── spark/                         # Spark processor tests
│   ├── integration/                       # Integration tests (per pipeline)
│   ├── e2e/                               # End-to-end pipeline tests
│   ├── terraform/                         # Terraform validation
│   └── fixtures/                          # Test data (orders, users, menu, promo)
│
├── docs/                                  # Documentation
│   ├── architecture/
│   │   ├── ARCHITECTURE.md               # Architecture decisions and pipeline details
│   │   └── zomato_architecture.jpg       # Architecture diagram
│   ├── execution_flow/                    # End-to-end execution flow docs
│   ├── local-setup.md                    # Local development setup guide
│   ├── production-setup.md               # Production deployment guide
│   └── data_stores.md                    # Data store reference
│
├── .github/workflows/                     # CI/CD (GitHub Actions)
│   ├── terraform_plan.yml                 # Terraform plan on PR
│   ├── terraform_apply.yml                # Terraform apply on merge
│   ├── docker_build.yml                   # Docker image builds
│   ├── spark_test.yml                     # Spark unit tests
│   ├── flink_build.yml                    # Flink job build
│   └── airflow_lint.yml                   # DAG linting
├── Makefile                               # Task runner (local + production targets)
├── Jenkinsfile                            # Jenkins CI pipeline
└── requirements.txt                       # Python dependencies
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Message Bus | Amazon MSK (2 clusters) | 450M msgs/min event streaming |
| Stream Processing | Amazon Managed Flink | Complex event processing |
| Batch Processing | Apache Spark (EMR) | DynamoDB stream processing |
| Batch ETL | Apache Spark JDBC | MySQL → Iceberg/ORC batch ingestion |
| CDC | Debezium | MySQL change data capture |
| Data Lake | S3 + Iceberg + ORC | Petabyte-scale storage |
| Query Engine | Amazon Athena (Trino-based, serverless) | Federated SQL queries |
| Metadata Catalog | AWS Glue Data Catalog | Centralized metadata management |
| Schema Registry | AWS Glue Schema Registry | Schema management for streaming data |
| Real-time OLAP | Apache Druid | Millisecond analytics |
| Orchestration | Apache Airflow | Pipeline scheduling |
| Dashboards | Amazon QuickSight (or any BI tool) | Visualization |

## Quick Start (Local Development)

```bash
# Start full local stack and seed all data automatically
make dev-setup

# Or start and seed separately
make docker-up   # also triggers seed service automatically via docker-compose
make seed        # re-seed manually at any time

# Seed startup flow (automatic on docker-compose up)
# docker-compose up
#     ├── mysql (healthy?) ──┐
#     └── kafka-1 (healthy?) ┤
#                            └── seed (runs once, exits)

# Seed individual sources
make seed-mysql
make seed-dynamodb
make seed-kafka

# Produce continuous real-time events (runs forever until Ctrl+C)
make produce                          # all targets at 5 events/sec
make produce-fast                     # all targets at 23 events/sec (~2M orders/day)
make produce-timed                    # all targets at 10 events/sec for 10 minutes
make produce-kafka                    # Kafka only
make produce-mysql                    # MySQL only
make produce-dynamodb                 # DynamoDB only
python local/tools/produce_realtime.py --rate 23 --duration 3600  # custom rate/duration

# Run Pipeline 1 (Batch ETL)
cd platform/pipelines/pipeline1_batch_etl && python src/batch_etl.py

# Run Pipeline 4 (Real-time Events)
cd platform/pipelines/pipeline4_realtime_events && python src/event_producer.py
```

## Deployment

```bash
# Initialize Terraform
cd infra/terraform/environments/dev
terraform init
terraform plan
terraform apply
```
