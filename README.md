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
├── platform/                        # Application code
│   ├── pipelines/                   # Pipeline application code
│   │   ├── pipeline1_batch_etl/     # Spark JDBC batch ETL
│   │   ├── pipeline2_cdc/          # Debezium CDC → Kafka → Flink
│   │   ├── pipeline3_dynamodb_streams/ # DynamoDB → Spark
│   │   └── pipeline4_realtime_events/  # App events → Kafka → Flink → Druid
│   ├── airflow/                     # Airflow DAGs and plugins
│   └── config/                      # Application configuration
├── services/                        # External service configs
│   ├── druid/                       # Druid ingestion specs
│   └── msk/                         # MSK topics, connectors, schemas
├── infra/                           # Infrastructure
│   ├── terraform/                   # IaC (modules + environments)
│   ├── docker/                      # Docker configs for local dev
│   ├── ci/                          # CI/CD workflows (GitHub Actions, Jenkins)
│   └── scripts/                     # Deployment and ops scripts
├── observability/                   # Monitoring and alerting
│   ├── prometheus/                  # Metrics collection and alert rules
│   ├── grafana/                     # Dashboards and provisioning
│   └── alertmanager/                # Alert routing
├── tests/                           # Unit, integration, and e2e tests
└── docs/                            # Architecture documentation
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
make produce-kafka                    # Kafka only
make produce-mysql                    # MySQL only
make produce-dynamodb                 # DynamoDB only
python infra/scripts/produce_realtime.py --rate 23 --duration 3600  # 2M orders/day for 1h

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
