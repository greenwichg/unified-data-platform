# Zomato Data Platform Architecture on AWS

A production-grade data platform processing 2M+ orders/day, 450M Kafka messages/min,
20B events/week, and petabytes of data queried daily.

## Architecture Overview

### Four Data Pipelines

| Pipeline | Source | Processing | Sink | Format |
|----------|--------|-----------|------|--------|
| Pipeline 1 - Batch ETL | Aurora MySQL | Sqoop → S3 | Kafka → Flink → S3 | ORC |
| Pipeline 2 - CDC | Aurora MySQL | Debezium → Kafka | Flink → S3 (Iceberg) | Avro/ORC |
| Pipeline 3 - DynamoDB Streams | DynamoDB | Streams → S3 JSON | Spark (EMR) → S3 | ORC |
| Pipeline 4 - Real-time Events | Microservices/Web/Mobile | Custom Producer → Kafka | Flink → S3 + Druid | ORC |

### Query Layer
- **Trino**: 250K+ queries/week, 2PB scanned, 3 cluster types (Adhoc, ETL, Reporting)
- **Druid**: 20B events/week, 8M queries/week, millisecond response times

### Dashboards
- Apache Superset, Redash, Jupyter Notebooks

## Project Structure

```
├── terraform/           # Infrastructure as Code (AWS resources)
│   ├── modules/         # Reusable Terraform modules
│   └── environments/    # Environment-specific configs (dev/staging/prod)
├── docker/              # Docker configs for local development
├── pipelines/           # Pipeline application code
│   ├── pipeline1_batch_etl/        # Sqoop batch ETL
│   ├── pipeline2_cdc/              # Debezium CDC → Kafka → Flink
│   ├── pipeline3_dynamodb_streams/ # DynamoDB → Spark
│   └── pipeline4_realtime_events/  # App events → Kafka → Flink → Druid
├── airflow/             # Airflow DAGs for orchestration
├── schemas/             # Avro schemas for Kafka topics
├── scripts/             # Utility and deployment scripts
├── config/              # Application configuration
└── docs/                # Architecture documentation
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Message Bus | Apache Kafka (self-hosted EC2) | 450M msgs/min event streaming |
| Stream Processing | Apache Flink | Complex event processing |
| Batch Processing | Apache Spark (EMR) | DynamoDB stream processing |
| Batch ETL | Apache Sqoop | MySQL → S3 batch ingestion |
| CDC | Debezium | MySQL change data capture |
| Data Lake | S3 + Iceberg + ORC | Petabyte-scale storage |
| Query Engine | Trino | Federated SQL queries |
| Real-time OLAP | Apache Druid | Millisecond analytics |
| Orchestration | Apache Airflow | Pipeline scheduling |
| Dashboards | Superset, Redash, Jupyter | Visualization |

## Quick Start (Local Development)

```bash
# Start local infrastructure
docker-compose up -d

# Run Pipeline 1 (Batch ETL)
cd pipelines/pipeline1_batch_etl && python src/batch_etl.py

# Run Pipeline 4 (Real-time Events)
cd pipelines/pipeline4_realtime_events && python src/event_producer.py
```

## Deployment

```bash
# Initialize Terraform
cd terraform/environments/dev
terraform init
terraform plan
terraform apply
```
