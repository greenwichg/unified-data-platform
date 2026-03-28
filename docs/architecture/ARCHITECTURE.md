# Zomato's Data Platform Architecture on AWS

> **Implementation notes vs original whiteboard design:**
> - Pipeline 1 uses **Spark JDBC** (not Apache Sqoop shown on whiteboard) — Spark JDBC supports Iceberg and is faster for large tables
> - Pipeline 2 uses **Amazon MSK** (not self-hosted Kafka on EC2 shown on whiteboard) — MSK is fully managed with built-in HA, no cluster ops overhead

## Pipeline Details

### Data Pipeline-1: Batch ETL
- **Source**: Aurora MySQL
- **Ingestion**: Apache Spark JDBC on Amazon EMR reads tables via partitioned JDBC, applies transformations and data quality checks
- **Sink**: S3 data lake using ORC format with Apache Iceberg table format (dual write: Iceberg primary, ORC secondary)
- **Schedule**: Runs every 6 hours via Airflow on Amazon EMR
- **Quality**: Built-in null checks, duplicate detection, configurable fail-on-error

### Data Pipeline-2: CDC (Change Data Capture)
- **Source**: Aurora MySQL (binlog)
- **Ingestion**: Debezium MySQL connectors on ECS Fargate (distributed mode)
- **Serialization**: Avro format with AWS Glue Schema Registry
- **Topics**: `menu`, `promo`, `orders`, `users` on Amazon MSK
- **Processing**: Amazon Managed Flink consumes Avro from MSK, transforms, writes ORC to S3
- **Sink**: S3 with Iceberg table format

### Data Pipeline-3: DynamoDB Streams
- **Source**: DynamoDB with streams enabled
- **Ingestion**: DynamoDB Streams delivers change records directly to S3 as JSON (real-time micro-batches)
- **Processing**: Apache Spark on Amazon EMR reads JSON, deduplicates, builds sessions
- **Sink**: S3 in ORC format

### Data Pipeline-4: Real-time Events
- **Sources**: Microservices, Web Application, Mobile
- **Ingestion**: Custom Kafka Producer → Amazon MSK Cluster 1 (topics: `orders`, `users`, `menu`, `promo`)
- **Processing**: Amazon Managed Flink with dual output:
  - **Path A**: Direct to S3 (ORC) for batch analytics via Amazon Athena
  - **Path B**: To Amazon MSK Cluster 2 (`druid-ingestion-events`) → EC2 Auto-Scaling consumer fleet → Apache Druid for millisecond OLAP queries
- **Two MSK Clusters**: Cluster 1 (9 brokers, event collection) and Cluster 2 (6 brokers, Druid ingestion buffer) for resource isolation and independent scaling

### Query Layer: Amazon Athena (Trino-based, serverless)
- **Serverless (Athena)** — no infrastructure to manage
- **3 Athena workgroups** to prevent workload interference:
  - **Adhoc Workgroup**: Interactive queries from analysts
  - **ETL Workgroup**: Airflow-driven transformation workloads
  - **Reporting Workgroup**: Dashboard queries run directly against Athena
- Metadata managed by **AWS Glue Data Catalog**

### Real-time OLAP: Apache Druid
- Ingests from Amazon MSK Cluster 2 via EC2 Auto-Scaling consumer fleet
- Sub-second query response on 20B events/week
- Deep storage on S3, segment caching on local SSD
- Deployed on R8g instances (Coordinator, Broker, Historical, MiddleManager, Router)

### Orchestration
- **Amazon MWAA** (Managed Workflows for Apache Airflow): Pipeline scheduling and monitoring

### Monitoring
- **CloudWatch**: Dashboards, metric alarms (Kafka lag, Flink checkpoints, Druid latency)
- **SNS**: Alert routing for SLA breaches

## Design Pattern Annotations

The architecture uses three design pattern categories (shown in the diagram):

| Pattern | Label | Where Applied |
|---------|-------|---------------|
| **Creational** (C) | Custom Producer | Pipeline 4 producer abstracts event creation |
| **Structural** (S) | S3 Data Lake | Unified storage layer bridges all pipelines |
| **Behavioral** (B) | Kafka, Flink, Druid | Event-driven processing and reactive streaming |
