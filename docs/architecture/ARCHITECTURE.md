# Zomato's Data Platform Architecture on AWS

## Pipeline Details

### Data Pipeline-1: Batch ETL
- **Source**: Aurora MySQL
- **Ingestion**: Apache Sqoop on Amazon EMR bulk-imports tables to S3 as ORC
- **Processing**: Data flows through self-hosted Kafka cluster, then Apache Flink performs Complex Event Processing (CEP)
- **Sink**: S3 data lake using ORC format with Apache Iceberg table format
- **Feedback**: Flink CEP results feed back into Kafka for recursive pattern detection

### Data Pipeline-2: CDC (Change Data Capture)
- **Source**: Aurora MySQL (binlog)
- **Ingestion**: Kafka Debezium source connector in distributed mode (Worker-A, B, C)
- **Serialization**: Avro format with Schema Registry
- **Topics**: `menu`, `promo`, `orders`, `users`
- **Processing**: Amazon Flink consumes Avro from Kafka, transforms, writes ORC to S3
- **Sink**: S3 with Iceberg table format

### Data Pipeline-3: DynamoDB Streams
- **Source**: DynamoDB with streams enabled
- **Ingestion**: ECS Multi-AZ service streams records to S3 as JSON (real-time micro-batches)
- **Processing**: Apache Spark on Amazon EMR reads JSON, deduplicates, builds sessions
- **Sink**: S3 in ORC format

### Data Pipeline-4: Real-time Events
- **Sources**: Microservices, Web Application, Mobile
- **Ingestion**: Custom Kafka Producer → Self-Hosted Kafka Cluster 2 (topics: menu, promo, orders, users)
- **Processing**: Amazon Flink with dual output:
  - **Path A**: Direct to S3 (ORC) for batch analytics via Trino
  - **Path B**: To intermediate Kafka cluster → EC2 Auto-Scaling consumer → Apache Druid for millisecond OLAP queries

### Query Layer: Trino
- Deployed on **ECS with R8g instances**
- **3 isolated clusters** to prevent workload interference:
  - **Adhoc Clusters**: Interactive queries from analysts
  - **ETL Clusters**: Airflow-driven transformation workloads
  - **Reporting Clusters**: Dashboard queries from Superset/Redash

### Real-time OLAP: Apache Druid
- Ingests from Kafka via EC2 Auto-Scaling consumers
- Sub-second query response on 20B events/week
- Deep storage on S3, segment caching on local SSD

## Design Pattern Annotations

The architecture uses three design pattern categories (shown in the diagram):

| Pattern | Label | Where Applied |
|---------|-------|---------------|
| **Creational** (C) | Custom Producer | Pipeline 4 producer abstracts event creation |
| **Structural** (S) | S3 Data Lake | Unified storage layer bridges all pipelines |
| **Behavioral** (B) | Kafka, Flink, Druid | Event-driven processing and reactive streaming |
