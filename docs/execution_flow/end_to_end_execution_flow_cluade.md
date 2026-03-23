# Zomato Data Platform — End-to-End Execution Flow

## 1. Platform Overview

Zomato's Data Platform processes **2M+ orders/day**, **450M Kafka messages/minute**, and **20B events/week** across four interconnected data pipelines. All pipelines converge into a unified **S3 data lake** (ORC format with Apache Iceberg table management), queryable through **Amazon Athena** (serverless, Trino-based) for batch analytics and **Apache Druid** for sub-second real-time OLAP.

**Orchestration** is handled by **Amazon MWAA** (Managed Workflows for Apache Airflow), which schedules pipeline jobs, monitors health, enforces SLAs, and triggers alerting when thresholds are breached.

---

## 2. Architecture at a Glance

```
┌─────────────────────┐   ┌─────────────────────┐   ┌──────────────────────┐   ┌──────────────────────────┐
│  Aurora MySQL       │   │  Aurora MySQL       │   │  DynamoDB            │   │  Microservices / Web /   │
│  (Source DB)        │   │  (binlog)           │   │  (Streams Enabled)   │   │  Mobile Backends         │
└────────┬────────────┘   └────────┬────────────┘   └────────┬─────────────┘   └────────┬─────────────────┘
         │                         │                          │                          │
    Pipeline 1                Pipeline 2                 Pipeline 3                 Pipeline 4
    Batch ETL                 CDC                        DynamoDB Streams           Real-time Events
         │                         │                          │                          │
    Spark JDBC                Debezium → MSK             ECS Multi-AZ              Custom Producer
    (Amazon EMR)              Flink CDC                  → S3 JSON                 → MSK Cluster 1
         │                         │                     → Spark (EMR)                   │
         │                         │                          │                     Flink Real-time
         ▼                         ▼                          ▼                     ┌────┴──────┐
    ┌─────────────────────────────────────────────────────────────┐            S3 (ORC)   MSK Cluster 2
    │                   S3 Data Lake                              │                 │           │
    │   Iceberg Tables (ORC format) + Raw ORC partitions          │                 │     EC2 Consumer Fleet
    │   Managed by AWS Glue Data Catalog                          │                 │           │
    └──────────────────────┬──────────────────────────────────────┘                 │     Apache Druid
                           │                                                        │     (Real-time OLAP)
              ┌────────────┼────────────┐                                           │
              ▼            ▼            ▼                                           │
         Athena        Athena       Athena                                          │
         (Adhoc)       (ETL)        (Reporting)                                     │
              │            │            │                                           │
              └────────────┼────────────┘                                           │
                           ▼                                                        │ 
              ┌─────────────────────────────────────────────────────────────────────┘
              │              Serving Layer
              │   ┌────────────┬─────────────┬─────────────┐
              └──►│  Superset  │   Redash    │  JupyterHub │
                  │ (Dashboards)│ (Ad-hoc SQL)│ (Notebooks)│
                  └────────────┴─────────────┴─────────────┘
```

---

## 3. Data Pipelines — Detailed Execution Flow

### 3.1 Pipeline 1: Batch ETL

**Path:** Aurora MySQL → Spark JDBC (Amazon EMR) → Iceberg/ORC → S3

**Schedule:** Every 6 hours (`0 */6 * * *`)

**Purpose:** Full and incremental imports of transactional tables from the Aurora MySQL source database into the S3 data lake. This is the foundational batch layer that ensures all core business entities are available for analytics.

#### Execution Steps

1. **Airflow triggers** the `pipeline1_batch_etl` DAG every 6 hours.
2. For each of the six tables (`orders`, `users`, `restaurants`, `menu_items`, `payments`, `promotions`), an **EMR Spark step** is submitted via `EmrAddStepsOperator`.
3. The Spark job (`batch_etl.py`) connects to Aurora MySQL via **partitioned JDBC reads**, splitting each table across 8–32 parallel readers on the partition column (e.g., `order_id`).
4. Incremental state is tracked in a JSON checkpoint file — only rows updated since the last import are fetched (using the `updated_at` column).
5. **Transformations** are applied: decimal casting, timestamp normalization, and audit columns (`dt`, `_etl_loaded_at`).
6. **Data quality checks** run inline: null checks on critical columns (< 1% threshold), duplicate detection on primary keys.
7. The DataFrame is written in **dual mode**:
   - **Primary:** Appended to an Iceberg table in AWS Glue Data Catalog (`iceberg.zomato_raw.<table>`) in ORC format with Snappy compression.
   - **Secondary:** Written as raw ORC files partitioned by `dt` to `s3://<bucket>/pipeline1-batch-etl/orc/<table>/`.
8. On completion, Airflow logs quality metrics and updates the incremental checkpoint.

#### Flink CEP Sub-Pipeline (Pipeline 1 Extension)

Pipeline 1 also includes a **Flink Complex Event Processing** application that runs continuously on Amazon Managed Flink. It consumes from the `orders` MSK topic and applies two detection patterns:

- **Fraud Detection:** Identifies users placing orders from 2+ distinct delivery addresses within a 5-minute window using keyed state and event-time timers.
- **Cancellation Anomaly Detection:** Detects cancellation spikes per restaurant via sliding windows (10-min window, 1-min slide) compared against an EMA baseline.

Alerts are written to Iceberg tables (`fraud_alerts`, `cancellation_anomalies`) via the AWS Glue catalog.

---

### 3.2 Pipeline 2: CDC (Change Data Capture)

**Path:** Aurora MySQL (binlog) → Debezium (ECS Fargate) → MSK Cluster 1 (Avro) → Flink CDC (Managed Flink) → Iceberg/ORC → S3

**Schedule:** Continuous (real-time, SLA < 10 minutes lag)

**Purpose:** Near-real-time capture of every INSERT, UPDATE, and DELETE on the Aurora MySQL source tables, delivering change events to the data lake within minutes. This is the streaming complement to Pipeline 1's batch ETL.

#### Execution Steps

1. **Debezium MySQL connectors** run on ECS Fargate in distributed Kafka Connect mode. Four connectors capture changes from `orders`, `users`, `menu_items`, and `promotions`.
2. Aurora MySQL binlog is read by Debezium (server IDs 1001–1004), which publishes Avro-serialized change events to **MSK Cluster 1** topics (`orders`, `users`, `menu`, `promo`). Schema registration is handled by **AWS Glue Schema Registry**.
3. **Amazon Managed Flink** (application: `pipeline2-cdc`) consumes Avro events from MSK, applies the `ExtractNewRecordState` transform, and writes upserts to Iceberg tables on S3 via the Glue catalog.
4. Iceberg v2 merge-on-read handles upserts using equality delete files keyed on primary keys (`order_id`, `user_id`, etc.).
5. The Flink application checkpoints every 60 seconds to S3 with exactly-once semantics and RocksDB state backend.

#### How Airflow Monitors Pipeline 2

The `pipeline2_cdc_management` DAG runs **every 10 minutes** and performs:

1. **Debezium connector health check:** Queries the Kafka Connect REST API for connector and task states. If any connector/task is not `RUNNING`, the DAG automatically restarts failed tasks.
2. **MSK consumer lag check:** Verifies that Flink consumer groups are keeping up with the Kafka topic offsets.
3. **Managed Flink application health:** Queries the `kinesisanalyticsv2` API to confirm the CDC Flink application is in `RUNNING` status.
4. **Data freshness validation:** Runs Athena queries against the Iceberg tables to verify records have been written within the last hour.
5. **CloudWatch metric publication:** Publishes healthy connector counts and stale table counts for dashboarding and alarming.

---

### 3.3 Pipeline 3: DynamoDB Streams

**Path:** DynamoDB (Streams) → ECS Multi-AZ → S3 (JSON) → Spark (Amazon EMR) → ORC → S3

**Schedule:** Hourly (`@hourly`)

**Purpose:** Captures operational data from DynamoDB tables (orders, payments, user locations) that is not stored in Aurora MySQL, converts it from JSON to ORC, and lands it in the data lake for analytical querying.

#### Execution Steps

1. **DynamoDB Streams** are enabled on three tables: `orders`, `payments`, and `user_locations` (all with `NEW_AND_OLD_IMAGES` view type).
2. An **ECS Multi-AZ service** (6 desired tasks, spread across 3 AZs) consumes stream records in real-time micro-batches. Each record is deserialized from DynamoDB's typed attribute format into flat JSON, enriched with metadata (`event_id`, `table_name`, `processed_at`), and written to S3 as newline-delimited JSON files partitioned by `table_name/YYYY/MM/DD/HH`.
3. Every hour, Airflow triggers the `pipeline3_dynamodb_spark` DAG.
4. An **EMR Spark step** (`spark_orc_converter.py`) reads the previous hour's JSON files from S3, applies per-table processing:
   - **Orders:** Extracts nested data fields, casts numeric types, deduplicates by `order_id`, adds `dt` and `hour` partition columns.
   - **Payments:** Extracts payment fields, partitions by `dt`.
   - **User Locations:** Extracts GPS coordinates, partitions by `dt` and `hour`.
5. Each table's DataFrame is written as **ORC with Snappy compression** to `s3://<bucket>/pipeline3-dynamodb/orc/<table>/`.
6. After the Spark job completes, a **Glue Data Catalog partition refresh** is triggered via Athena (`MSCK REPAIR TABLE`) so new partitions are immediately queryable.

---

### 3.4 Pipeline 4: Real-time Events

**Path:** Microservices/Web/Mobile → Custom Producer → MSK Cluster 1 → Flink Real-time (Managed Flink) → S3 (ORC) + MSK Cluster 2 → EC2 Consumer Fleet → Apache Druid

**Schedule:** Continuous (real-time, SLA < 5 minutes end-to-end)

**Purpose:** Captures all application-level events (order placements, user logins, menu views, delivery tracking, etc.) and makes them available for both batch analytics (S3/Athena) and sub-second real-time OLAP (Druid).

#### Execution Steps

1. A **Custom Kafka Producer** (`event_producer.py`) collects events from Zomato's microservices, web application, and mobile backends. Events are routed to MSK topics based on type: `orders`, `users`, `menu`, `promo`, or `topics` (generic).
2. **Amazon Managed Flink** (application: `pipeline4-realtime`) consumes from all five topics on MSK Cluster 1 with dual output:
   - **Path A (Batch):** Events are flattened, enriched with location/device metadata, and written as ORC files to S3 partitioned by `dt` and `hour`. Rolling policy: 256 MB file size or 10-minute rollover.
   - **Path B (Real-time):** Events are transformed and produced to the `druid-ingestion-events` topic on **MSK Cluster 2** (the secondary cluster dedicated to Druid ingestion).
3. **Complex Event Processing** runs within the same Flink application: 5-minute tumbling windows detect order velocity spikes per city (thresholds: >100 NORMAL, >500 WARNING, >1000 CRITICAL). Spike alerts are written back to the `order-spike-alerts` MSK topic.
4. An **EC2 Auto-Scaling consumer fleet** (R8g instances, 3–24 nodes) consumes from MSK Cluster 2 and feeds events into **Apache Druid** via Kafka ingestion supervisors.
5. Druid ingests events with HOUR segment granularity and MINUTE query granularity, applying rollup with HLLSketch for unique user counts. Deep storage is on S3; segment caching uses local SSD on R8g Historical nodes.

#### Why Two MSK Clusters?

MSK Cluster 1 (9 brokers, `kafka.r8g.4xlarge`) handles high-throughput event collection from all producers and Flink consumers. MSK Cluster 2 (6 brokers, `kafka.r8g.2xlarge`) is dedicated to Druid ingestion, providing resource isolation so that Druid's consumption patterns (high-frequency, low-latency pulls) don't interfere with the primary event pipeline. The EC2 consumer fleet bridges the two clusters, performing lightweight enrichment and batching.

#### How Airflow Monitors Pipeline 4

The `pipeline4_realtime_monitoring` DAG runs **every 5 minutes** and checks:

1. **Flink streaming job health:** Queries the Managed Flink API for application status, checkpoint configuration, parallelism, and CloudWatch metrics (checkpoint duration, failures).
2. **Druid ingestion supervisor health:** Checks all Kafka ingestion supervisors via the Druid Overlord API for healthy state and active task counts.
3. **Druid segment freshness:** Verifies that recent segments are loaded on Historical nodes via the Coordinator API.
4. **End-to-end latency:** Runs a Druid SQL query to measure the time gap between `now()` and the latest event timestamp in the `zomato_realtime_events` datasource. Alerts if latency exceeds 30 seconds.
5. **CloudWatch metric publication:** Publishes running job count, alert count, and E2E latency for dashboarding.

---

## 4. Airflow Workflows — Roles and Responsibilities

### 4.1 Workflow Inventory

| DAG ID | Schedule | Purpose | Pipelines Involved |
|--------|----------|---------|-------------------|
| `pipeline1_batch_etl` | `0 */6 * * *` | Imports Aurora MySQL tables via Spark JDBC to Iceberg/ORC on S3 | Pipeline 1 |
| `pipeline2_cdc_management` | `*/10 * * * *` | Monitors Debezium connectors, Flink CDC, validates data freshness | Pipeline 2 |
| `pipeline3_dynamodb_spark` | `@hourly` | Converts DynamoDB stream JSON to ORC via EMR Spark | Pipeline 3 |
| `pipeline4_realtime_monitoring` | `*/5 * * * *` | Monitors Flink streaming, Druid ingestion, E2E latency | Pipeline 4 |
| `athena_etl_queries` | `0 2 * * *` | Daily Athena SQL aggregations across the data lake | Query layer (all pipelines) |
| `data_quality_checks` | `0 */4 * * *` | Cross-pipeline data freshness, row counts, schema conformance | All pipelines |
| `datalake_compaction` | `@daily` | Iceberg table maintenance: file compaction, snapshot expiry, orphan cleanup | Data lake (all pipelines) |
| `druid_maintenance` | `0 3 * * *` | Druid segment compaction, retention enforcement, zombie task cleanup | Pipeline 4 (Druid) |

### 4.2 DAG Dependency and Interaction Map

```
                              pipeline1_batch_etl (every 6h)
                                       │
                                       ▼
                              S3 Iceberg Tables
                                       │
    pipeline2_cdc_management ──────────┤
           (every 10m)                 │
                                       │
    pipeline3_dynamodb_spark ──────────┤
           (hourly)                    │
                                       │
    pipeline4_realtime_monitoring ─────┤
           (every 5m)                  │
                                       ▼
                              ┌─────────────────┐
                              │ data_quality_    │ ◄── Validates freshness/counts
                              │ checks (4h)     │     across ALL pipeline outputs
                              └────────┬────────┘
                                       │
                              ┌────────▼────────┐
                              │ athena_etl_     │ ◄── Daily aggregations that
                              │ queries (2 AM)  │     JOIN data from all pipelines
                              └────────┬────────┘
                                       │
                              ┌────────▼────────┐
                              │ datalake_       │ ◄── Compacts Iceberg tables
                              │ compaction      │     written by P1, P2, P3
                              │ (daily)         │
                              └─────────────────┘

                              ┌─────────────────┐
                              │ druid_          │ ◄── Compacts Druid segments
                              │ maintenance     │     from Pipeline 4
                              │ (3 AM daily)    │
                              └─────────────────┘
```

### 4.3 Detailed DAG Descriptions

#### `pipeline1_batch_etl`

- **Owner:** data-engineering
- **Trigger:** Cron every 6 hours
- **Max active runs:** 1
- **Retry policy:** 2 retries, 5-minute delay, 2-hour execution timeout
- **Task flow:**
  1. `start` — Log execution timestamp.
  2. `spark_jdbc_imports` (TaskGroup) — For each of the 6 tables, submits an EMR step (`EmrAddStepsOperator`) and waits for completion (`EmrStepSensor`, 30-second polling, 1-hour timeout). Steps run in parallel within the group.
  3. `log_quality_metrics` — Logs quality check results (inline in Spark jobs, published to CloudWatch).
  4. `end` — Log completion.

#### `pipeline2_cdc_management`

- **Owner:** data-engineering
- **Trigger:** Every 10 minutes
- **Max active runs:** 1
- **Retry policy:** 1 retry, 2-minute delay, 30-minute timeout
- **Task flow:**
  1. `start` — Log execution.
  2. `monitor_connectors` (TaskGroup):
     - `check_connector_status` — HTTP GET to Kafka Connect REST API for all 4 connectors.
     - `branch_on_health` — BranchPythonOperator routes to either `restart_failed_connectors` or `connectors_healthy`.
     - `restart_failed_connectors` — POSTs restart commands to failed connector tasks.
  3. `check_kafka_lag` — Validates MSK consumer group lag for CDC topics.
  4. `check_flink_jobs` — Queries `kinesisanalyticsv2` API for Managed Flink CDC application status.
  5. `validate_data_freshness` — Runs Athena queries to verify Iceberg tables have data within the last hour.
  6. `publish_cdc_metrics` — Publishes CloudWatch metrics (`trigger_rule=all_done`).
  7. `end`.

#### `pipeline3_dynamodb_spark`

- **Owner:** data-engineering
- **Trigger:** Hourly
- **Max active runs:** 2 (allows overlap for catch-up)
- **Retry policy:** 3 retries, 5-minute delay, 1-hour timeout
- **Task flow:**
  1. `start` — Log execution with Jinja-templated timestamp.
  2. `submit_spark_job` — `EmrAddStepsOperator` submits the `spark_orc_converter.py` step with the execution date formatted as `YYYY/MM/DD/HH`.
  3. `wait_spark_job` — `EmrStepSensor` polls every 30 seconds, 1-hour timeout.
  4. `refresh_glue_catalog` — Runs `MSCK REPAIR TABLE` via Athena CLI to register new ORC partitions.
  5. `end`.

#### `pipeline4_realtime_monitoring`

- **Owner:** data-engineering
- **Trigger:** Every 5 minutes
- **Max active runs:** 1
- **Retry policy:** 1 retry, 2-minute delay, 15-minute timeout
- **Task flow:**
  1. `start` — Log execution.
  2. `monitor_flink` (TaskGroup) — `check_flink_jobs` queries Managed Flink API, extracts checkpoint and parallelism details, checks CloudWatch for checkpoint duration metrics.
  3. `monitor_druid` (TaskGroup):
     - `check_druid_ingestion` — Queries Druid Overlord for supervisor health and active task counts.
     - `check_segment_freshness` — Queries Druid Coordinator for segment availability per datasource.
  4. `check_e2e_latency` — Druid SQL query measuring latest event time vs. current time.
  5. `publish_realtime_metrics` — CloudWatch publication (`trigger_rule=all_done`).
  6. `end`.

#### `athena_etl_queries`

- **Owner:** data-engineering
- **Trigger:** Daily at 2:00 AM
- **Max active runs:** 1
- **Retry policy:** 2 retries, 10-minute delay, 3-hour timeout
- **Depends on past:** Yes (ensures sequential daily execution)
- **Purpose:** Runs four daily aggregation queries against the Iceberg data lake via Athena's ETL workgroup:
  1. `daily_order_summary` — Revenue, order counts, and cancellation rates per restaurant and city.
  2. `daily_user_activity` — Active users, pro users, and average spend per city.
  3. `popular_items_by_city` — Top menu items by order count and revenue per city and cuisine.
  4. `promo_effectiveness` — Promotion usage, order value impact, and discount totals.
- These queries **JOIN data from multiple pipelines**: orders from Pipeline 1/2, menu data from Pipeline 2, and user data from Pipeline 1/2.

#### `data_quality_checks`

- **Owner:** data-engineering
- **Trigger:** Every 4 hours
- **Max active runs:** 1
- **Purpose:** Cross-pipeline validation that runs independently of individual pipeline DAGs:
  1. `check_data_freshness` — Validates all four pipelines are within SLA (P1: 7h, P2: 10m, P3: 2h, P4: 5m).
  2. `check_row_counts` — Verifies minimum expected daily record counts (orders: 2M, users: 100K, etc.).
  3. `schema_checks` (TaskGroup) — Runs `DESCRIBE` via Athena on each core Iceberg table to detect schema drift.
  4. `check_msk_lag` — Runs `kafka-consumer-groups.sh` against MSK to flag consumer groups with lag > 1M.

#### `datalake_compaction`

- **Owner:** data-engineering
- **Trigger:** Daily
- **Max active runs:** 1
- **Retry policy:** 2 retries, 15-minute delay, 4-hour timeout
- **Purpose:** Iceberg table maintenance for all tables written by Pipelines 1, 2, and 3. For each of 5 tables (`orders`, `users`, `menu_items`, `payments`, `promotions`):
  1. `rewrite_data_files` — Merges small files into optimally-sized ORC files (128–512 MB target depending on table).
  2. `expire_snapshots` — Removes snapshots older than 5–14 days (table-dependent).
  3. `remove_orphan_files` — Cleans up unreferenced S3 data files (2–5 day retention).
- All table maintenance groups run in parallel; a final `log_compaction_metrics` task runs after all complete.

#### `druid_maintenance`

- **Owner:** data-engineering
- **Trigger:** Daily at 3:00 AM
- **Max active runs:** 1
- **Retry policy:** 2 retries, 10-minute delay, 2-hour timeout
- **Purpose:** Maintains the Druid OLAP cluster's segment health:
  1. `compaction` — Submits auto-compaction configurations to the Druid Coordinator for each datasource (target: 500 MB segments, 5M rows/segment).
  2. `retention` — Marks segments older than the retention period (60–180 days depending on datasource) as unused for garbage collection.
  3. `cleanup` — Identifies and kills zombie ingestion tasks running longer than 6 hours.
  4. `health_monitoring` — Checks segment counts, load percentages, and availability for all datasources.
  5. `publish_druid_metrics` — Publishes total segments, size, healthy datasource count, and zombie kill count to CloudWatch.

---

## 5. Cross-Pipeline Integration Points

### 5.1 Shared Storage: S3 Data Lake

All four pipelines write to the same S3 data lake buckets, organized by pipeline prefix:

| Prefix | Pipeline | Format | Table Format |
|--------|----------|--------|--------------|
| `pipeline1-batch-etl/orc/` | Pipeline 1 | ORC (Snappy) | Iceberg + raw ORC |
| `pipeline2-cdc/iceberg/` | Pipeline 2 | ORC (Snappy) | Iceberg (upsert-enabled) |
| `pipeline3-dynamodb/orc/` | Pipeline 3 | ORC (Snappy) | Raw ORC partitioned |
| `pipeline4-realtime/flink-output/` | Pipeline 4 | ORC (Snappy) | Raw ORC partitioned |

### 5.2 Shared Catalog: AWS Glue Data Catalog

All Iceberg tables, ORC partitions, and metadata are registered in the **AWS Glue Data Catalog**, which replaced the self-hosted Hive Metastore. Three Glue databases organize the data:

- `zomato_raw` — Landing zone for Pipeline 1 batch imports
- `zomato_curated` — Cleaned, deduped data from Pipeline 2 CDC and Pipeline 3
- `zomato_gold` — Pre-aggregated metrics from `athena_etl_queries` DAG

### 5.3 Shared Messaging: Amazon MSK

**MSK Cluster 1** (9 brokers) serves as the shared event bus for:
- Pipeline 2: Debezium CDC events (`orders`, `users`, `menu`, `promo`)
- Pipeline 4: Application events from all sources (same topic names, different consumer groups)
- Pipeline 1 (Flink CEP): Consumes `orders` topic for fraud/anomaly detection

**MSK Cluster 2** (6 brokers) is dedicated to Pipeline 4's Druid ingestion path (`druid-ingestion-events`, `order-spike-alerts`).

### 5.4 Shared Compute: Amazon EMR

A single EMR cluster (R8g instances) is shared between Pipeline 1 (Spark JDBC ETL) and Pipeline 3 (DynamoDB JSON-to-ORC conversion). Airflow coordinates access via EMR step submission, and dynamic allocation ensures fair resource sharing.

### 5.5 Data Flow Between Pipelines

The `athena_etl_queries` DAG is the primary point where data from all pipelines is **joined**:

- `daily_order_summary` joins **Pipeline 1/2 orders** with restaurant data
- `daily_user_activity` joins **Pipeline 1/2 users** with **Pipeline 1/2 orders**
- `popular_items_by_city` aggregates **Pipeline 2 menu** data
- `promo_effectiveness` joins **Pipeline 1/2 orders** with **Pipeline 2 promo** data

The `data_quality_checks` DAG validates data from **all four pipelines** in a single workflow, ensuring end-to-end consistency.

---

## 6. Query Layer

### 6.1 Amazon Athena (Serverless, Trino-Based)

Three workgroups provide workload isolation:

| Workgroup | Purpose | Bytes Scanned Cutoff | Concurrent Queries |
|-----------|---------|---------------------|--------------------|
| `zomato-adhoc` | Interactive analyst queries | 10 GB | 50 |
| `zomato-etl` | Airflow-driven transformations | 100 GB | 20 |
| `zomato-reporting` | Superset/Redash dashboard queries | 50 GB | 100 |

### 6.2 Apache Druid (Real-Time OLAP)

Druid provides sub-second query response on 20B events/week for real-time dashboards. It is deployed on R8g instances (Coordinator, Broker, Historical, MiddleManager, Router) with S3 deep storage and local SSD segment caching.

### 6.3 Serving Layer

- **Apache Superset** (ECS Fargate + ElastiCache Redis) — Primary dashboarding platform connecting to both Athena and Druid.
- **Redash** (ECS Fargate + ElastiCache Redis) — Ad-hoc SQL analytics tool for analysts.
- **JupyterHub** (ECS Fargate) — Data science notebooks connecting to Athena, Druid, and S3 directly.

---

## 7. Monitoring and Alerting

### 7.1 CloudWatch

- **Dashboards:** Overview dashboard tracking Kafka messages/minute, Flink records processed, Druid query latency, Athena active queries, and per-pipeline lag.
- **Alarms:** Kafka consumer lag > 1M, Flink checkpoint failures, Druid p99 latency > 5s.
- **SNS:** Alert routing for SLA breaches to `#data-platform-alerts`.

### 7.2 Prometheus + Grafana

Four Grafana dashboards provide deep operational visibility:

- **Pipeline Health** — End-to-end data freshness, throughput, error rates, DLQ growth, Airflow DAG status.
- **Kafka Overview** — Broker health, throughput, consumer lag, ISR shrinks, request latency.
- **Flink Jobs** — Job status, restarts, checkpoint duration/failures, backpressure, TaskManager resources.
- **Druid Ingestion** — Cluster health, ingestion lag, events/sec, task success/failures, query latency, segment cache utilization.

### 7.3 Alertmanager Routing

Critical alerts route to **PagerDuty** and `#data-platform-critical` Slack channel. Service-specific warnings route to dedicated channels (`#data-platform-kafka`, `#data-platform-flink`, `#data-platform-athena`, `#data-platform-druid`). Order events pipeline alerts have a separate PagerDuty escalation path due to business criticality.

---

## 8. Infrastructure and Deployment

### 8.1 Terraform Modules

The platform is provisioned across three environments (`dev`, `staging`, `prod`) using 16 Terraform modules: `vpc`, `s3`, `aurora`, `dynamodb`, `kafka` (MSK), `kafka_consumer_fleet`, `flink` (Managed Flink), `emr`, `athena`, `druid`, `ecs`, `debezium`, `airflow` (MWAA), `superset`, `redash`, `jupyter`, and `monitoring`.

### 8.2 CI/CD

- **GitHub Actions:** Airflow DAG lint/validation, Flink build/test, Spark/PySpark tests, Terraform plan/apply, Docker build/push to ECR.
- **Jenkins (alternative):** Parallel lint → test → build → deploy pipeline with Kubernetes pod agents.
- **Deployment flow:** dev (automatic) → staging (automatic after dev) → prod (manual approval via GitHub Environment protection rules).

### 8.3 Production Scale

| Component | Dev | Staging | Production |
|-----------|-----|---------|------------|
| Aurora MySQL | 2× `db.r6g.2xlarge` | 2× `db.r6g.2xlarge` | 3× `db.r6g.4xlarge` |
| MSK Cluster 1 | 3× `kafka.r8g.2xlarge` | 6× `kafka.r8g.2xlarge` | 9× `kafka.r8g.4xlarge` |
| MSK Cluster 2 | 3× `kafka.r8g.xlarge` | 3× `kafka.r8g.xlarge` | 6× `kafka.r8g.2xlarge` |
| EC2 Consumer Fleet | 1–4× `r8g.large` | 2–8× `r8g.large` | 3–24× `r8g.xlarge` |
| EMR Core Nodes | 2× `r8g.2xlarge` | 3× `r8g.4xlarge` | 5× `r8g.4xlarge` (Spot) |
| Druid Historicals | 8× `r8g.8xlarge` | 8× `r8g.8xlarge` | 8× `r8g.8xlarge` |
| Athena | Serverless | Serverless | Serverless |
| MWAA Workers | 5–25 | 5–25 | 5–25 |
