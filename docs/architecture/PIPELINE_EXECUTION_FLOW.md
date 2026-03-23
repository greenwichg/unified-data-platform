# End-to-End Pipeline Execution Flow

This document describes how the four data pipelines and their Airflow workflows connect, the role of each workflow, and how the pipelines interact within the overall architecture.

---

## Architecture Overview

```
┌──────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌──────────────┐
│ Aurora MySQL  │    │    DynamoDB       │    │   App Events    │    │   Airflow     │
│  (OLTP)      │    │  (NoSQL Store)    │    │  (450M msgs/min)│    │ (Orchestrator)│
└──┬───────┬───┘    └────────┬─────────┘    └───────┬─────────┘    └──────┬────────┘
   │       │                 │                      │                     │
   │       │                 │                      │              Orchestrates all
   ▼       ▼                 ▼                      ▼              pipelines below
Pipeline1  Pipeline2     Pipeline3              Pipeline4
(Batch)    (CDC)         (Streams)              (Realtime)
   │       │                 │                   │      │
   ▼       ▼                 ▼                   ▼      ▼
┌──────────────────────────────────┐         ┌──────────────┐
│        S3 Data Lake              │         │ Apache Druid  │
│  (Iceberg / ORC via Glue Catalog)│         │  (OLAP)      │
└──────────────┬───────────────────┘         └──────┬───────┘
               │                                    │
               ▼                                    ▼
        ┌─────────────┐                  ┌──────────────────┐
        │ Amazon Athena│                  │ Superset / Redash│
        │ (SQL Engine) │                  │  (Dashboards)    │
        └─────────────┘                  └──────────────────┘
```

---

## Pipeline 1: Batch ETL

**Path:** Aurora MySQL → Spark JDBC → Iceberg/ORC → S3

### What It Does

Performs full/incremental bulk imports of six relational tables (`orders`, `users`, `restaurants`, `menu_items`, `payments`, `promotions`) from the Aurora MySQL OLTP database into the S3 data lake in Iceberg table format (ORC storage).

### Execution Flow

1. **Airflow DAG** `pipeline1_batch_etl` triggers every **6 hours** (`0 */6 * * *`).
2. The DAG submits one **EMR Spark step per table** in parallel via `EmrAddStepsOperator`, inside a `TaskGroup("spark_jdbc_imports")`.
3. Each Spark job (`batch_etl.py`) does:
   - Partitioned JDBC reads from Aurora MySQL for parallel ingestion.
   - Decimal type casting, timestamp normalization.
   - Inline data quality checks (null checks, duplicate detection on primary keys).
   - Writes to Iceberg tables using the AWS Glue Catalog as metastore.
4. `EmrStepSensor` polls each step until completion (30s interval, 1h timeout).
5. After all table imports finish, the DAG logs quality metrics.

### Airflow DAG Task Graph

```
start → [spark_import_orders, spark_import_users, ...] → log_quality_metrics → end
         (parallel per table: submit + wait sensor)
```

### Key Configuration

| Setting | Value |
|---------|-------|
| Schedule | Every 6 hours |
| Spark executors | 8 (4 cores, 8 GB each) |
| Output format | Iceberg (ORC) |
| Catalog | AWS Glue |
| SLA threshold | Max 7 hours lag |

---

## Pipeline 2: Change Data Capture (CDC)

**Path:** Aurora MySQL → Debezium → Kafka → Flink → Iceberg/S3

### What It Does

Continuously captures row-level changes from Aurora MySQL via Debezium binlog reading, streams them through Kafka, and applies them as upserts into Iceberg tables on S3. This keeps the data lake near-real-time (<10 min latency) for four CDC tables: `orders`, `users`, `menu_items`, `promotions`.

### Execution Flow

1. **Debezium** (running on ECS Fargate) reads MySQL binlog and publishes Avro-serialized change events to four Kafka topics on MSK Cluster 1:
   - `zomato-cdc.zomato.orders` (64 partitions)
   - `zomato-cdc.zomato.users` (64 partitions)
   - `zomato-cdc.zomato.menu` (64 partitions)
   - `zomato-cdc.zomato.promo` (64 partitions)
2. **Amazon Managed Flink** (`flink_cdc_processor.py`) consumes from these topics, performs deduplication via Complex Event Processing, and writes UPSERT operations to Iceberg tables (keyed on primary keys, with 5-second watermark tolerance).
3. **Airflow DAG** `pipeline2_cdc_management` runs every **10 minutes** (`*/10 * * * *`) as a **monitoring and self-healing** workflow:
   - **Monitor connectors:** Queries the Kafka Connect REST API for all four Debezium connector statuses. If any are unhealthy, branches to auto-restart failed connector tasks.
   - **Check Kafka lag:** Validates consumer group lag on all CDC topics stays within thresholds.
   - **Check Flink:** Queries the Managed Flink (kinesisanalyticsv2) API to confirm the CDC application is `RUNNING`.
   - **Validate freshness:** Runs Athena queries against each Iceberg table to verify records exist within the last hour. Raises an alert if any table is stale.
   - **Publish metrics:** Sends `CDCHealthyConnectors`, `CDCTotalConnectors`, and `CDCStaleTables` to CloudWatch.

### Airflow DAG Task Graph

```
start → [check_connector_status → branch → restart | healthy]
                                                          ↓
                                              [check_kafka_lag, check_flink_jobs]
                                                          ↓
                                                validate_data_freshness
                                                          ↓
                                                  publish_cdc_metrics → end
```

### Key Configuration

| Setting | Value |
|---------|-------|
| Schedule (Airflow) | Every 10 minutes |
| Target latency | < 10 minutes |
| Kafka partitions | 64 per topic |
| Serialization | Avro (Glue Schema Registry) |
| Flink watermark | 5-second tolerance |
| Max lag threshold | 300 seconds |

---

## Pipeline 3: DynamoDB Streams

**Path:** DynamoDB → ECS Stream Processor → S3 (JSON) → Spark EMR → S3 (ORC)

### What It Does

Captures change streams from four DynamoDB tables (`zomato-user-sessions`, `zomato-delivery-tracking`, `zomato-restaurant-inventory`, `zomato-user-preferences`), buffers them as compressed JSON on S3, then converts to optimized ORC format via Spark on EMR.

### Execution Flow

1. **ECS Stream Processor** (multi-AZ, 3–18 auto-scaling instances) continuously reads DynamoDB Streams and writes gzipped JSON to S3 at `pipeline3-dynamodb/json-raw/` (64 MB buffer, 5-minute flush intervals).
2. **Airflow DAG** `pipeline3_dynamodb_spark` triggers **hourly** (`@hourly`):
   - Submits a Spark step to the shared EMR cluster via `EmrAddStepsOperator`.
   - The Spark job (`spark_orc_converter.py`) reads the hourly JSON partition, converts to ORC with Snappy compression (256 MB stripe size, bloom filters on key columns), and writes to `pipeline3-dynamodb/orc/`.
   - `EmrStepSensor` waits for completion (30s poll, 1h timeout).
   - After conversion, runs `MSCK REPAIR TABLE` via Athena to refresh the Glue Catalog with new partitions.

### Airflow DAG Task Graph

```
start → submit_spark_job → wait_spark_job → refresh_glue_catalog → end
```

### Key Configuration

| Setting | Value |
|---------|-------|
| Schedule | Hourly |
| EMR cluster | 1 master + 4 core nodes (r8g.4xlarge) |
| Dynamic allocation | 4–32 executors |
| Output format | ORC (Snappy) |
| Quality threshold | 99% source-to-target row match |
| SLA threshold | Max 2 hours lag |

---

## Pipeline 4: Real-time Events

**Path:** App Events → Kafka → Flink → S3 (ORC) + Druid (OLAP)

### What It Does

Ingests 450M messages/minute of application events (orders, deliveries, user activity, restaurant events) through Kafka, processes them with Flink for enrichment and spike detection, then writes to two sinks simultaneously: S3 for batch analytics and Druid for sub-second real-time queries.

### Execution Flow

1. **Application events** are produced to topics on MSK Cluster 1 (9 brokers, kafka.r8g.4xlarge).
2. **Amazon Managed Flink** (`realtime_job.py`, parallelism=32, auto-scaling) consumes events and:
   - Performs Complex Event Processing for **order spike detection** (500+ orders in a 5-minute window).
   - Applies session windowing (30-minute gap) and late-event handling (10-second tolerance).
   - Writes to **two output sinks in parallel**:
     - **S3 ORC Sink** (`s3_orc_sink.py`): Rolling files at 256 MB / 15-minute intervals, partitioned by `event_type/year/month/day/hour`.
     - **Druid Sink** (`druid_sink.py`): Publishes enriched events to MSK Cluster 2 (6 brokers) topic `druid-ingestion-events`.
3. **EC2 Consumer Fleet** (auto-scaling group, 4–24 instances) reads from Cluster 2 and feeds events into Druid via Kafka supervisor ingestion specs.
4. **Druid** ingests into datasources `zomato_realtime_events` (8 tasks) and `zomato_orders` (16 tasks) with HOUR segment granularity, MINUTE query granularity, and 2 replicas.
5. **Airflow DAG** `pipeline4_realtime_monitoring` runs every **5 minutes** (`*/5 * * * *`) as a **monitoring** workflow:
   - **Monitor Flink:** Queries the Managed Flink API for application status, checkpoint health (duration, failures), and parallelism configuration.
   - **Monitor Druid:** Checks all Kafka ingestion supervisor statuses via Overlord REST API, then validates segment freshness for all three datasources via Coordinator API.
   - **Check E2E latency:** Queries Druid SQL to measure `MAX(__time)` vs current time. Alerts if latency exceeds 30 seconds.
   - **Publish metrics:** Sends `RealtimeFlinkJobsRunning`, `RealtimeAlertCount`, and `RealtimeE2ELatencySeconds` to CloudWatch.

### Airflow DAG Task Graph

```
start → [monitor_flink, monitor_druid(check_ingestion → check_freshness)]
                              ↓
                      check_e2e_latency
                              ↓
                    publish_realtime_metrics → end
```

### Key Configuration

| Setting | Value |
|---------|-------|
| Schedule (Airflow) | Every 5 minutes |
| Target E2E latency | < 5 minutes (alert at 30s) |
| Flink parallelism | 32 (auto-scaling) |
| Kafka clusters | 2 (9 + 6 brokers) |
| Druid replicas | 2 per datasource |
| Max consumer lag | 50,000 records |

---

## Supporting Airflow Workflows

Beyond the four pipeline DAGs, four additional workflows maintain and consume data lake outputs.

### Data Quality Checks (`data_quality_checks`)

- **Schedule:** Every 4 hours (`0 */4 * * *`)
- **Purpose:** Cross-pipeline validation of data freshness (per-pipeline SLA), row counts (minimum thresholds per table), schema conformance (via `DESCRIBE` on Iceberg tables), and MSK consumer lag monitoring.
- **Covers all four pipelines** with SLA thresholds: Pipeline 1 (7h), Pipeline 2 (10min), Pipeline 3 (2h), Pipeline 4 (5min).

### Data Lake Compaction (`datalake_compaction`)

- **Schedule:** Daily (`@daily`)
- **Purpose:** Iceberg table maintenance via Athena SQL for all five core tables (`orders`, `users`, `menu_items`, `payments`, `promotions`).
- **Operations per table (sequential):** `rewrite_data_files` (compact small files) → `expire_snapshots` (remove old snapshots) → `remove_orphan_files` (clean up S3).
- **Tables run in parallel**, then a final metrics log step runs.

### Druid Maintenance (`druid_maintenance`)

- **Schedule:** Daily at 3 AM (`0 3 * * *`)
- **Purpose:** Keeps Druid healthy for the 8M queries/week it serves.
- **Operations (parallel then sequential):**
  - **Compaction:** Updates auto-compaction config (target 500 MB segments, 5M max rows).
  - **Retention:** Marks segments as unused beyond retention (90 days realtime, 180 days orders, 60 days delivery).
  - **Cleanup:** Kills zombie ingestion tasks running longer than 6 hours.
  - → **Health monitoring:** Checks segment counts, sizes, and load percentage per datasource.
  - → **Publish metrics** to CloudWatch (`DruidTotalSegments`, `DruidTotalSizeGB`, `DruidHealthyDatasources`, etc.).

### Athena ETL Queries (`athena_etl_queries`)

- **Schedule:** Daily at 2 AM (`0 2 * * *`)
- **Purpose:** Runs aggregation queries on the data lake to produce analytics/gold-layer tables consumed by dashboards.
- **Queries:** `daily_order_summary`, `daily_user_activity`, `popular_items_by_city`, `promo_effectiveness`.
- **Depends on** Pipeline 1 and Pipeline 2 having populated the Iceberg tables that day.

---

## How the Pipelines Interconnect

### Shared Infrastructure

| Resource | Used By |
|----------|---------|
| **EMR Cluster** | Pipeline 1 (Spark JDBC), Pipeline 3 (Spark ORC conversion) |
| **MSK Cluster 1** (9 brokers) | Pipeline 2 (CDC topics), Pipeline 4 (event topics) |
| **MSK Cluster 2** (6 brokers) | Pipeline 4 only (Druid ingestion buffer) |
| **AWS Glue Catalog** | All pipelines (Iceberg metastore + Athena catalog) |
| **S3 Data Lake** | All pipelines write here; Athena ETL and compaction operate on it |
| **Amazon Athena** | Pipeline 2 (freshness validation), Pipeline 3 (catalog refresh), compaction, ETL, quality checks |

### Data Flow Between Pipelines

```
Aurora MySQL ──────────────────────────────────────────────────────────
     │                              │
     │ (every 6h, bulk)             │ (continuous, binlog CDC)
     ▼                              ▼
  Pipeline 1                    Pipeline 2
  Spark JDBC                  Debezium → Kafka → Flink
     │                              │
     │    ┌─────────────────────────┤
     ▼    ▼                         │
  S3 Data Lake (Iceberg/ORC)        │
     │                              │
     ├──── Athena ETL (2 AM daily) ─┘  ← reads Iceberg tables from P1 + P2
     ├──── Compaction (daily)          ← maintains Iceberg tables from P1 + P2
     ├──── Quality Checks (4-hourly)   ← validates all pipelines
     │
     │
DynamoDB ──────────────────────────────────────────────────────────────
     │
     │ (continuous streams → hourly Spark)
     ▼
  Pipeline 3
  ECS → S3 JSON → Spark EMR → ORC
     │
     ▼
  S3 Data Lake (ORC) ──→ Glue Catalog refresh ──→ Athena queryable
     │
     │
App Events ────────────────────────────────────────────────────────────
     │
     │ (continuous, 450M msgs/min)
     ▼
  Pipeline 4
  Kafka → Flink → ┬──→ S3 Data Lake (ORC)  ──→ Athena queryable
                   └──→ Kafka Cluster 2 ──→ EC2 Fleet ──→ Druid
                                                            │
                                                            ▼
                                                   Superset / Redash
                                                   (real-time dashboards)
```

### Pipeline Complementarity

- **Pipeline 1 + Pipeline 2** both source from Aurora MySQL but serve different needs. Pipeline 1 provides complete bulk snapshots every 6 hours (all 6 tables). Pipeline 2 provides near-real-time incremental updates for 4 high-change tables. Together they ensure the Iceberg tables are both complete and fresh.
- **Pipeline 3** is independent, covering DynamoDB-sourced operational data (sessions, delivery tracking, inventory, preferences) that doesn't exist in Aurora MySQL.
- **Pipeline 4** is independent, handling high-volume application event streams. It is the only pipeline that feeds Druid for sub-second real-time analytics. It also writes to S3 for historical event analysis via Athena.
- **Athena ETL** sits downstream of Pipelines 1 and 2, joining their Iceberg outputs to produce gold-layer analytics tables.
- **Data Quality Checks** span all four pipelines, enforcing per-pipeline SLA thresholds.
- **Compaction** maintains the Iceberg tables that Pipelines 1 and 2 write to.
- **Druid Maintenance** maintains the datasources that Pipeline 4 feeds.

### Scheduling Timeline (24-hour cycle)

```
00:00 ──┬── Pipeline 1 Batch ETL (runs at 00:00, 06:00, 12:00, 18:00)
        ├── Pipeline 2 CDC Monitoring (every 10 min, continuous)
        ├── Pipeline 3 DynamoDB Spark (every hour)
        ├── Pipeline 4 Realtime Monitoring (every 5 min, continuous)
        ├── Data Quality Checks (every 4 hours)
02:00 ──┼── Athena ETL Queries (daily at 2 AM)
03:00 ──┼── Druid Maintenance (daily at 3 AM)
        └── Data Lake Compaction (daily)
```

---

## Observability

All pipelines publish metrics to **CloudWatch** under the `ZomatoDataPlatform/` namespace:

| Namespace | Key Metrics | Source DAG |
|-----------|-------------|------------|
| `ZomatoDataPlatform/CDC` | `CDCHealthyConnectors`, `CDCStaleTables` | `pipeline2_cdc_management` |
| `ZomatoDataPlatform/Realtime` | `RealtimeFlinkJobsRunning`, `RealtimeE2ELatencySeconds` | `pipeline4_realtime_monitoring` |
| `ZomatoDataPlatform/Druid` | `DruidTotalSegments`, `DruidTotalSizeGB`, `DruidZombieTasksKilled` | `druid_maintenance` |

Failure callbacks on Pipeline 2, Pipeline 4, and Druid maintenance DAGs send alerts to Slack via `common.slack_alerts.on_failure_callback`. All DAGs send email alerts on failure to `data-alerts@zomato.com`.
