# Production Setup Guide

This guide walks through deploying the Zomato Data Platform to AWS from scratch.

---

## How Production Maps to Local

| Local Service | Production Equivalent | AWS Service |
|---|---|---|
| MySQL 8.0 (Docker) | Amazon Aurora MySQL | RDS |
| Kafka + ZooKeeper (Docker) | Amazon MSK (2 clusters) | MSK |
| Confluent Schema Registry | AWS Glue Schema Registry | Glue |
| Debezium Kafka Connect | Debezium on ECS Fargate | ECS |
| Apache Flink (Docker) | Amazon Managed Flink | Kinesis Analytics v2 |
| DynamoDB Local (Docker) | Amazon DynamoDB | DynamoDB |
| Apache Spark (Docker) | Amazon EMR | EMR |
| DynamoDB Stream Processor (Docker) | AWS Lambda (DynamoDB Streams trigger) | Lambda |
| Druid Feeder (Docker) | EC2 ASG consumer fleet | EC2 + ASG |
| Trino (Docker) | Amazon Athena (serverless) | Athena |
| Apache Druid (Docker) | Apache Druid on EC2 R8g | EC2 + ASG |
| MinIO (Docker) | Amazon S3 | S3 |
| Apache Airflow (Docker) | Amazon MWAA | MWAA |

---

## Prerequisites

### Tools

| Tool | Minimum Version | Purpose |
|---|---|---|
| Terraform | 1.7+ | Infrastructure provisioning |
| AWS CLI | 2.15+ | AWS authentication and operations |
| Python | 3.11+ | Migration scripts, ops tooling |
| jq | 1.6+ | JSON parsing in shell scripts |
| make | 3.8+ | Task runner |

### AWS Requirements

- AWS account with admin or PowerUser IAM permissions
- S3 bucket for Terraform remote state (create manually before first deploy)
- ACM certificate ARN for HTTPS (if exposing internal ALBs)
- Route 53 hosted zone (optional, for DNS)

### AWS CLI Configuration

```bash
aws configure
# AWS Access Key ID: <your-key>
# AWS Secret Access Key: <your-secret>
# Default region: ap-south-1
# Default output format: json
```

Verify:

```bash
aws sts get-caller-identity
```

---

## Terraform State Backend

Before running any Terraform, create the remote state bucket manually:

```bash
aws s3 mb s3://zomato-data-platform-terraform-state --region ap-south-1
aws s3api put-bucket-versioning \
  --bucket zomato-data-platform-terraform-state \
  --versioning-configuration Status=Enabled
aws s3api put-public-access-block \
  --bucket zomato-data-platform-terraform-state \
  --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
```

Then configure the backend in `infra/terraform/environments/prod/backend.tf`:

```hcl
terraform {
  backend "s3" {
    bucket         = "zomato-data-platform-terraform-state"
    key            = "prod/terraform.tfstate"
    region         = "ap-south-1"
    encrypt        = true
  }
}
```

---

## Environment Variables

Create `infra/terraform/environments/prod/terraform.tfvars`:

```hcl
# Core
environment = "prod"
aws_region  = "ap-south-1"

# Aurora
aurora_master_password = "<strong-password>"   # stored in Secrets Manager after first apply

# MSK (no passwords — IAM auth)
# No credentials needed

# Flink
flink_application_bucket = "zomato-data-platform-prod-checkpoints"

# Athena
athena_results_bucket = "zomato-data-platform-prod-processed-data-lake"

# EMR
emr_master_instance_type = "r8g.2xlarge"
emr_core_instance_type   = "r8g.4xlarge"
emr_core_instance_count  = 5

# ACM (for internal ALBs — optional)
certificate_arn = "arn:aws:acm:ap-south-1:<account-id>:certificate/<cert-id>"
```

> **Security:** Never commit `terraform.tfvars` to git. Add it to `.gitignore`.

---

## Deployment Order

Services must be deployed in dependency order. The `deploy.sh` script handles this automatically, but the logical order is:

```
1. VPC
2. S3
3. Aurora MySQL
4. MSK (Cluster 1 — primary)
5. MSK (Cluster 2 — Druid ingestion)
6. ECS Cluster
7. Glue Schema Registry (part of Kafka module)
8. Debezium on ECS
9. Managed Flink
10. EMR
11. DynamoDB
12. Druid (EC2 + ASG)
13. Athena + Glue Catalog
14. MWAA (Airflow)
15. Monitoring (CloudWatch + SNS)
```

---

## Step 1 — Deploy Infrastructure

### Plan first (always)

```bash
make deploy-plan-prod
# or
bash infra/scripts/deploy.sh prod plan
```

Review the plan output carefully before applying.

### Apply

```bash
make deploy-prod
# or
bash infra/scripts/deploy.sh prod apply
```

This will prompt for explicit confirmation before applying to production:

```
WARNING: You are deploying to PRODUCTION.
Type 'yes-deploy-prod' to confirm:
```

### Verify outputs

```bash
cd infra/terraform/environments/prod
terraform output
```

Key outputs to note:

| Output | Description |
|---|---|
| `vpc_id` | VPC ID for all services |
| `aurora_cluster_endpoint` | Aurora write endpoint |
| `aurora_reader_endpoint` | Aurora read endpoint |
| `msk_bootstrap_brokers` | MSK Cluster 1 broker list (IAM auth) |
| `msk_bootstrap_brokers_2` | MSK Cluster 2 broker list (Druid ingestion) |
| `msk_zookeeper_connect` | ZooKeeper connection string |
| `ecs_cluster_id` | ECS cluster for Debezium |
| `emr_cluster_id` | EMR cluster for Spark jobs |
| `druid_coordinator_endpoint` | Druid coordinator (internal) |
| `druid_broker_endpoint` | Druid broker (internal) |
| `airflow_url` | MWAA web UI URL |
| `athena_workgroup_adhoc` | Athena adhoc workgroup name |

---

## Step 2 — Aurora MySQL Schema

Run the schema migration to initialise the Aurora database:

```bash
make migrate-aurora
# or
python infra/scripts/migration/schema_migration.py \
  --target aurora \
  --host <aurora_cluster_endpoint> \
  --user admin \
  --password <password> \
  --database zomato
```

This applies versioned migrations from `platform/pipelines/*/sql/` in sequence (V001__, V002__, ...).

**Verify:**

```bash
aws rds describe-db-cluster-parameters \
  --db-cluster-parameter-group-name zomato-prod-aurora-params \
  --query 'Parameters[?ParameterName==`binlog_format`].ParameterValue'
# Expected: ["ROW"]
```

Binary logging in ROW format is required for Pipeline 2 (Debezium CDC).

---

## Step 3 — Kafka Topics (MSK)

Create all production topics on both MSK clusters:

```bash
make msk-topics
# or
bash infra/scripts/ops/msk-create-topics.sh
```

This creates 25+ topics including:

| Topic | Cluster | Partitions | Retention | Purpose |
|---|---|---|---|---|
| `orders` | Cluster 1 | 256 | 7 days | Pipeline 2 & 4 |
| `users` | Cluster 1 | 64 | 7 days | Pipeline 2 & 4 |
| `menu` | Cluster 1 | 64 | 7 days | Pipeline 2 & 4 |
| `promo` | Cluster 1 | 64 | 7 days | Pipeline 2 & 4 |
| `druid-ingestion-events` | Cluster 2 | 128 | 2 days | Pipeline 4 → Druid |
| `debezium.zomato.*` | Cluster 1 | 16 | 7 days | Pipeline 2 CDC |
| `*.dlq` | Cluster 1 | 8 | 30 days | Dead-letter queues |
| `_schemas` | Cluster 1 | 1 | infinite | Schema Registry |

**Verify topics exist:**

```bash
aws kafka list-clusters --query 'ClusterInfoList[*].ClusterName'
# Then connect via kafka-topics.sh with IAM auth
```

---

## Step 4 — Debezium Connectors

Deploy the Debezium MySQL source connector to capture CDC from Aurora:

```bash
bash infra/scripts/register-connectors.sh
```

This registers two connectors via the Debezium REST API (port 8083 via CloudMap service discovery):

### MySQL Source Connector (`debezium-mysql-source`)

```json
# infra/configs/msk/connectors/debezium-mysql-source.json
{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "database.hostname": "<aurora_cluster_endpoint>",
  "database.include.list": "zomato",
  "table.include.list": "zomato.orders,zomato.users,zomato.menu_items,zomato.promotions",
  "snapshot.mode": "initial",
  "tasks.max": "3"
}
```

### S3 Sink Connector (`s3-sink`)

```json
# infra/configs/msk/connectors/s3-sink.json
{
  "connector.class": "io.confluent.connect.s3.S3SinkConnector",
  "topics": "orders,users,menu,promo",
  "s3.bucket.name": "zomato-data-platform-prod-raw-data-lake",
  "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
  "flush.size": "100000"
}
```

**Verify connector status:**

```bash
# Via Kafka Connect REST API (internal VPC access)
curl http://<debezium-service-discovery-endpoint>:8083/connectors
curl http://<debezium-service-discovery-endpoint>:8083/connectors/debezium-mysql-source/status
```

Expected state: `RUNNING` for connector and all tasks.

---

## Step 5 — Glue Schema Registry

The Glue Schema Registry is provisioned by Terraform as part of the Kafka module. Register Avro schemas for Pipeline 2 topics:

```bash
# Register schemas for CDC topics
aws glue create-registry \
  --registry-name zomato-prod \
  --compatibility BACKWARD \
  --data-format AVRO

# Register individual topic schemas
aws glue create-schema \
  --registry-id RegistryName=zomato-prod \
  --schema-name orders-value \
  --compatibility BACKWARD \
  --data-format AVRO \
  --schema-definition file://platform/schemas/orders.avsc
```

Debezium is configured to auto-register schemas on first event — manual registration is only needed if you want to pre-validate schemas before CDC starts.

---

## Step 6 — Managed Flink Applications

Three Flink applications are deployed by Terraform:

| Application | Pipeline | Parallelism | Input → Output |
|---|---|---|---|
| `pipeline1-cep` | Batch CEP | 8 | Kafka → S3 Iceberg |
| `pipeline2-cdc` | CDC | 16 | MSK (Debezium) → S3 Iceberg |
| `pipeline4-realtime` | Real-time | 32 | MSK Cluster 1 → S3 ORC + MSK Cluster 2 |

Upload the application JARs to S3 before starting:

```bash
# Build and upload Flink jobs
aws s3 cp platform/pipelines/pipeline2_cdc/target/pipeline2-cdc.jar \
  s3://zomato-data-platform-prod-checkpoints/flink-jars/

aws s3 cp platform/pipelines/pipeline4_realtime_events/target/pipeline4-realtime.jar \
  s3://zomato-data-platform-prod-checkpoints/flink-jars/
```

Start each application:

```bash
aws kinesisanalyticsv2 start-application \
  --application-name zomato-prod-pipeline2-cdc \
  --run-configuration '{"ApplicationRestoreConfiguration":{"ApplicationRestoreType":"SKIP_RESTORE_FROM_SNAPSHOT"}}'

aws kinesisanalyticsv2 start-application \
  --application-name zomato-prod-pipeline4-realtime \
  --run-configuration '{"ApplicationRestoreConfiguration":{"ApplicationRestoreType":"SKIP_RESTORE_FROM_SNAPSHOT"}}'
```

**Verify:**

```bash
aws kinesisanalyticsv2 describe-application \
  --application-name zomato-prod-pipeline4-realtime \
  --query 'ApplicationDetail.ApplicationStatus'
# Expected: "RUNNING"
```

---

## Step 7 — EMR (Spark)

Pipeline 1 (batch ETL) and Pipeline 3 (DynamoDB Streams) run Spark jobs on EMR. The cluster is provisioned by Terraform.

Upload Spark scripts to S3:

```bash
aws s3 cp platform/pipelines/pipeline1_batch_etl/src/ \
  s3://zomato-data-platform-prod-checkpoints/spark-scripts/pipeline1/ --recursive

aws s3 cp platform/pipelines/pipeline3_dynamodb_streams/src/ \
  s3://zomato-data-platform-prod-checkpoints/spark-scripts/pipeline3/ --recursive
```

Jobs are triggered by MWAA Airflow DAGs (see Step 9) — no manual submission needed.

**Verify cluster is waiting:**

```bash
aws emr describe-cluster \
  --cluster-id <emr_cluster_id> \
  --query 'Cluster.Status.State'
# Expected: "WAITING"
```

---

## Step 8 — Druid

Druid is deployed on EC2 R8g instances via Auto Scaling Groups (one per node type). All 5 node types are provisioned by Terraform:

| Node | Instance | Count | Purpose |
|---|---|---|---|
| Coordinator | r8g.2xlarge | 2 | Segment management, compaction |
| Broker | r8g.4xlarge | 4 | Query routing |
| Historical | r8g.8xlarge | 8 | Segment serving |
| MiddleManager | r8g.4xlarge | 6 | Ingestion tasks |
| Router | r8g.xlarge | 2 | Unified API gateway |

**Verify all nodes are running:**

```bash
# Coordinator console (internal VPC)
curl http://<druid-coordinator-endpoint>:8081/status/health
# Expected: {"healthy":true}

# Check all nodes are registered
curl http://<druid-coordinator-endpoint>:8081/druid/coordinator/v1/servers
```

Configure the Kafka ingestion supervisor for Pipeline 4 (Druid reads from MSK Cluster 2):

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d @platform/services/druid/ingestion-spec.json \
  http://<druid-coordinator-endpoint>:8081/druid/indexer/v1/supervisor
```

**Verify supervisor is running:**

```bash
curl http://<druid-coordinator-endpoint>:8081/druid/indexer/v1/supervisor/druid-ingestion-events/status
# Expected: "state": "RUNNING"
```

---

## Step 9 — MWAA (Airflow)

MWAA is fully provisioned by Terraform. Upload DAGs to the S3 DAGs bucket:

```bash
DAG_BUCKET=$(terraform -chdir=infra/terraform/environments/prod output -raw airflow_dags_bucket)

aws s3 sync platform/airflow/dags/ s3://${DAG_BUCKET}/dags/ \
  --exclude "__pycache__/*" \
  --exclude "*.pyc"
```

**Access the Airflow UI:**

```bash
terraform -chdir=infra/terraform/environments/prod output airflow_url
```

Open the URL in a browser. Default admin credentials are set via Secrets Manager — retrieve them:

```bash
aws secretsmanager get-secret-value \
  --secret-id zomato-prod-airflow-admin \
  --query SecretString --output text | jq .
```

**Enable DAGs** (all start paused by default):

Enable in order:

1. `pipeline1_batch_etl` — runs every 6 hours
2. `pipeline2_cdc` — monitors Debezium connector health
3. `pipeline3_dynamodb_spark` — runs hourly
4. `pipeline4_realtime` — monitors Flink application health
5. `data_quality` — runs daily
6. `datalake_compaction` — runs weekly
7. `druid_maintenance` — runs nightly

---

## Step 10 — Athena

Athena and the Glue Data Catalog are provisioned by Terraform. Three workgroups are created:

| Workgroup | Scan Limit | Concurrency | Purpose |
|---|---|---|---|
| `zomato-adhoc` | 10 GB | 50 | Interactive analyst queries |
| `zomato-etl` | 100 GB | 20 | Airflow-driven transformations |
| `zomato-reporting` | 50 GB | 100 | Dashboard queries |

Run the Athena schema migration to create Glue catalog tables:

```bash
make migrate-athena
# or
python infra/scripts/migration/schema_migration.py \
  --target athena \
  --workgroup zomato-etl \
  --results-bucket s3://zomato-data-platform-prod-processed-data-lake/athena-results/
```

**Verify with a test query:**

```bash
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM zomato_raw.orders LIMIT 1;" \
  --work-group zomato-adhoc \
  --result-configuration OutputLocation=s3://zomato-data-platform-prod-processed-data-lake/athena-results/
```

---

## Step 11 — Monitoring

CloudWatch dashboards and alarms are provisioned by Terraform. An SNS topic receives all alerts.

Subscribe your team to the SNS topic:

```bash
SNS_ARN=$(terraform -chdir=infra/terraform/environments/prod output -raw sns_alerts_arn)

aws sns subscribe \
  --topic-arn $SNS_ARN \
  --protocol email \
  --notification-endpoint ops-team@zomato.com
```

### Alarms provisioned

| Alarm | Threshold | Action |
|---|---|---|
| `kafka-consumer-lag` | > 1M messages for 15 min | SNS alert |
| `flink-checkpoint-failure` | > 0 failures in 5 min | SNS alert |
| `druid-high-latency` | p99 > 5s for 15 min | SNS alert |

### Ops scripts for manual health checks

```bash
# Kafka consumer lag across all groups
make ops-kafka-rebalance

# Athena workgroup health
make ops-athena-health

# Druid segment compaction
make ops-druid-compact

# Aurora snapshot
make ops-aurora-snapshot
```

---

## Step 12 — Aurora Backups

Automated snapshots are configured in Terraform (7-day retention). For on-demand snapshots:

```bash
make ops-aurora-snapshot
# or
bash infra/scripts/backup/aurora_snapshot.sh \
  --cluster-id zomato-prod-aurora \
  --retention 30
```

S3 lifecycle policies are applied to all data lake buckets by the `s3_lifecycle_policies.json` config:

| Prefix | Transitions | Expiry |
|---|---|---|
| `pipeline1-batch-etl/` | → IA (30d) → Glacier (90d) → Deep Archive (365d) | Never |
| `pipeline2-cdc/` | → IA (60d) → Glacier (180d) | Never |
| `pipeline3-dynamodb/json-raw/` | → IA (7d) → Glacier (30d) | 180 days |
| `pipeline4-realtime/` | → IA (14d) → Glacier (60d) | 365 days |
| `checkpoints/` | — | 7 days |

---

## Production Service Endpoints

All endpoints are internal (VPC-only). Access requires VPN or a bastion host.

| Service | Endpoint | Port | Notes |
|---|---|---|---|
| Aurora (write) | `<aurora_cluster_endpoint>` | 3306 | Use Secrets Manager for credentials |
| Aurora (read) | `<aurora_reader_endpoint>` | 3306 | Read-only replica |
| MSK Cluster 1 | `<msk_bootstrap_brokers>` | 9098 | IAM auth |
| MSK Cluster 2 | `<msk_bootstrap_brokers_2>` | 9098 | IAM auth, Druid ingestion |
| Debezium REST | `debezium.zomato-data.internal` | 8083 | CloudMap DNS |
| Druid Router | `<druid_router_endpoint>` | 8888 | Query API + console |
| Druid Coordinator | `<druid_coordinator_endpoint>` | 8081 | Admin API |
| Athena | AWS Console / API | — | Serverless |
| MWAA | `<airflow_url>` | 443 | Web UI |
| EMR | AWS Console | — | Job submission via Airflow |

---

## Tearing Down

To destroy an environment:

```bash
bash infra/scripts/deploy.sh prod destroy
```

> **Warning:** This destroys all infrastructure including S3 buckets and Aurora databases. Data is unrecoverable unless S3 versioning and Aurora PITR snapshots are preserved manually beforehand.

For non-production:

```bash
bash infra/scripts/deploy.sh dev destroy
bash infra/scripts/deploy.sh staging destroy
```

---

## Makefile Reference

```bash
# Deployment
make deploy-plan-prod      # Terraform plan for production
make deploy-prod           # Apply production infrastructure
make deploy-plan-dev       # Terraform plan for dev
make deploy-dev            # Apply dev infrastructure

# Kafka
make msk-topics            # Create all topics on MSK

# Schema migrations
make migrate-aurora        # Run Aurora MySQL migrations
make migrate-athena        # Run Athena/Glue migrations

# Operations
make ops-kafka-rebalance   # Check Kafka partition balance
make ops-athena-health     # Check Athena workgroup health
make ops-druid-compact     # Trigger Druid compaction
make ops-aurora-snapshot   # Take Aurora snapshot
```
