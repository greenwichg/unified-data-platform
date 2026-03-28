# Local Development Setup

This guide walks through setting up the full Zomato Data Platform on your local machine. The local environment simulates the production AWS architecture using Docker containers.

## How Local Maps to Production

| Local Service | Production Equivalent |
|---|---|
| MySQL 8.0 (Docker) | Amazon Aurora MySQL |
| Kafka + ZooKeeper (Docker) | Amazon MSK |
| Confluent Schema Registry | AWS Glue Schema Registry |
| Debezium Kafka Connect | Debezium on ECS Fargate |
| Apache Flink (Docker) | Amazon Managed Flink |
| Trino (Docker) | Amazon Athena (Trino-based, serverless) |
| Apache Druid (Docker) | Apache Druid on EC2 R8g |
| MinIO (Docker) | Amazon S3 |
| Apache Airflow (Docker) | Amazon MWAA |
| Apache Superset (Docker) | Apache Superset on ECS Fargate |

---

## Prerequisites

| Tool | Minimum Version | Purpose |
|---|---|---|
| Docker Desktop | 24+ | Container runtime |
| Docker Compose | 2.20+ | Multi-container orchestration |
| Python | 3.11+ | Seed / producer scripts |
| Make | Any | Convenience targets |
| Git | Any | Clone the repo |

Check your versions:

```bash
docker --version
docker compose version
python3 --version
make --version
```

> **Memory**: The full stack uses ~8 GB RAM. Allocate at least 10 GB to Docker Desktop under *Settings → Resources*.

---

## Quick Start (Automated)

The fastest path — one command starts the stack, seeds all data stores, and registers CDC connectors:

```bash
# 1. Clone the repository
git clone <repo-url>
cd unified-data-platform

# 2. Set up environment variables
cp local/.env.example local/.env

# 3. Start everything (stack + seed + CDC connectors)
make dev-setup
```

`make dev-setup` runs `docker-up`, waits for services to be healthy, seeds data, and registers Debezium connectors automatically.

---

## Step-by-Step Setup

If you prefer to control each step individually:

### Step 1 — Configure Environment Variables

```bash
cp local/.env.example local/.env
```

The defaults in `.env.example` work out of the box for local dev. Edit `local/.env` only if you need to change passwords.

| Variable | Default | Description |
|---|---|---|
| `MYSQL_ROOT_PASSWORD` | `rootpass` | MySQL root password |
| `MYSQL_USER` | `zomato_app` | Application DB user |
| `MYSQL_PASSWORD` | `zomato_pass` | Application DB password |
| `MINIO_ROOT_USER` | `minioadmin` | MinIO (S3 mock) admin user |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | MinIO admin password |
| `SUPERSET_SECRET_KEY` | `local-dev-secret-change-in-prod` | Superset session key |

### Step 2 — Install Python Dependencies

```bash
make build
# or manually:
pip3 install -r requirements.txt
```

### Step 3 — Start the Docker Stack

```bash
make docker-up
```

This pulls images and starts all services in the background. First run takes 3–5 minutes while images download.

Monitor startup progress:

```bash
make docker-ps        # show container status
make docker-logs      # tail all container logs
```

### Step 4 — Wait for Services to Be Healthy

Services have health checks and come up in dependency order. MySQL, Kafka, and kafka-connect take the longest (~60 seconds). Check they are all `healthy`:

```bash
docker compose -f local/docker-compose.yml --project-directory . ps
```

Expected output when ready:

```
NAME                STATUS
mysql               healthy
kafka-1             healthy
kafka-connect       healthy
schema-registry     running
flink-jobmanager    running
trino               running
druid-coordinator   running
airflow-webserver   running
minio               running
```

### Step 5 — Seed Data

Load initial records into MySQL and Kafka topics:

```bash
make seed
```

To seed individual targets:

```bash
make seed-mysql      # Aurora MySQL tables (orders, users, menu_items, promotions, …)
make seed-dynamodb   # DynamoDB tables (requires AWS credentials for real DynamoDB)
make seed-kafka      # Kafka topics (orders, users, menu, promo)
```

The seed script inserts realistic Zomato-style data: ~50 restaurants, ~100 users, ~500 orders, menu items, and promotions spread across 6 Indian cities.

### Step 6 — Register CDC Connectors (Pipeline 2)

Debezium connectors are auto-registered by the `connect-init` container on first startup. Verify they are running:

```bash
make cdc-status
```

Expected output:

```
  zomato-orders-cdc: RUNNING
  zomato-users-cdc:  RUNNING
  zomato-menu-cdc:   RUNNING
  zomato-promo-cdc:  RUNNING
```

If any connector shows `FAILED`, re-register manually:

```bash
make cdc-register
```

---

## Service Access Points

| Service | URL | Credentials |
|---|---|---|
| **Airflow** | http://localhost:8080 | admin / admin |
| **Flink UI** | http://localhost:8084 | — |
| **Trino** | http://localhost:8085 | any username, no password |
| **Druid Router** | http://localhost:8888 | — |
| **Kafka Connect** | http://localhost:8083 | — |
| **Schema Registry** | http://localhost:8081 | — |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Superset** | http://localhost:8088 | — |
| **MySQL** | localhost:3306 | root / rootpass |
| **Kafka Broker 1** | localhost:9092 | — |
| **Kafka Broker 2** | localhost:9093 | — |

---

## Data Generation

### One-Time Seed

Inserts a static batch of records into all data stores once:

```bash
make seed              # MySQL + Kafka (recommended for first run)
make seed-mysql        # MySQL only
make seed-kafka        # Kafka only
```

### Continuous Real-Time Producer

Streams events continuously, simulating live app traffic across all pipelines:

```bash
make produce           # all targets, 5 events/sec (default)
make produce-fast      # all targets, 23 events/sec (~2M orders/day)
make produce-timed     # all targets, 10 events/sec for 10 minutes
make produce-kafka     # Kafka only, 5 events/sec
make produce-mysql     # MySQL only, 5 events/sec
make produce-dynamodb  # DynamoDB only, 5 events/sec (requires AWS credentials)
```

To run the producer inside a Docker container (no local Python needed):

```bash
make produce-docker            # 5 events/sec, all targets
make produce-docker-fast       # 23 events/sec, all targets
make produce-docker-kafka      # Kafka only
make produce-docker-stop       # stop the producer container
```

Custom rate and duration:

```bash
PRODUCE_RATE=10 PRODUCE_DURATION=300 make produce-docker   # 10/sec for 5 minutes
PRODUCE_TARGET=kafka make produce-docker                    # Kafka only
```

---

## Pipeline-Specific Verification

### Pipeline 2 — CDC (MySQL → Kafka)

Test that a MySQL change flows into the Kafka `orders` topic:

```bash
make cdc-test
```

This inserts a test row into `zomato.orders`, waits 5 seconds, and checks that the CDC event appears in the `orders` Kafka topic.

You can also insert manually and observe:

```bash
# Insert a row
docker exec $(docker compose -f local/docker-compose.yml --project-directory . ps -q mysql) \
  mysql -uroot -prootpass zomato \
  -e "INSERT INTO orders (order_id, user_id, restaurant_id, status, total_amount, city, created_at, updated_at) \
      VALUES ('test-001', 'u-1', 'r-1', 'PLACED', 350.00, 'Mumbai', NOW(), NOW())
      ON DUPLICATE KEY UPDATE status='PLACED';"

# Check Kafka
docker exec $(docker compose -f local/docker-compose.yml --project-directory . ps -q kafka-1) \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders --from-beginning --max-messages 5 --timeout-ms 5000
```

### Pipeline 3 — DynamoDB Streams → Spark

The local stack simulates the Spark processing step using Trino instead of EMR. Real DynamoDB Streams testing requires AWS credentials and a real DynamoDB table with streams enabled.

For local testing of the Spark logic, run the converter directly:

```bash
python3 platform/pipelines/pipeline3_dynamodb_streams/src/spark_orc_converter.py \
  --raw-bucket zomato-data-platform-dev-raw-data-lake \
  --output-bucket zomato-data-platform-dev-processed-data-lake
```

### Pipeline 4 — Real-time Events (Kafka → Flink → S3 + Druid)

Start the real-time producer (produces to Kafka topics):

```bash
make produce-kafka
```

Check that events are flowing into the Kafka topics:

```bash
# List topic offsets
docker exec $(docker compose -f local/docker-compose.yml --project-directory . ps -q kafka-1) \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group flink-realtime-events --describe
```

Query results in Trino (local Athena equivalent):

```bash
docker exec -it $(docker compose -f local/docker-compose.yml --project-directory . ps -q trino) \
  trino --server localhost:8080 --catalog iceberg --schema zomato_cdc \
  --execute "SELECT COUNT(*) FROM orders LIMIT 1;"
```

---

## Airflow DAGs

The Airflow UI at http://localhost:8080 (admin / admin) shows all four pipeline DAGs:

| DAG | Schedule | What it does |
|---|---|---|
| `pipeline1_batch_etl` | Every 6 hours | Spark JDBC → S3 ORC + Iceberg |
| `pipeline2_cdc` | Continuous | Monitors Debezium connector health |
| `pipeline3_dynamodb_spark` | Hourly | Submits Spark job for JSON → ORC |
| `pipeline4_realtime` | Continuous | Monitors Flink job health |

To trigger a DAG manually from the CLI:

```bash
docker exec $(docker compose -f local/docker-compose.yml --project-directory . ps -q airflow-webserver) \
  airflow dags trigger pipeline3_dynamodb_spark
```

---

## Running Tests

```bash
make test              # full suite (unit + integration + e2e) with coverage
make test-unit         # unit tests only (fast, no Docker needed)
make test-int          # integration tests (requires Docker stack running)
make test-e2e          # end-to-end tests (requires Docker stack running)

# Per-pipeline tests
make test-pipeline1
make test-pipeline2
make test-pipeline3
make test-pipeline4
```

---

## Common Operations

### Stop the Stack

```bash
make docker-down                        # stop containers, preserve data volumes
docker compose -f local/docker-compose.yml --project-directory . down -v   # stop + delete volumes (fresh start)
```

### Restart a Single Service

```bash
docker compose -f local/docker-compose.yml --project-directory . restart kafka-connect
```

### View Logs for One Service

```bash
docker compose -f local/docker-compose.yml --project-directory . logs -f kafka-connect
docker compose -f local/docker-compose.yml --project-directory . logs -f mysql
```

### Connect to MySQL

```bash
docker exec -it \
  $(docker compose -f local/docker-compose.yml --project-directory . ps -q mysql) \
  mysql -uroot -prootpass zomato
```

### List Kafka Topics and Messages

```bash
# List all topics
docker exec $(docker compose -f local/docker-compose.yml --project-directory . ps -q kafka-1) \
  kafka-topics --bootstrap-server localhost:9092 --list

# Consume from a topic
docker exec $(docker compose -f local/docker-compose.yml --project-directory . ps -q kafka-1) \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders --from-beginning --max-messages 10
```

### Browse MinIO (S3 Mock)

Open http://localhost:9001 and log in with `minioadmin / minioadmin`.

Three buckets are pre-created:
- `zomato-data-platform-dev-raw-data-lake` — raw JSON from DynamoDB Streams
- `zomato-data-platform-dev-processed-data-lake` — ORC + Iceberg tables
- `zomato-data-platform-dev-checkpoints` — Flink checkpoints

---

## Troubleshooting

### `kafka-connect` keeps restarting

Wait for MySQL to be fully healthy before kafka-connect starts. Run:

```bash
make docker-ps
```

If MySQL shows `starting`, wait another 30 seconds. You can re-run CDC registration once it's healthy:

```bash
make cdc-register
```

### Port already in use

A port conflict means another service is using that port. Find and stop it:

```bash
lsof -i :3306    # MySQL
lsof -i :9092    # Kafka
lsof -i :8080    # Airflow
```

Or change the port mapping in `local/docker-compose.yml`.

### Seed script fails with connection error

Ensure MySQL is healthy before running seed:

```bash
docker compose -f local/docker-compose.yml --project-directory . ps mysql
# Should show: healthy
make seed
```

### Out of disk space

Docker volumes accumulate data. Clean unused volumes:

```bash
docker system prune --volumes
```

### Reset everything (fresh start)

```bash
make docker-down
docker compose -f local/docker-compose.yml --project-directory . down -v --remove-orphans
make dev-setup
```

---

## Local File Structure

```
local/
├── docker-compose.yml          # All services for local development
├── .env.example                # Template — copy to .env
├── .env                        # Your local env vars (git-ignored)
├── configs/
│   └── mysql-init/
│       └── 01-schema.sql       # Database schema loaded on first MySQL startup
├── scripts/
│   ├── create-kafka-topics.sh  # Creates all Kafka topics (local dev)
│   └── register-connectors.sh # Registers Debezium CDC connectors
└── tools/
    ├── seed_helpers.py         # Shared writers (MySQL, DynamoDB, Kafka)
    ├── seed_data.py            # One-time bulk seed script
    └── produce_realtime.py     # Continuous real-time event producer
```

Production infrastructure lives separately under `infra/` (Terraform, MSK configs, Trino configs, deployment scripts) and is not needed for local development.
