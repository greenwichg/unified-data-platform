"""
Pipeline 2 - Flink CDC Processor: Kafka (Avro) → Flink → Iceberg/ORC → S3

Reads CDC events from Kafka topics, performs complex event processing,
and writes to Apache Iceberg tables on S3 in ORC format.

Handles: Deduplication, late arrivals, schema evolution, and exactly-once semantics.
"""

import json
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("pipeline2_flink_cdc")

# Flink SQL for creating Kafka source tables and Iceberg sink tables
FLINK_SQL_STATEMENTS = {
    "create_kafka_source": """
        CREATE TABLE kafka_orders_cdc (
            order_id STRING,
            user_id STRING,
            restaurant_id STRING,
            status STRING,
            subtotal DECIMAL(10, 2),
            tax DECIMAL(10, 2),
            delivery_fee DECIMAL(10, 2),
            total_amount DECIMAL(10, 2),
            payment_method STRING,
            city STRING,
            created_at TIMESTAMP(3),
            updated_at TIMESTAMP(3),
            op_type STRING,
            WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'orders',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'properties.group.id' = 'flink-cdc-orders',
            'format' = 'avro-confluent',
            'avro-confluent.url' = '{schema_registry_url}',
            'scan.startup.mode' = 'earliest-offset'
        );
    """,
    "create_kafka_users_source": """
        CREATE TABLE kafka_users_cdc (
            user_id STRING,
            name STRING,
            email STRING,
            phone STRING,
            city STRING,
            signup_date TIMESTAMP(3),
            is_pro_member BOOLEAN,
            total_orders BIGINT,
            updated_at TIMESTAMP(3),
            op_type STRING,
            WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'users',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'properties.group.id' = 'flink-cdc-users',
            'format' = 'avro-confluent',
            'avro-confluent.url' = '{schema_registry_url}',
            'scan.startup.mode' = 'earliest-offset'
        );
    """,
    "create_kafka_menu_source": """
        CREATE TABLE kafka_menu_cdc (
            item_id STRING,
            restaurant_id STRING,
            name STRING,
            category STRING,
            cuisine_type STRING,
            price DECIMAL(10, 2),
            is_vegetarian BOOLEAN,
            is_available BOOLEAN,
            preparation_time_mins INT,
            rating FLOAT,
            updated_at TIMESTAMP(3),
            op_type STRING,
            WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'menu',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'properties.group.id' = 'flink-cdc-menu',
            'format' = 'avro-confluent',
            'avro-confluent.url' = '{schema_registry_url}',
            'scan.startup.mode' = 'earliest-offset'
        );
    """,
    "create_iceberg_orders_sink": """
        CREATE TABLE iceberg_orders (
            order_id STRING,
            user_id STRING,
            restaurant_id STRING,
            status STRING,
            subtotal DECIMAL(10, 2),
            tax DECIMAL(10, 2),
            delivery_fee DECIMAL(10, 2),
            total_amount DECIMAL(10, 2),
            payment_method STRING,
            city STRING,
            created_at TIMESTAMP(3),
            updated_at TIMESTAMP(3),
            op_type STRING,
            processing_time TIMESTAMP(3),
            dt STRING,
            PRIMARY KEY (order_id) NOT ENFORCED
        ) PARTITIONED BY (dt) WITH (
            'connector' = 'iceberg',
            'catalog-name' = 'zomato_iceberg',
            'catalog-type' = 'hive',
            'warehouse' = 's3://{s3_bucket}/pipeline2-cdc/iceberg',
            'format-version' = '2',
            'write.format.default' = 'orc',
            'write.orc.compress' = 'SNAPPY',
            'write.upsert.enabled' = 'true',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '10'
        );
    """,
    "create_iceberg_users_sink": """
        CREATE TABLE iceberg_users (
            user_id STRING,
            name STRING,
            email STRING,
            phone STRING,
            city STRING,
            signup_date TIMESTAMP(3),
            is_pro_member BOOLEAN,
            total_orders BIGINT,
            updated_at TIMESTAMP(3),
            processing_time TIMESTAMP(3),
            dt STRING,
            PRIMARY KEY (user_id) NOT ENFORCED
        ) PARTITIONED BY (dt) WITH (
            'connector' = 'iceberg',
            'catalog-name' = 'zomato_iceberg',
            'catalog-type' = 'hive',
            'warehouse' = 's3://{s3_bucket}/pipeline2-cdc/iceberg',
            'format-version' = '2',
            'write.format.default' = 'orc',
            'write.upsert.enabled' = 'true'
        );
    """,
    "insert_orders": """
        INSERT INTO iceberg_orders
        SELECT
            order_id,
            user_id,
            restaurant_id,
            status,
            subtotal,
            tax,
            delivery_fee,
            total_amount,
            payment_method,
            city,
            created_at,
            updated_at,
            op_type,
            CURRENT_TIMESTAMP AS processing_time,
            DATE_FORMAT(updated_at, 'yyyy-MM-dd') AS dt
        FROM kafka_orders_cdc;
    """,
    "insert_users": """
        INSERT INTO iceberg_users
        SELECT
            user_id,
            name,
            email,
            phone,
            city,
            signup_date,
            is_pro_member,
            total_orders,
            updated_at,
            CURRENT_TIMESTAMP AS processing_time,
            DATE_FORMAT(updated_at, 'yyyy-MM-dd') AS dt
        FROM kafka_users_cdc;
    """,
    "complex_event_order_velocity": """
        CREATE VIEW order_velocity AS
        SELECT
            restaurant_id,
            city,
            COUNT(*) AS order_count,
            SUM(total_amount) AS total_revenue,
            AVG(total_amount) AS avg_order_value,
            TUMBLE_START(updated_at, INTERVAL '5' MINUTE) AS window_start,
            TUMBLE_END(updated_at, INTERVAL '5' MINUTE) AS window_end
        FROM kafka_orders_cdc
        WHERE op_type IN ('INSERT', 'UPDATE')
        GROUP BY
            restaurant_id,
            city,
            TUMBLE(updated_at, INTERVAL '5' MINUTE);
    """,
}


def generate_flink_job_config(
    kafka_bootstrap: str,
    schema_registry_url: str,
    s3_bucket: str,
    checkpoint_dir: str,
) -> dict:
    """Generate the complete Flink job configuration."""
    return {
        "job_name": "zomato-cdc-to-iceberg",
        "parallelism": 32,
        "checkpoint_interval_ms": 60000,
        "checkpoint_dir": checkpoint_dir,
        "min_pause_between_checkpoints_ms": 30000,
        "state_backend": "rocksdb",
        "restart_strategy": {
            "type": "fixed-delay",
            "attempts": 10,
            "delay_ms": 30000,
        },
        "kafka": {
            "bootstrap_servers": kafka_bootstrap,
            "schema_registry_url": schema_registry_url,
            "consumer_config": {
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                "max.poll.records": "10000",
                "fetch.min.bytes": "1048576",
                "fetch.max.wait.ms": "500",
            },
        },
        "iceberg": {
            "warehouse": f"s3://{s3_bucket}/pipeline2-cdc/iceberg",
            "catalog_type": "hive",
            "write_format": "orc",
            "upsert_enabled": True,
        },
        "sql_statements": {
            key: stmt.format(
                kafka_bootstrap=kafka_bootstrap,
                schema_registry_url=schema_registry_url,
                s3_bucket=s3_bucket,
            )
            for key, stmt in FLINK_SQL_STATEMENTS.items()
        },
    }


def write_flink_job_config(config: dict, output_path: str) -> None:
    """Write the Flink job config to a JSON file for the Flink job to consume."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(config, f, indent=2)
    logger.info("Flink job config written to: %s", output_path)


if __name__ == "__main__":
    config = generate_flink_job_config(
        kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        schema_registry_url=os.environ.get(
            "SCHEMA_REGISTRY_URL", "http://localhost:8081"
        ),
        s3_bucket=os.environ.get("S3_BUCKET", "zomato-data-platform-dev-raw-data-lake"),
        checkpoint_dir=os.environ.get(
            "CHECKPOINT_DIR",
            "s3://zomato-data-platform-dev-checkpoints/flink/pipeline2",
        ),
    )
    write_flink_job_config(config, "/tmp/flink_cdc_job_config.json")
