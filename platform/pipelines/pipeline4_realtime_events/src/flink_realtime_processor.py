"""
Pipeline 4 - Flink Real-time Processor: Kafka → Flink → S3 + Druid

Consumes events from Kafka topics, performs real-time aggregations
and complex event processing, then sinks to both S3 (ORC) and Druid
for real-time OLAP queries.

Druid handles: 20B events/week, 8M queries/week, millisecond response.
"""

import json
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("pipeline4_flink_realtime")


# Flink SQL definitions for the real-time pipeline
FLINK_SQL = {
    # Source: Kafka events from all app topics
    "create_kafka_events_source": """
        CREATE TABLE app_events (
            event_id STRING,
            event_type STRING,
            source STRING,
            user_id STRING,
            session_id STRING,
            `timestamp` TIMESTAMP(3),
            properties MAP<STRING, STRING>,
            device MAP<STRING, STRING>,
            location MAP<STRING, STRING>,
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'orders;users;menu;promo;topics',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'properties.group.id' = 'flink-realtime-events',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'AWS_MSK_IAM',
            'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
            'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'scan.startup.mode' = 'latest-offset'
        );
    """,
    # Sink: S3 in ORC format
    "create_s3_orc_sink": """
        CREATE TABLE s3_events_orc (
            event_id STRING,
            event_type STRING,
            source STRING,
            user_id STRING,
            session_id STRING,
            event_timestamp TIMESTAMP(3),
            city STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            platform STRING,
            order_id STRING,
            total_amount DOUBLE,
            dt STRING,
            `hour` INT
        ) PARTITIONED BY (dt, `hour`) WITH (
            'connector' = 'filesystem',
            'path' = 's3://{s3_bucket}/pipeline4-realtime/flink-output',
            'format' = 'orc',
            'sink.partition-commit.policy.kind' = 'success-file',
            'sink.rolling-policy.file-size' = '256MB',
            'sink.rolling-policy.rollover-interval' = '10min',
            'auto-compaction' = 'true'
        );
    """,
    # Sink: Druid for real-time OLAP
    "create_druid_sink": """
        CREATE TABLE druid_events (
            event_id STRING,
            event_type STRING,
            source STRING,
            user_id STRING,
            city STRING,
            order_id STRING,
            total_amount DOUBLE,
            event_count BIGINT,
            event_timestamp TIMESTAMP(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'druid-ingestion-events',
            'properties.bootstrap.servers' = '{kafka_bootstrap_2}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'AWS_MSK_IAM',
            'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
            'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
            'format' = 'json',
            'sink.partitioner' = 'round-robin'
        );
    """,
    # Insert into S3 ORC sink
    "insert_to_s3": """
        INSERT INTO s3_events_orc
        SELECT
            event_id,
            event_type,
            source,
            user_id,
            session_id,
            `timestamp` AS event_timestamp,
            location['city'] AS city,
            CAST(location['latitude'] AS DOUBLE) AS latitude,
            CAST(location['longitude'] AS DOUBLE) AS longitude,
            device['platform'] AS platform,
            properties['order_id'] AS order_id,
            CAST(properties['total_amount'] AS DOUBLE) AS total_amount,
            DATE_FORMAT(`timestamp`, 'yyyy-MM-dd') AS dt,
            HOUR(`timestamp`) AS `hour`
        FROM app_events;
    """,
    # Insert into Druid sink
    "insert_to_druid": """
        INSERT INTO druid_events
        SELECT
            event_id,
            event_type,
            source,
            user_id,
            location['city'] AS city,
            properties['order_id'] AS order_id,
            CAST(properties['total_amount'] AS DOUBLE) AS total_amount,
            1 AS event_count,
            `timestamp` AS event_timestamp
        FROM app_events;
    """,
    # Complex Event Processing: Order velocity per city (5-min windows)
    "create_order_velocity_view": """
        CREATE VIEW city_order_velocity AS
        SELECT
            location['city'] AS city,
            COUNT(*) AS order_count,
            SUM(CAST(properties['total_amount'] AS DOUBLE)) AS total_revenue,
            AVG(CAST(properties['total_amount'] AS DOUBLE)) AS avg_order_value,
            COUNT(DISTINCT user_id) AS unique_users,
            TUMBLE_START(`timestamp`, INTERVAL '5' MINUTE) AS window_start,
            TUMBLE_END(`timestamp`, INTERVAL '5' MINUTE) AS window_end
        FROM app_events
        WHERE event_type = 'ORDER_PLACED'
        GROUP BY
            location['city'],
            TUMBLE(`timestamp`, INTERVAL '5' MINUTE);
    """,
    # Complex Event Processing: Spike detection (feedback loop)
    "create_spike_detection": """
        CREATE VIEW order_spike_alerts AS
        SELECT
            city,
            order_count,
            total_revenue,
            avg_order_value,
            window_start,
            window_end,
            CASE
                WHEN order_count > 1000 THEN 'CRITICAL'
                WHEN order_count > 500  THEN 'WARNING'
                ELSE 'NORMAL'
            END AS alert_level
        FROM city_order_velocity
        WHERE order_count > 100;
    """,
    # Feedback loop: Write spike alerts back to Kafka
    "create_alerts_sink": """
        CREATE TABLE spike_alerts_kafka (
            city STRING,
            order_count BIGINT,
            total_revenue DOUBLE,
            avg_order_value DOUBLE,
            alert_level STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'order-spike-alerts',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'AWS_MSK_IAM',
            'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
            'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
            'format' = 'json'
        );
    """,
    "insert_alerts": """
        INSERT INTO spike_alerts_kafka
        SELECT
            city,
            order_count,
            total_revenue,
            avg_order_value,
            alert_level,
            window_start,
            window_end
        FROM order_spike_alerts;
    """,
}


def generate_druid_ingestion_spec(
    kafka_bootstrap: str,
    datasource: str = "zomato_realtime_events",
) -> dict:
    """Generate Apache Druid ingestion spec for real-time event data."""
    return {
        "type": "kafka",
        "spec": {
            "ioConfig": {
                "type": "kafka",
                "consumerProperties": {
                    "bootstrap.servers": kafka_bootstrap,
                    "group.id": "druid-realtime-ingestion",
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "AWS_MSK_IAM",
                    "sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
                    "sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
                },
                "topic": "druid-ingestion-events",
                "inputFormat": {
                    "type": "json",
                    "flattenSpec": {
                        "useFieldDiscovery": True,
                        "fields": [],
                    },
                },
                "useEarliestOffset": False,
            },
            "tuningConfig": {
                "type": "kafka",
                "maxRowsInMemory": 1000000,
                "maxBytesInMemory": 536870912,
                "maxRowsPerSegment": 5000000,
                "maxTotalRows": 20000000,
                "intermediatePersistPeriod": "PT10M",
                "taskCount": 8,
                "replicas": 2,
                "taskDuration": "PT1H",
                "completionTimeout": "PT30M",
            },
            "dataSchema": {
                "dataSource": datasource,
                "timestampSpec": {
                    "column": "event_timestamp",
                    "format": "iso",
                },
                "dimensionsSpec": {
                    "dimensions": [
                        "event_id",
                        "event_type",
                        "source",
                        "user_id",
                        "city",
                        "order_id",
                    ],
                },
                "metricsSpec": [
                    {"type": "count", "name": "count"},
                    {
                        "type": "doubleSum",
                        "name": "total_revenue",
                        "fieldName": "total_amount",
                    },
                    {
                        "type": "doubleMin",
                        "name": "min_order_value",
                        "fieldName": "total_amount",
                    },
                    {
                        "type": "doubleMax",
                        "name": "max_order_value",
                        "fieldName": "total_amount",
                    },
                    {
                        "type": "longSum",
                        "name": "event_count",
                        "fieldName": "event_count",
                    },
                    {
                        "type": "HLLSketchBuild",
                        "name": "unique_users",
                        "fieldName": "user_id",
                        "lgK": 12,
                    },
                ],
                "granularitySpec": {
                    "segmentGranularity": "HOUR",
                    "queryGranularity": "MINUTE",
                    "rollup": True,
                },
            },
        },
    }


def generate_flink_job_config(
    kafka_bootstrap: str,
    kafka_bootstrap_2: str,
    s3_bucket: str,
    checkpoint_dir: str,
) -> dict:
    """Generate the Flink job config for Pipeline 4."""
    return {
        "job_name": "zomato-realtime-events",
        "parallelism": 64,
        "checkpoint_interval_ms": 30000,
        "checkpoint_dir": checkpoint_dir,
        "state_backend": "rocksdb",
        "restart_strategy": {
            "type": "exponential-delay",
            "initial_backoff_ms": 1000,
            "max_backoff_ms": 60000,
            "backoff_multiplier": 2.0,
            "jitter_factor": 0.1,
        },
        "sql_statements": {
            key: stmt.format(
                kafka_bootstrap=kafka_bootstrap,
                kafka_bootstrap_2=kafka_bootstrap_2,
                s3_bucket=s3_bucket,
            )
            for key, stmt in FLINK_SQL.items()
        },
    }


if __name__ == "__main__":
    # Generate configs for deployment
    flink_config = generate_flink_job_config(
        kafka_bootstrap=os.environ.get("MSK_BOOTSTRAP", "b-1.zomato-msk.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-2.zomato-msk.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-3.zomato-msk.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098"),
        kafka_bootstrap_2=os.environ.get("MSK_BOOTSTRAP_2", "b-1.zomato-msk-rt.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-2.zomato-msk-rt.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-3.zomato-msk-rt.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098"),
        s3_bucket=os.environ.get("S3_BUCKET", "zomato-data-platform-dev-raw-data-lake"),
        checkpoint_dir=os.environ.get(
            "CHECKPOINT_DIR",
            "s3://zomato-data-platform-dev-checkpoints/flink/pipeline4",
        ),
    )

    os.makedirs("/tmp/pipeline4", exist_ok=True)
    with open("/tmp/pipeline4/flink_job_config.json", "w") as f:
        json.dump(flink_config, f, indent=2)

    druid_spec = generate_druid_ingestion_spec(
        kafka_bootstrap=os.environ.get("MSK_BOOTSTRAP_2", "b-1.zomato-msk-rt.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-2.zomato-msk-rt.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098,b-3.zomato-msk-rt.xxxxx.c2.kafka.ap-south-1.amazonaws.com:9098"),
    )
    with open("/tmp/pipeline4/druid_ingestion_spec.json", "w") as f:
        json.dump(druid_spec, f, indent=2)

    logger.info("Pipeline 4 configs generated in /tmp/pipeline4/")
