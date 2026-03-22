"""
Pipeline 4 - Druid Sink.

Flink SQL DDL and INSERT statements for writing real-time events to a
secondary Kafka topic consumed by Apache Druid for real-time OLAP queries.

Also provides the Druid ingestion spec generator for supervisor creation.
"""

# ---------------------------------------------------------------------------
# Druid sink table DDL (writes to secondary Kafka cluster)
# ---------------------------------------------------------------------------

DRUID_SINK_DDL = """
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
"""

# ---------------------------------------------------------------------------
# Insert from app_events source to Druid sink
# ---------------------------------------------------------------------------

INSERT_DRUID_SQL = """
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
"""


# ---------------------------------------------------------------------------
# Druid ingestion spec generator
# ---------------------------------------------------------------------------

def generate_druid_ingestion_spec(
    kafka_bootstrap: str,
    datasource: str = "zomato_realtime_events",
) -> dict:
    """Generate Apache Druid Kafka ingestion supervisor spec."""
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
