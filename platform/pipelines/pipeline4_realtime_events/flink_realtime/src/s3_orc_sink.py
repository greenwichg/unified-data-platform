"""
Pipeline 4 - S3 ORC Sink.

Flink SQL DDL and INSERT statements for writing real-time events to S3
in ORC format, partitioned by date and hour.
"""

# ---------------------------------------------------------------------------
# S3 ORC sink table DDL
# ---------------------------------------------------------------------------

S3_SINK_DDL = """
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
"""

# ---------------------------------------------------------------------------
# Insert from app_events source to S3 ORC sink
# ---------------------------------------------------------------------------

INSERT_S3_SQL = """
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
"""
