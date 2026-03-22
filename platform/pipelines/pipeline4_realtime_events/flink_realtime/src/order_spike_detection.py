"""
Pipeline 4 - Order Spike Detection (Complex Event Processing).

Flink SQL views that detect order velocity spikes per city using 5-minute
tumbling windows. Spike alerts are written back to a Kafka topic for
downstream consumption by the alerting system.

Alert levels:
  - NORMAL: order_count <= 100
  - WARNING: order_count > 500
  - CRITICAL: order_count > 1000
"""

# ---------------------------------------------------------------------------
# View: city order velocity (5-minute tumbling windows)
# ---------------------------------------------------------------------------

ORDER_VELOCITY_VIEW_SQL = """
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
"""

# ---------------------------------------------------------------------------
# View: spike detection (filters high-velocity windows)
# ---------------------------------------------------------------------------

SPIKE_DETECTION_VIEW_SQL = """
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
"""

# ---------------------------------------------------------------------------
# Spike alerts Kafka sink DDL
# ---------------------------------------------------------------------------

ALERTS_SINK_DDL = """
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
"""

# ---------------------------------------------------------------------------
# Insert spike alerts to Kafka
# ---------------------------------------------------------------------------

INSERT_ALERTS_SQL = """
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
"""
