-- ============================================================================
-- Zomato Data Platform - Raw Layer: Orders
-- Format: ORC | Partitioned by: dt (ingestion date)
-- Source: Kafka topic 'orders' via Flink sink
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.orders_raw (
    order_id            VARCHAR,
    customer_id         VARCHAR,
    restaurant_id       VARCHAR,
    city_id             INTEGER,
    city_name           VARCHAR,
    order_status        VARCHAR,
    order_type          VARCHAR,        -- 'delivery', 'pickup', 'dine_in'
    payment_method      VARCHAR,        -- 'upi', 'card', 'cod', 'wallet'
    payment_status      VARCHAR,
    coupon_code         VARCHAR,
    subtotal_amount     DOUBLE,
    tax_amount          DOUBLE,
    delivery_fee        DOUBLE,
    discount_amount     DOUBLE,
    total_amount        DOUBLE,
    currency            VARCHAR,
    item_count          INTEGER,
    items               ARRAY(ROW(
        item_id         VARCHAR,
        item_name       VARCHAR,
        quantity        INTEGER,
        unit_price      DOUBLE,
        category        VARCHAR,
        is_veg          BOOLEAN
    )),
    delivery_address    ROW(
        lat             DOUBLE,
        lng             DOUBLE,
        pincode         VARCHAR,
        locality        VARCHAR
    ),
    rider_id            VARCHAR,
    restaurant_lat      DOUBLE,
    restaurant_lng      DOUBLE,
    estimated_delivery_minutes  INTEGER,
    actual_delivery_minutes     INTEGER,
    order_placed_at     TIMESTAMP,
    order_accepted_at   TIMESTAMP,
    order_prepared_at   TIMESTAMP,
    order_picked_up_at  TIMESTAMP,
    order_delivered_at  TIMESTAMP,
    order_cancelled_at  TIMESTAMP,
    cancellation_reason VARCHAR,
    platform            VARCHAR,        -- 'android', 'ios', 'web'
    app_version         VARCHAR,
    device_id           VARCHAR,
    session_id          VARCHAR,
    kafka_offset        BIGINT,
    kafka_partition     INTEGER,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP
)
WITH (
    format           = 'ORC',
    external_location = 's3a://zomato-data-lake-prod/raw/orders/',
    partitioned_by   = ARRAY['dt'],
    orc_compression  = 'ZSTD'
);

-- Repair partitions after new data lands
-- CALL system.sync_partition_metadata('raw', 'orders_raw', 'FULL');

-- Example partition pruning query:
-- SELECT * FROM raw.orders_raw WHERE dt = '2026-03-21';
