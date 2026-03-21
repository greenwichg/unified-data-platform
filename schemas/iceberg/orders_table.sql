-- =============================================================================
-- Iceberg Table DDL: Orders
-- Catalog: iceberg | Schema: zomato
--
-- Stores all order data from Pipeline 1 (batch ETL), Pipeline 2 (CDC),
-- and Pipeline 3 (DynamoDB streams). Supports upserts via Iceberg v2
-- merge-on-read.
--
-- Partitioned by date (dt) for efficient time-range queries.
-- Written in ORC format with Snappy compression.
-- =============================================================================

CREATE TABLE IF NOT EXISTS iceberg.zomato.orders (
    order_id        VARCHAR     NOT NULL,
    user_id         VARCHAR     NOT NULL,
    restaurant_id   VARCHAR     NOT NULL,
    status          VARCHAR     NOT NULL,
    items           ARRAY(ROW(
                        item_id         VARCHAR,
                        name            VARCHAR,
                        quantity        INTEGER,
                        unit_price      DECIMAL(10, 2),
                        customizations  ARRAY(VARCHAR)
                    )),
    subtotal        DECIMAL(10, 2),
    tax             DECIMAL(10, 2),
    delivery_fee    DECIMAL(10, 2),
    total_amount    DECIMAL(10, 2)  NOT NULL,
    payment_method  VARCHAR,
    delivery_address ROW(
                        latitude        DOUBLE,
                        longitude       DOUBLE,
                        address_line    VARCHAR,
                        city            VARCHAR,
                        pincode         VARCHAR
                    ),
    city            VARCHAR,
    latitude        DOUBLE,
    longitude       DOUBLE,
    created_at      TIMESTAMP(6),
    updated_at      TIMESTAMP(6),
    cdc_operation   VARCHAR         COMMENT 'INSERT, UPDATE, DELETE',
    source_pipeline VARCHAR         COMMENT 'pipeline1, pipeline2, pipeline3, pipeline4',
    processing_time TIMESTAMP(6)    COMMENT 'When the record was processed by the pipeline',
    dt              DATE            NOT NULL COMMENT 'Partition column: order date'
)
WITH (
    format = 'ORC',
    format_version = 2,
    location = 's3://zomato-data-platform-raw-data-lake/iceberg/zomato/orders',
    partitioning = ARRAY['dt'],
    orc_compression = 'SNAPPY',
    write_data_strength = 'full'
);
