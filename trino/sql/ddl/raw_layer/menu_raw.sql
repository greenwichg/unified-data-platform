-- ============================================================================
-- Zomato Data Platform - Raw Layer: Menu
-- Format: ORC | Partitioned by: dt (ingestion date)
-- Source: Kafka topic 'menu' via Flink sink (CDC from Aurora PostgreSQL)
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.menu_raw (
    menu_item_id        VARCHAR,
    restaurant_id       VARCHAR,
    restaurant_name     VARCHAR,
    category_id         VARCHAR,
    category_name       VARCHAR,        -- 'Starters', 'Main Course', 'Beverages', 'Desserts'
    item_name           VARCHAR,
    item_description    VARCHAR,
    item_slug           VARCHAR,
    is_veg              BOOLEAN,
    is_egg              BOOLEAN,
    is_bestseller       BOOLEAN,
    is_available        BOOLEAN,
    base_price          DOUBLE,
    selling_price       DOUBLE,
    currency            VARCHAR,
    tax_percentage      DOUBLE,
    packaging_charge    DOUBLE,
    preparation_time_minutes INTEGER,
    serves              INTEGER,
    calories            INTEGER,
    spice_level         VARCHAR,        -- 'mild', 'medium', 'hot', 'extra_hot'
    cuisine_type        VARCHAR,        -- 'North Indian', 'South Indian', 'Chinese', 'Italian', etc.
    tags                ARRAY(VARCHAR), -- ['gluten-free', 'jain', 'keto', 'sugar-free']
    allergens           ARRAY(VARCHAR),
    image_url           VARCHAR,
    customizations      ARRAY(ROW(
        group_name      VARCHAR,
        group_id        VARCHAR,
        min_selection   INTEGER,
        max_selection   INTEGER,
        options         ARRAY(ROW(
            option_id   VARCHAR,
            option_name VARCHAR,
            price_delta DOUBLE,
            is_default  BOOLEAN
        ))
    )),
    rating_avg          DOUBLE,
    rating_count        INTEGER,
    order_count_30d     BIGINT,
    sort_order          INTEGER,
    cdc_operation       VARCHAR,        -- 'INSERT', 'UPDATE', 'DELETE'
    cdc_timestamp       TIMESTAMP,
    source_updated_at   TIMESTAMP,
    kafka_offset        BIGINT,
    kafka_partition     INTEGER,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP
)
WITH (
    format           = 'ORC',
    external_location = 's3a://zomato-data-lake-prod/raw/menu/',
    partitioned_by   = ARRAY['dt'],
    orc_compression  = 'ZSTD'
);
