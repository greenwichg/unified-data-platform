-- ============================================================================
-- Zomato Data Platform - Raw Layer: Menu (Athena / Glue Data Catalog)
-- Format: ORC | Partitioned by: dt (ingestion date)
-- Source: Kafka topic 'menu' via Flink sink (CDC from Aurora PostgreSQL)
-- NOTE: Schema references use Glue Data Catalog (no catalog prefix needed).
-- ============================================================================

CREATE TABLE IF NOT EXISTS zomato_raw.menu_raw (
    menu_item_id        STRING,
    restaurant_id       STRING,
    restaurant_name     STRING,
    category_id         STRING,
    category_name       STRING,        -- 'Starters', 'Main Course', 'Beverages', 'Desserts'
    item_name           STRING,
    item_description    STRING,
    item_slug           STRING,
    is_veg              BOOLEAN,
    is_egg              BOOLEAN,
    is_bestseller       BOOLEAN,
    is_available        BOOLEAN,
    base_price          DOUBLE,
    selling_price       DOUBLE,
    currency            STRING,
    tax_percentage      DOUBLE,
    packaging_charge    DOUBLE,
    preparation_time_minutes INT,
    serves              INT,
    calories            INT,
    spice_level         STRING,        -- 'mild', 'medium', 'hot', 'extra_hot'
    cuisine_type        STRING,        -- 'North Indian', 'South Indian', 'Chinese', 'Italian', etc.
    tags                ARRAY<STRING>, -- ['gluten-free', 'jain', 'keto', 'sugar-free']
    allergens           ARRAY<STRING>,
    image_url           STRING,
    customizations      ARRAY<STRUCT<
        group_name:     STRING,
        group_id:       STRING,
        min_selection:  INT,
        max_selection:  INT,
        options:        ARRAY<STRUCT<
            option_id:  STRING,
            option_name:STRING,
            price_delta:DOUBLE,
            is_default: BOOLEAN
        >>
    >>,
    rating_avg          DOUBLE,
    rating_count        INT,
    order_count_30d     BIGINT,
    sort_order          INT,
    cdc_operation       STRING,        -- 'INSERT', 'UPDATE', 'DELETE'
    cdc_timestamp       TIMESTAMP,
    source_updated_at   TIMESTAMP,
    kafka_offset        BIGINT,
    kafka_partition     INT,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP
)
WITH (
    table_type   = 'ICEBERG',
    format       = 'ORC',
    location     = 's3://zomato-data-lake-prod/raw/menu/',
    is_external  = false
);
