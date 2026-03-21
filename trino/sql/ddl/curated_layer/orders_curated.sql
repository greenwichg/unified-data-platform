-- ============================================================================
-- Zomato Data Platform - Curated Layer: Orders
-- Format: Iceberg (Apache Iceberg v2) | Partitioned by: order_date, city_id
-- Deduped, cleaned, enriched orders from raw layer
-- ============================================================================

CREATE TABLE IF NOT EXISTS curated.orders_curated (
    order_id                VARCHAR         NOT NULL,
    customer_id             VARCHAR         NOT NULL,
    restaurant_id           VARCHAR         NOT NULL,
    city_id                 INTEGER         NOT NULL,
    city_name               VARCHAR,
    zone_id                 INTEGER,
    zone_name               VARCHAR,
    order_status            VARCHAR,
    is_completed            BOOLEAN,
    is_cancelled            BOOLEAN,
    order_type              VARCHAR,
    payment_method          VARCHAR,
    payment_status          VARCHAR,
    coupon_code             VARCHAR,
    coupon_type             VARCHAR,        -- 'flat', 'percentage', 'freebie', 'bogo'
    subtotal_amount         DECIMAL(12,2),
    tax_amount              DECIMAL(12,2),
    delivery_fee            DECIMAL(12,2),
    discount_amount         DECIMAL(12,2),
    platform_commission     DECIMAL(12,2),
    restaurant_payout       DECIMAL(12,2),
    total_amount            DECIMAL(12,2),
    net_revenue             DECIMAL(12,2),  -- total - discount - restaurant_payout
    currency                VARCHAR,
    item_count              INTEGER,
    unique_item_count       INTEGER,
    has_veg_items           BOOLEAN,
    has_non_veg_items       BOOLEAN,
    cuisine_primary         VARCHAR,
    rider_id                VARCHAR,
    delivery_distance_km    DOUBLE,
    estimated_delivery_min  INTEGER,
    actual_delivery_min     INTEGER,
    delivery_delay_min      INTEGER,        -- actual - estimated
    is_late_delivery        BOOLEAN,
    sla_breach              BOOLEAN,
    order_placed_at         TIMESTAMP(6),
    order_accepted_at       TIMESTAMP(6),
    food_ready_at           TIMESTAMP(6),
    order_picked_up_at      TIMESTAMP(6),
    order_delivered_at      TIMESTAMP(6),
    order_cancelled_at      TIMESTAMP(6),
    cancellation_reason     VARCHAR,
    cancellation_source     VARCHAR,        -- 'customer', 'restaurant', 'system', 'rider'
    accept_to_deliver_min   DOUBLE,         -- end-to-end fulfillment time
    platform                VARCHAR,
    app_version             VARCHAR,
    customer_lat            DOUBLE,
    customer_lng            DOUBLE,
    restaurant_lat          DOUBLE,
    restaurant_lng          DOUBLE,
    is_first_order          BOOLEAN,
    is_pro_member           BOOLEAN,
    customer_lifetime_orders INTEGER,
    restaurant_rating       DOUBLE,
    order_rating            INTEGER,
    order_feedback          VARCHAR,
    order_date              DATE            NOT NULL,
    order_hour              INTEGER,
    is_weekend              BOOLEAN,
    is_peak_hour            BOOLEAN,        -- 12-14, 19-22
    processed_at            TIMESTAMP(6)
)
WITH (
    format           = 'PARQUET',
    location         = 's3a://zomato-data-lake-prod/curated/orders/',
    partitioning     = ARRAY['order_date', 'city_id'],
    sorted_by        = ARRAY['order_placed_at'],
    format_version   = 2
);

-- Iceberg table maintenance procedures
-- OPTIMIZE: compact small files produced by streaming ingestion
-- ALTER TABLE curated.orders_curated EXECUTE optimize
--     WHERE order_date >= CURRENT_DATE - INTERVAL '3' DAY;

-- EXPIRE SNAPSHOTS: clean up old metadata (retain 7 days)
-- ALTER TABLE curated.orders_curated EXECUTE expire_snapshots(retention_threshold => '7d');

-- REMOVE ORPHAN FILES
-- ALTER TABLE curated.orders_curated EXECUTE remove_orphan_files(retention_threshold => '7d');
