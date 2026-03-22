-- ============================================================================
-- Zomato Data Platform - Gold Layer: Restaurant Performance KPIs
-- Format: Iceberg | Partitioned by: metric_date
-- Restaurant-level daily performance metrics for ops dashboards and
-- restaurant partner analytics portal
-- Refreshed: Hourly via Spark scheduled job
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.restaurant_performance (
    metric_date                 DATE            NOT NULL,
    restaurant_id               VARCHAR         NOT NULL,
    restaurant_name             VARCHAR,
    city_id                     INTEGER         NOT NULL,
    city_name                   VARCHAR,
    zone_id                     INTEGER,
    zone_name                   VARCHAR,
    cuisine_primary             VARCHAR,
    is_pro_partner              BOOLEAN,
    is_promoted                 BOOLEAN,

    -- Order volume
    total_orders                BIGINT,
    completed_orders            BIGINT,
    cancelled_orders            BIGINT,
    rejected_orders             BIGINT,
    completion_rate             DOUBLE,
    rejection_rate              DOUBLE,

    -- Revenue
    gross_merchandise_value     DECIMAL(14,2),
    net_revenue_to_restaurant   DECIMAL(14,2),
    platform_commission_paid    DECIMAL(14,2),
    avg_order_value             DECIMAL(10,2),
    total_discount_funded_rest  DECIMAL(14,2),  -- restaurant-funded discounts
    total_discount_funded_plat  DECIMAL(14,2),  -- platform-funded discounts

    -- Menu performance
    total_items_sold            BIGINT,
    unique_items_sold           INTEGER,
    top_item_id                 VARCHAR,
    top_item_name               VARCHAR,
    top_item_quantity           BIGINT,
    menu_utilization_rate       DOUBLE,         -- items_sold / total_menu_items
    avg_item_price              DECIMAL(10,2),
    veg_order_ratio             DOUBLE,

    -- Preparation and delivery
    avg_preparation_time_min    DOUBLE,
    median_preparation_time_min DOUBLE,
    p90_preparation_time_min    DOUBLE,
    avg_delivery_time_min       DOUBLE,
    avg_accept_time_sec         DOUBLE,         -- time to accept order
    sla_breach_count            BIGINT,
    sla_breach_rate             DOUBLE,

    -- Customer metrics
    unique_customers            BIGINT,
    new_customers               BIGINT,
    repeat_customers            BIGINT,
    repeat_customer_rate        DOUBLE,
    customer_retention_7d       DOUBLE,
    customer_retention_30d      DOUBLE,

    -- Ratings and reviews
    avg_order_rating            DOUBLE,
    total_ratings               BIGINT,
    five_star_count             BIGINT,
    one_star_count              BIGINT,
    avg_food_rating             DOUBLE,
    avg_packaging_rating        DOUBLE,
    negative_review_count       BIGINT,

    -- Cancellation reasons
    cancel_reason_out_of_stock  BIGINT,
    cancel_reason_too_busy      BIGINT,
    cancel_reason_closing_soon  BIGINT,
    cancel_reason_other         BIGINT,

    -- Availability
    online_hours                DOUBLE,         -- hours restaurant was online
    offline_hours               DOUBLE,
    availability_rate           DOUBLE,         -- online_hours / scheduled_hours
    menu_items_unavailable      INTEGER,

    -- Ranking
    city_rank_by_orders         INTEGER,
    city_rank_by_gmv            INTEGER,
    city_rank_by_rating         INTEGER,
    zone_rank_by_orders         INTEGER,

    -- Metadata
    last_refreshed_at           TIMESTAMP(6),
    pipeline_run_id             VARCHAR
)
WITH (
    format           = 'PARQUET',
    location         = 's3a://zomato-data-lake-prod/gold/restaurant_performance/',
    partitioning     = ARRAY['metric_date'],
    sorted_by        = ARRAY['city_id', 'restaurant_id'],
    format_version   = 2
);
