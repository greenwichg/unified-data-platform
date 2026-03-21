-- ============================================================================
-- Zomato Data Platform - Gold Layer: Daily Order Metrics
-- Format: Iceberg | Partitioned by: metric_date
-- Pre-computed daily order KPIs consumed by executive dashboards and BI tools
-- Refreshed: Every 30 minutes via Spark scheduled job
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.daily_order_metrics (
    metric_date                 DATE            NOT NULL,
    city_id                     INTEGER         NOT NULL,
    city_name                   VARCHAR         NOT NULL,
    zone_id                     INTEGER,
    zone_name                   VARCHAR,

    -- Volume metrics
    total_orders                BIGINT,
    completed_orders            BIGINT,
    cancelled_orders            BIGINT,
    completion_rate             DOUBLE,         -- completed / total
    first_time_orders           BIGINT,
    repeat_orders               BIGINT,
    repeat_rate                 DOUBLE,

    -- GMV and revenue
    gross_merchandise_value     DECIMAL(16,2),
    net_revenue                 DECIMAL(16,2),
    total_discount_given        DECIMAL(16,2),
    total_delivery_fee          DECIMAL(16,2),
    total_tax_collected         DECIMAL(16,2),
    platform_commission         DECIMAL(16,2),
    avg_order_value             DECIMAL(10,2),
    median_order_value          DECIMAL(10,2),
    p90_order_value             DECIMAL(10,2),

    -- Order type breakdown
    delivery_orders             BIGINT,
    pickup_orders               BIGINT,
    dine_in_orders              BIGINT,

    -- Payment breakdown
    upi_orders                  BIGINT,
    card_orders                 BIGINT,
    cod_orders                  BIGINT,
    wallet_orders               BIGINT,

    -- Delivery performance
    avg_delivery_time_min       DOUBLE,
    median_delivery_time_min    DOUBLE,
    p90_delivery_time_min       DOUBLE,
    p99_delivery_time_min       DOUBLE,
    sla_breach_count            BIGINT,
    sla_breach_rate             DOUBLE,
    avg_accept_to_pickup_min    DOUBLE,
    avg_pickup_to_deliver_min   DOUBLE,

    -- Cancellation analysis
    customer_cancellations      BIGINT,
    restaurant_cancellations    BIGINT,
    system_cancellations        BIGINT,
    rider_cancellations         BIGINT,

    -- Supply metrics
    active_restaurants          BIGINT,
    active_riders               BIGINT,
    orders_per_rider            DOUBLE,

    -- Customer metrics
    unique_customers            BIGINT,
    new_customers               BIGINT,
    returning_customers         BIGINT,
    pro_member_orders           BIGINT,
    pro_member_gmv              DECIMAL(16,2),

    -- Peak hour analysis
    peak_hour_orders            BIGINT,         -- orders during 12-14, 19-22
    off_peak_orders             BIGINT,
    weekend_flag                BOOLEAN,

    -- Quality
    avg_order_rating            DOUBLE,
    orders_with_rating          BIGINT,
    negative_feedback_count     BIGINT,

    -- Metadata
    last_refreshed_at           TIMESTAMP(6),
    pipeline_run_id             VARCHAR
)
WITH (
    format           = 'PARQUET',
    location         = 's3a://zomato-data-lake-prod/gold/daily_order_metrics/',
    partitioning     = ARRAY['metric_date'],
    sorted_by        = ARRAY['city_id'],
    format_version   = 2
);

-- Materialization query (executed by Spark)
-- INSERT INTO gold.daily_order_metrics
-- SELECT
--     o.order_date                                    AS metric_date,
--     o.city_id,
--     o.city_name,
--     o.zone_id,
--     o.zone_name,
--     COUNT(*)                                        AS total_orders,
--     COUNT(*) FILTER (WHERE o.is_completed)          AS completed_orders,
--     COUNT(*) FILTER (WHERE o.is_cancelled)          AS cancelled_orders,
--     ...
-- FROM curated.orders_curated o
-- WHERE o.order_date = CURRENT_DATE - INTERVAL '1' DAY
-- GROUP BY o.order_date, o.city_id, o.city_name, o.zone_id, o.zone_name;
