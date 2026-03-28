-- ============================================================================
-- Zomato Data Platform - Gold Layer: Delivery Performance Metrics
-- Format: Iceberg | Partitioned by: metric_date
-- Pre-computed delivery KPIs for logistics ops dashboards and rider management
-- Refreshed: Every 15 minutes via Spark streaming micro-batch
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.delivery_metrics (
    metric_date                 DATE            NOT NULL,
    metric_hour                 INTEGER,        -- 0-23, null for daily rollup
    city_id                     INTEGER         NOT NULL,
    city_name                   VARCHAR         NOT NULL,
    zone_id                     INTEGER,
    zone_name                   VARCHAR,

    -- Volume
    total_deliveries            BIGINT,
    completed_deliveries        BIGINT,
    failed_deliveries           BIGINT,
    reattempted_deliveries      BIGINT,
    completion_rate             DOUBLE,

    -- Delivery time breakdown (minutes)
    avg_total_delivery_time     DOUBLE,         -- order placed to delivered
    median_total_delivery_time  DOUBLE,
    p90_total_delivery_time     DOUBLE,
    p99_total_delivery_time     DOUBLE,
    avg_accept_time             DOUBLE,         -- order placed to restaurant accept
    avg_preparation_time        DOUBLE,         -- restaurant accept to food ready
    avg_pickup_time             DOUBLE,         -- food ready to rider picks up
    avg_transit_time            DOUBLE,         -- rider pickup to delivery
    avg_last_mile_time          DOUBLE,         -- reached location to delivered

    -- SLA
    sla_target_min              INTEGER,        -- configured SLA target
    sla_met_count               BIGINT,
    sla_breach_count            BIGINT,
    sla_breach_rate             DOUBLE,
    sla_breach_by_restaurant    BIGINT,         -- breach attributed to restaurant delay
    sla_breach_by_rider         BIGINT,         -- breach attributed to rider delay
    sla_breach_by_demand        BIGINT,         -- breach attributed to high demand

    -- Distance
    avg_delivery_distance_km    DOUBLE,
    total_distance_covered_km   DOUBLE,
    avg_distance_per_rider_km   DOUBLE,

    -- Rider supply
    total_active_riders         BIGINT,
    riders_on_delivery          BIGINT,
    riders_idle                 BIGINT,
    rider_utilization_rate      DOUBLE,         -- on_delivery / active
    avg_deliveries_per_rider    DOUBLE,
    avg_rider_earnings          DECIMAL(10,2),
    rider_login_hours           DOUBLE,
    riders_with_multi_order     BIGINT,         -- batched deliveries

    -- Order batching
    batched_orders              BIGINT,
    batch_rate                  DOUBLE,
    avg_batch_size              DOUBLE,
    batched_sla_breach_rate     DOUBLE,
    non_batched_sla_breach_rate DOUBLE,

    -- Cancellations during delivery
    cancel_after_accept         BIGINT,
    cancel_after_pickup         BIGINT,
    rider_unassign_count        BIGINT,
    avg_reassignment_count      DOUBLE,

    -- Customer experience
    avg_delivery_rating         DOUBLE,
    deliveries_with_rating      BIGINT,
    contactless_delivery_count  BIGINT,
    wrong_delivery_count        BIGINT,
    damaged_delivery_count      BIGINT,
    missing_item_count          BIGINT,

    -- Weather and external factors
    rain_affected_deliveries    BIGINT,
    surge_pricing_active        BOOLEAN,
    surge_multiplier_avg        DOUBLE,

    -- Cost
    total_delivery_cost         DECIMAL(16,2),
    avg_cost_per_delivery       DECIMAL(10,2),
    total_rider_incentives      DECIMAL(16,2),
    total_surge_payout          DECIMAL(16,2),

    -- Metadata
    last_refreshed_at           TIMESTAMP(6),
    pipeline_run_id             VARCHAR
)
WITH (
    format           = 'PARQUET',
    location         = 's3a://zomato-data-lake-prod/gold/delivery_metrics/',
    partitioning     = ARRAY['metric_date'],
    sorted_by        = ARRAY['city_id', 'metric_hour'],
    format_version   = 2
);
