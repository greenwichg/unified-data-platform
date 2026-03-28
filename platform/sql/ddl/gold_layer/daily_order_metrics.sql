-- ============================================================================
-- Zomato Data Platform - Gold Layer: Aggregation Tables
-- Format: Iceberg | Pre-computed metrics for executive dashboards and BI tools
-- Refreshed: Every 30 minutes via Spark scheduled job
-- ============================================================================


-- ============================================================================
-- 1. DAILY ORDER METRICS
-- Granularity: metric_date x city_id x zone_id
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


-- ============================================================================
-- Daily Materialization Query (executed by Spark scheduler)
-- ============================================================================
INSERT INTO gold.daily_order_metrics
SELECT
    o.order_date                                                AS metric_date,
    o.city_id,
    o.city_name,
    o.zone_id,
    o.zone_name,

    -- Volume metrics
    COUNT(*)                                                    AS total_orders,
    COUNT(*) FILTER (WHERE o.is_completed)                      AS completed_orders,
    COUNT(*) FILTER (WHERE o.is_cancelled)                      AS cancelled_orders,
    CAST(COUNT(*) FILTER (WHERE o.is_completed) AS DOUBLE)
        / NULLIF(COUNT(*), 0)                                   AS completion_rate,
    COUNT(*) FILTER (WHERE o.is_first_order)                    AS first_time_orders,
    COUNT(*) FILTER (WHERE NOT o.is_first_order)                AS repeat_orders,
    CAST(COUNT(*) FILTER (WHERE NOT o.is_first_order) AS DOUBLE)
        / NULLIF(COUNT(*), 0)                                   AS repeat_rate,

    -- GMV and revenue
    SUM(o.total_amount)                                         AS gross_merchandise_value,
    SUM(o.net_revenue)                                          AS net_revenue,
    SUM(o.discount_amount)                                      AS total_discount_given,
    SUM(o.delivery_fee)                                         AS total_delivery_fee,
    SUM(o.tax_amount)                                           AS total_tax_collected,
    SUM(o.platform_commission)                                  AS platform_commission,
    AVG(o.total_amount)                                         AS avg_order_value,
    APPROX_PERCENTILE(o.total_amount, 0.50)                     AS median_order_value,
    APPROX_PERCENTILE(o.total_amount, 0.90)                     AS p90_order_value,

    -- Order type breakdown
    COUNT(*) FILTER (WHERE o.order_type = 'delivery')           AS delivery_orders,
    COUNT(*) FILTER (WHERE o.order_type = 'pickup')             AS pickup_orders,
    COUNT(*) FILTER (WHERE o.order_type = 'dine_in')            AS dine_in_orders,

    -- Payment breakdown
    COUNT(*) FILTER (WHERE o.payment_method = 'upi')            AS upi_orders,
    COUNT(*) FILTER (WHERE o.payment_method IN ('credit_card', 'debit_card', 'card'))
                                                                AS card_orders,
    COUNT(*) FILTER (WHERE o.payment_method = 'cod')            AS cod_orders,
    COUNT(*) FILTER (WHERE o.payment_method = 'wallet')         AS wallet_orders,

    -- Delivery performance
    AVG(o.actual_delivery_min)                                  AS avg_delivery_time_min,
    APPROX_PERCENTILE(CAST(o.actual_delivery_min AS DOUBLE), 0.50)
                                                                AS median_delivery_time_min,
    APPROX_PERCENTILE(CAST(o.actual_delivery_min AS DOUBLE), 0.90)
                                                                AS p90_delivery_time_min,
    APPROX_PERCENTILE(CAST(o.actual_delivery_min AS DOUBLE), 0.99)
                                                                AS p99_delivery_time_min,
    COUNT(*) FILTER (WHERE o.sla_breach)                        AS sla_breach_count,
    CAST(COUNT(*) FILTER (WHERE o.sla_breach) AS DOUBLE)
        / NULLIF(COUNT(*) FILTER (WHERE o.is_completed), 0)     AS sla_breach_rate,
    AVG(
        DATE_DIFF('second', o.order_accepted_at, o.order_picked_up_at) / 60.0
    ) FILTER (WHERE o.order_picked_up_at IS NOT NULL)           AS avg_accept_to_pickup_min,
    AVG(
        DATE_DIFF('second', o.order_picked_up_at, o.order_delivered_at) / 60.0
    ) FILTER (WHERE o.order_delivered_at IS NOT NULL)           AS avg_pickup_to_deliver_min,

    -- Cancellation analysis
    COUNT(*) FILTER (WHERE o.cancellation_source = 'customer')      AS customer_cancellations,
    COUNT(*) FILTER (WHERE o.cancellation_source = 'restaurant')    AS restaurant_cancellations,
    COUNT(*) FILTER (WHERE o.cancellation_source = 'system')        AS system_cancellations,
    COUNT(*) FILTER (WHERE o.cancellation_source = 'rider')         AS rider_cancellations,

    -- Supply metrics
    COUNT(DISTINCT o.restaurant_id)
        FILTER (WHERE o.is_completed)                           AS active_restaurants,
    COUNT(DISTINCT o.rider_id)
        FILTER (WHERE o.rider_id IS NOT NULL)                   AS active_riders,
    CAST(COUNT(*) FILTER (WHERE o.is_completed) AS DOUBLE)
        / NULLIF(COUNT(DISTINCT o.rider_id)
                 FILTER (WHERE o.rider_id IS NOT NULL), 0)      AS orders_per_rider,

    -- Customer metrics
    COUNT(DISTINCT o.customer_id)                               AS unique_customers,
    COUNT(DISTINCT o.customer_id) FILTER (WHERE o.is_first_order)
                                                                AS new_customers,
    COUNT(DISTINCT o.customer_id) FILTER (WHERE NOT o.is_first_order)
                                                                AS returning_customers,
    COUNT(*) FILTER (WHERE o.is_pro_member)                     AS pro_member_orders,
    SUM(o.total_amount) FILTER (WHERE o.is_pro_member)          AS pro_member_gmv,

    -- Peak hour analysis
    COUNT(*) FILTER (WHERE o.is_peak_hour)                      AS peak_hour_orders,
    COUNT(*) FILTER (WHERE NOT o.is_peak_hour)                  AS off_peak_orders,
    o.is_weekend                                                AS weekend_flag,

    -- Quality
    AVG(CAST(o.order_rating AS DOUBLE))
        FILTER (WHERE o.order_rating IS NOT NULL)               AS avg_order_rating,
    COUNT(*) FILTER (WHERE o.order_rating IS NOT NULL)          AS orders_with_rating,
    COUNT(*) FILTER (WHERE o.order_rating <= 2)                 AS negative_feedback_count,

    -- Metadata
    CURRENT_TIMESTAMP                                           AS last_refreshed_at,
    'spark-daily-' || CAST(CURRENT_DATE AS VARCHAR)             AS pipeline_run_id

FROM curated.orders_curated o
WHERE o.order_date = CURRENT_DATE - INTERVAL '1' DAY
GROUP BY
    o.order_date,
    o.city_id,
    o.city_name,
    o.zone_id,
    o.zone_name,
    o.is_weekend
;


-- ============================================================================
-- 2. WEEKLY ORDER METRICS (rolled up from daily)
-- Granularity: week_start x city_id
-- ============================================================================
CREATE TABLE IF NOT EXISTS gold.weekly_order_metrics (
    week_start                  DATE            NOT NULL,
    week_end                    DATE            NOT NULL,
    iso_week                    INTEGER,
    iso_year                    INTEGER,
    city_id                     INTEGER         NOT NULL,
    city_name                   VARCHAR         NOT NULL,

    -- Volume
    total_orders                BIGINT,
    completed_orders            BIGINT,
    cancelled_orders            BIGINT,
    completion_rate             DOUBLE,
    wow_order_growth_pct        DOUBLE,         -- week-over-week growth

    -- GMV
    gross_merchandise_value     DECIMAL(16,2),
    net_revenue                 DECIMAL(16,2),
    total_discount_given        DECIMAL(16,2),
    avg_order_value             DECIMAL(10,2),
    wow_gmv_growth_pct          DOUBLE,

    -- Delivery
    avg_delivery_time_min       DOUBLE,
    p90_delivery_time_min       DOUBLE,
    sla_breach_rate             DOUBLE,

    -- Customer
    unique_customers            BIGINT,
    new_customers               BIGINT,
    returning_customers         BIGINT,
    customer_retention_rate     DOUBLE,

    -- Supply
    active_restaurants          BIGINT,
    active_riders               BIGINT,

    -- Metadata
    last_refreshed_at           TIMESTAMP(6),
    pipeline_run_id             VARCHAR
)
WITH (
    format           = 'PARQUET',
    location         = 's3a://zomato-data-lake-prod/gold/weekly_order_metrics/',
    partitioning     = ARRAY['week_start'],
    sorted_by        = ARRAY['city_id'],
    format_version   = 2
);

-- Weekly rollup materialization
INSERT INTO gold.weekly_order_metrics
WITH current_week AS (
    SELECT
        DATE_TRUNC('week', metric_date)             AS week_start,
        DATE_TRUNC('week', metric_date) + INTERVAL '6' DAY AS week_end,
        WEEK(metric_date)                           AS iso_week,
        YEAR(metric_date)                           AS iso_year,
        city_id,
        city_name,
        SUM(total_orders)                           AS total_orders,
        SUM(completed_orders)                       AS completed_orders,
        SUM(cancelled_orders)                       AS cancelled_orders,
        CAST(SUM(completed_orders) AS DOUBLE)
            / NULLIF(SUM(total_orders), 0)          AS completion_rate,
        SUM(gross_merchandise_value)                AS gross_merchandise_value,
        SUM(net_revenue)                            AS net_revenue,
        SUM(total_discount_given)                   AS total_discount_given,
        SUM(gross_merchandise_value)
            / NULLIF(SUM(total_orders), 0)          AS avg_order_value,
        AVG(avg_delivery_time_min)                  AS avg_delivery_time_min,
        MAX(p90_delivery_time_min)                  AS p90_delivery_time_min,
        CAST(SUM(sla_breach_count) AS DOUBLE)
            / NULLIF(SUM(completed_orders), 0)      AS sla_breach_rate,
        SUM(unique_customers)                       AS unique_customers,
        SUM(new_customers)                          AS new_customers,
        SUM(returning_customers)                    AS returning_customers,
        CAST(SUM(returning_customers) AS DOUBLE)
            / NULLIF(SUM(unique_customers), 0)      AS customer_retention_rate,
        MAX(active_restaurants)                     AS active_restaurants,
        MAX(active_riders)                          AS active_riders
    FROM gold.daily_order_metrics
    WHERE metric_date >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '7' DAY)
      AND metric_date <  DATE_TRUNC('week', CURRENT_DATE)
    GROUP BY
        DATE_TRUNC('week', metric_date),
        WEEK(metric_date),
        YEAR(metric_date),
        city_id,
        city_name
),
previous_week AS (
    SELECT
        city_id,
        SUM(total_orders)               AS prev_total_orders,
        SUM(gross_merchandise_value)    AS prev_gmv
    FROM gold.daily_order_metrics
    WHERE metric_date >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '14' DAY)
      AND metric_date <  DATE_TRUNC('week', CURRENT_DATE - INTERVAL '7' DAY)
    GROUP BY city_id
)
SELECT
    cw.week_start,
    cw.week_end,
    cw.iso_week,
    cw.iso_year,
    cw.city_id,
    cw.city_name,
    cw.total_orders,
    cw.completed_orders,
    cw.cancelled_orders,
    cw.completion_rate,
    (CAST(cw.total_orders AS DOUBLE) - pw.prev_total_orders)
        / NULLIF(pw.prev_total_orders, 0) * 100    AS wow_order_growth_pct,
    cw.gross_merchandise_value,
    cw.net_revenue,
    cw.total_discount_given,
    cw.avg_order_value,
    (CAST(cw.gross_merchandise_value AS DOUBLE) - CAST(pw.prev_gmv AS DOUBLE))
        / NULLIF(CAST(pw.prev_gmv AS DOUBLE), 0) * 100 AS wow_gmv_growth_pct,
    cw.avg_delivery_time_min,
    cw.p90_delivery_time_min,
    cw.sla_breach_rate,
    cw.unique_customers,
    cw.new_customers,
    cw.returning_customers,
    cw.customer_retention_rate,
    cw.active_restaurants,
    cw.active_riders,
    CURRENT_TIMESTAMP                               AS last_refreshed_at,
    'spark-weekly-' || CAST(CURRENT_DATE AS VARCHAR) AS pipeline_run_id
FROM current_week cw
LEFT JOIN previous_week pw ON cw.city_id = pw.city_id
;


-- ============================================================================
-- 3. MONTHLY ORDER METRICS (rolled up from daily)
-- Granularity: month_start x city_id
-- ============================================================================
CREATE TABLE IF NOT EXISTS gold.monthly_order_metrics (
    month_start                 DATE            NOT NULL,
    month_end                   DATE            NOT NULL,
    year_month                  VARCHAR,        -- '2026-03'
    city_id                     INTEGER         NOT NULL,
    city_name                   VARCHAR         NOT NULL,

    -- Volume
    total_orders                BIGINT,
    completed_orders            BIGINT,
    cancelled_orders            BIGINT,
    completion_rate             DOUBLE,
    mom_order_growth_pct        DOUBLE,         -- month-over-month growth
    avg_daily_orders            DOUBLE,

    -- GMV and revenue
    gross_merchandise_value     DECIMAL(16,2),
    net_revenue                 DECIMAL(16,2),
    total_discount_given        DECIMAL(16,2),
    total_delivery_fee          DECIMAL(16,2),
    platform_commission         DECIMAL(16,2),
    avg_order_value             DECIMAL(10,2),
    mom_gmv_growth_pct          DOUBLE,

    -- Delivery performance
    avg_delivery_time_min       DOUBLE,
    p90_delivery_time_min       DOUBLE,
    sla_breach_rate             DOUBLE,

    -- Customer cohorts
    unique_customers            BIGINT,
    new_customers               BIGINT,
    returning_customers         BIGINT,
    customer_retention_rate     DOUBLE,
    pro_member_orders           BIGINT,
    pro_member_share_pct        DOUBLE,

    -- Supply
    active_restaurants          BIGINT,
    active_riders               BIGINT,

    -- Metadata
    last_refreshed_at           TIMESTAMP(6),
    pipeline_run_id             VARCHAR
)
WITH (
    format           = 'PARQUET',
    location         = 's3a://zomato-data-lake-prod/gold/monthly_order_metrics/',
    partitioning     = ARRAY['month_start'],
    sorted_by        = ARRAY['city_id'],
    format_version   = 2
);

-- Monthly rollup materialization
INSERT INTO gold.monthly_order_metrics
WITH current_month AS (
    SELECT
        DATE_TRUNC('month', metric_date)            AS month_start,
        LAST_DAY_OF_MONTH(metric_date)              AS month_end,
        DATE_FORMAT(metric_date, '%Y-%m')           AS year_month,
        city_id,
        city_name,
        SUM(total_orders)                           AS total_orders,
        SUM(completed_orders)                       AS completed_orders,
        SUM(cancelled_orders)                       AS cancelled_orders,
        CAST(SUM(completed_orders) AS DOUBLE)
            / NULLIF(SUM(total_orders), 0)          AS completion_rate,
        AVG(CAST(total_orders AS DOUBLE))           AS avg_daily_orders,
        SUM(gross_merchandise_value)                AS gross_merchandise_value,
        SUM(net_revenue)                            AS net_revenue,
        SUM(total_discount_given)                   AS total_discount_given,
        SUM(total_delivery_fee)                     AS total_delivery_fee,
        SUM(platform_commission)                    AS platform_commission,
        SUM(gross_merchandise_value)
            / NULLIF(SUM(total_orders), 0)          AS avg_order_value,
        AVG(avg_delivery_time_min)                  AS avg_delivery_time_min,
        MAX(p90_delivery_time_min)                  AS p90_delivery_time_min,
        CAST(SUM(sla_breach_count) AS DOUBLE)
            / NULLIF(SUM(completed_orders), 0)      AS sla_breach_rate,
        SUM(unique_customers)                       AS unique_customers,
        SUM(new_customers)                          AS new_customers,
        SUM(returning_customers)                    AS returning_customers,
        CAST(SUM(returning_customers) AS DOUBLE)
            / NULLIF(SUM(unique_customers), 0)      AS customer_retention_rate,
        SUM(pro_member_orders)                      AS pro_member_orders,
        CAST(SUM(pro_member_orders) AS DOUBLE)
            / NULLIF(SUM(total_orders), 0) * 100    AS pro_member_share_pct,
        MAX(active_restaurants)                     AS active_restaurants,
        MAX(active_riders)                          AS active_riders
    FROM gold.daily_order_metrics
    WHERE metric_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH)
      AND metric_date <  DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY
        DATE_TRUNC('month', metric_date),
        LAST_DAY_OF_MONTH(metric_date),
        DATE_FORMAT(metric_date, '%Y-%m'),
        city_id,
        city_name
),
previous_month AS (
    SELECT
        city_id,
        SUM(total_orders)               AS prev_total_orders,
        SUM(gross_merchandise_value)    AS prev_gmv
    FROM gold.daily_order_metrics
    WHERE metric_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2' MONTH)
      AND metric_date <  DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH)
    GROUP BY city_id
)
SELECT
    cm.month_start,
    cm.month_end,
    cm.year_month,
    cm.city_id,
    cm.city_name,
    cm.total_orders,
    cm.completed_orders,
    cm.cancelled_orders,
    cm.completion_rate,
    (CAST(cm.total_orders AS DOUBLE) - pm.prev_total_orders)
        / NULLIF(pm.prev_total_orders, 0) * 100    AS mom_order_growth_pct,
    cm.avg_daily_orders,
    cm.gross_merchandise_value,
    cm.net_revenue,
    cm.total_discount_given,
    cm.total_delivery_fee,
    cm.platform_commission,
    cm.avg_order_value,
    (CAST(cm.gross_merchandise_value AS DOUBLE) - CAST(pm.prev_gmv AS DOUBLE))
        / NULLIF(CAST(pm.prev_gmv AS DOUBLE), 0) * 100 AS mom_gmv_growth_pct,
    cm.avg_delivery_time_min,
    cm.p90_delivery_time_min,
    cm.sla_breach_rate,
    cm.unique_customers,
    cm.new_customers,
    cm.returning_customers,
    cm.customer_retention_rate,
    cm.pro_member_orders,
    cm.pro_member_share_pct,
    cm.active_restaurants,
    cm.active_riders,
    CURRENT_TIMESTAMP                               AS last_refreshed_at,
    'spark-monthly-' || CAST(CURRENT_DATE AS VARCHAR) AS pipeline_run_id
FROM current_month cm
LEFT JOIN previous_month pm ON cm.city_id = pm.city_id
;


-- ============================================================================
-- Table Maintenance
-- ============================================================================
-- OPTIMIZE gold tables weekly (they are append-heavy)
-- ALTER TABLE gold.daily_order_metrics EXECUTE optimize
--     WHERE metric_date >= CURRENT_DATE - INTERVAL '30' DAY;
-- ALTER TABLE gold.weekly_order_metrics EXECUTE optimize;
-- ALTER TABLE gold.monthly_order_metrics EXECUTE optimize;

-- EXPIRE SNAPSHOTS: retain 30 days for gold tables
-- ALTER TABLE gold.daily_order_metrics EXECUTE expire_snapshots(retention_threshold => '30d');
-- ALTER TABLE gold.weekly_order_metrics EXECUTE expire_snapshots(retention_threshold => '30d');
-- ALTER TABLE gold.monthly_order_metrics EXECUTE expire_snapshots(retention_threshold => '30d');
