-- ============================================================================
-- Zomato Data Platform - Gold Layer: Promotion Effectiveness Analysis
-- Format: Iceberg | Partitioned by: analysis_date
-- Pre-computed promo ROI, incrementality, and cohort impact metrics
-- Used by growth/marketing teams and the promo budget optimization engine
-- Refreshed: Daily via Spark batch job at 04:00 UTC
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.promo_effectiveness (
    analysis_date               DATE            NOT NULL,
    promo_id                    VARCHAR         NOT NULL,
    promo_code                  VARCHAR         NOT NULL,
    promo_name                  VARCHAR,
    city_id                     INTEGER         NOT NULL,
    city_name                   VARCHAR,

    -- Promo configuration snapshot
    discount_type               VARCHAR,        -- 'flat', 'percentage', 'freebie', 'bogo', 'cashback'
    discount_value              DECIMAL(10,2),
    max_discount_cap            DECIMAL(10,2),
    min_order_value             DECIMAL(10,2),
    funding_source              VARCHAR,        -- 'platform', 'restaurant', 'co_funded'
    target_segment              VARCHAR,
    promo_category              VARCHAR,        -- 'new_user', 'retention', 'winback', 'festive'

    -- Distribution
    total_impressions           BIGINT,         -- promo shown to users
    total_distributed           BIGINT,         -- promo codes sent/visible
    total_applied               BIGINT,         -- added to cart
    total_redeemed              BIGINT,         -- order completed with promo
    apply_rate                  DOUBLE,         -- applied / distributed
    redemption_rate             DOUBLE,         -- redeemed / distributed
    conversion_rate             DOUBLE,         -- redeemed / applied

    -- Financial impact
    total_gmv                   DECIMAL(16,2),  -- GMV of orders with this promo
    total_discount_cost         DECIMAL(16,2),  -- total discount given
    platform_discount_cost      DECIMAL(16,2),  -- platform-funded portion
    restaurant_discount_cost    DECIMAL(16,2),  -- restaurant-funded portion
    avg_order_value             DECIMAL(10,2),
    avg_discount_per_order      DECIMAL(10,2),
    discount_as_pct_of_gmv      DOUBLE,

    -- Incrementality analysis
    baseline_order_probability  DOUBLE,         -- predicted P(order) without promo
    treated_order_probability   DOUBLE,         -- observed P(order) with promo
    estimated_incremental_orders BIGINT,
    incrementality_rate         DOUBLE,         -- incremental / total redeemed
    incremental_gmv             DECIMAL(16,2),
    cost_per_incremental_order  DECIMAL(10,2),

    -- ROI
    gross_roi                   DOUBLE,         -- (total_gmv - discount_cost) / discount_cost
    net_roi                     DOUBLE,         -- (incremental_gmv - discount_cost) / discount_cost
    payback_period_days         INTEGER,        -- estimated days to recover cost via repeat orders

    -- User impact
    unique_users_redeemed       BIGINT,
    new_users_acquired          BIGINT,
    dormant_users_reactivated   BIGINT,
    existing_users_retained     BIGINT,
    customer_acquisition_cost   DECIMAL(10,2),  -- for new user promos
    reactivation_cost           DECIMAL(10,2),  -- for winback promos

    -- Post-promo behavior (30-day lookback from redemption)
    users_ordered_again_7d      BIGINT,
    users_ordered_again_14d     BIGINT,
    users_ordered_again_30d     BIGINT,
    repeat_rate_7d              DOUBLE,
    repeat_rate_14d             DOUBLE,
    repeat_rate_30d             DOUBLE,
    avg_orders_post_promo_30d   DOUBLE,         -- avg orders in 30d after promo use
    avg_gmv_post_promo_30d      DECIMAL(10,2),
    ltv_uplift_estimate         DECIMAL(10,2),  -- estimated LTV uplift from promo

    -- Cannibalization
    cannibalization_rate        DOUBLE,         -- orders that would have happened anyway
    promo_stacking_count        BIGINT,         -- users using multiple promos
    avg_promos_per_user         DOUBLE,

    -- Benchmarks
    category_avg_redemption_rate DOUBLE,
    category_avg_roi            DOUBLE,
    performance_vs_category     VARCHAR,        -- 'above_avg', 'avg', 'below_avg'

    -- Metadata
    model_version               VARCHAR,        -- incrementality model version
    confidence_interval_lower   DOUBLE,
    confidence_interval_upper   DOUBLE,
    last_refreshed_at           TIMESTAMP(6),
    pipeline_run_id             VARCHAR
)
WITH (
    format           = 'PARQUET',
    location         = 's3a://zomato-data-lake-prod/gold/promo_effectiveness/',
    partitioning     = ARRAY['analysis_date'],
    sorted_by        = ARRAY['city_id', 'promo_code'],
    format_version   = 2
);
