-- ============================================================================
-- Zomato Data Platform - Curated Layer: Promotions (Athena / Glue Data Catalog)
-- Format: Iceberg (Apache Iceberg v2) | Partitioned by: promo_date, city_id
-- Cleaned and enriched promo/coupon data with usage and effectiveness metrics
-- NOTE: Schema references use Glue Data Catalog (no catalog prefix needed).
-- ============================================================================

CREATE TABLE IF NOT EXISTS zomato_curated.promo_curated (
    promo_id                    STRING,
    promo_code                  STRING,
    promo_name                  STRING,
    city_id                     INT,
    city_name                   STRING,
    zone_id                     INT,

    -- Promo configuration
    discount_type               STRING,            -- 'flat', 'percentage', 'freebie', 'bogo', 'cashback'
    discount_value              DECIMAL(10,2),              -- absolute value or percentage
    max_discount_cap            DECIMAL(10,2),              -- maximum discount amount
    min_order_value             DECIMAL(10,2),              -- minimum order value to apply
    promo_category              STRING,                    -- 'new_user', 'retention', 'winback', 'festive', 'partnership'
    funding_source              STRING,                    -- 'platform', 'restaurant', 'co_funded', 'brand_partner'
    platform_funding_pct        DOUBLE,
    restaurant_funding_pct      DOUBLE,

    -- Targeting
    target_segment              STRING,                    -- 'all', 'new_users', 'dormant', 'pro_members', 'specific_cohort'
    target_cuisine              STRING,                    -- null = all cuisines
    target_restaurant_ids       STRING,                    -- comma-separated, null = all restaurants
    is_personalized             BOOLEAN,
    is_auto_applied             BOOLEAN,

    -- Validity
    valid_from                  TIMESTAMP,
    valid_until                 TIMESTAMP,
    is_active                   BOOLEAN,
    is_expired                  BOOLEAN,
    max_total_redemptions       BIGINT,
    max_per_user_redemptions    INT,
    remaining_budget            DECIMAL(14,2),
    total_budget                DECIMAL(14,2),

    -- Usage metrics (as of promo_date)
    total_redemptions           BIGINT,
    unique_users_redeemed       BIGINT,
    total_order_value           DECIMAL(16,2),
    total_discount_given        DECIMAL(16,2),
    avg_order_value_with_promo  DECIMAL(10,2),
    avg_order_value_without     DECIMAL(10,2),  -- baseline AOV for same segment
    aov_lift_pct                DOUBLE,         -- (with - without) / without * 100
    redemption_rate             DOUBLE,         -- redeemed / distributed

    -- Effectiveness
    incremental_orders          BIGINT,         -- estimated orders that would not have happened
    incrementality_rate         DOUBLE,         -- incremental / total
    cost_per_order              DECIMAL(10,2),  -- total_discount / total_redemptions
    cost_per_incremental_order  DECIMAL(10,2),
    roi                         DOUBLE,         -- (incremental_revenue - discount_cost) / discount_cost
    customer_acquisition_cost   DECIMAL(10,2),  -- for new user promos

    -- Repeat behavior
    users_repeat_within_7d      BIGINT,
    users_repeat_within_30d     BIGINT,
    repeat_rate_7d              DOUBLE,
    repeat_rate_30d             DOUBLE,

    -- Metadata
    promo_date                  DATE,
    created_at                  TIMESTAMP,
    processed_at                TIMESTAMP,
    pipeline_run_id             STRING
)
PARTITIONED BY (promo_date, city_id)
WITH (
    table_type   = 'ICEBERG',
    format       = 'PARQUET',
    location     = 's3://zomato-data-lake-prod/curated/promo/',
    is_external  = false,
    write_compression = 'ZSTD'
);
