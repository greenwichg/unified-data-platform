-- ============================================================================
-- Zomato Data Platform - Curated Layer: Promotions
-- Format: Iceberg (Apache Iceberg v2) | Partitioned by: promo_date, city_id
-- Cleaned and enriched promo/coupon data with usage and effectiveness metrics
-- ============================================================================

CREATE TABLE IF NOT EXISTS curated.promo_curated (
    promo_id                    VARCHAR         NOT NULL,
    promo_code                  VARCHAR         NOT NULL,
    promo_name                  VARCHAR,
    city_id                     INTEGER         NOT NULL,
    city_name                   VARCHAR,
    zone_id                     INTEGER,

    -- Promo configuration
    discount_type               VARCHAR         NOT NULL,   -- 'flat', 'percentage', 'freebie', 'bogo', 'cashback'
    discount_value              DECIMAL(10,2),              -- absolute value or percentage
    max_discount_cap            DECIMAL(10,2),              -- maximum discount amount
    min_order_value             DECIMAL(10,2),              -- minimum order value to apply
    promo_category              VARCHAR,                    -- 'new_user', 'retention', 'winback', 'festive', 'partnership'
    funding_source              VARCHAR,                    -- 'platform', 'restaurant', 'co_funded', 'brand_partner'
    platform_funding_pct        DOUBLE,
    restaurant_funding_pct      DOUBLE,

    -- Targeting
    target_segment              VARCHAR,                    -- 'all', 'new_users', 'dormant', 'pro_members', 'specific_cohort'
    target_cuisine              VARCHAR,                    -- null = all cuisines
    target_restaurant_ids       VARCHAR,                    -- comma-separated, null = all restaurants
    is_personalized             BOOLEAN,
    is_auto_applied             BOOLEAN,

    -- Validity
    valid_from                  TIMESTAMP(6),
    valid_until                 TIMESTAMP(6),
    is_active                   BOOLEAN,
    is_expired                  BOOLEAN,
    max_total_redemptions       BIGINT,
    max_per_user_redemptions    INTEGER,
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
    promo_date                  DATE            NOT NULL,
    created_at                  TIMESTAMP(6),
    processed_at                TIMESTAMP(6),
    pipeline_run_id             VARCHAR
)
WITH (
    format           = 'PARQUET',
    location         = 's3a://zomato-data-lake-prod/curated/promo/',
    partitioning     = ARRAY['promo_date', 'city_id'],
    sorted_by        = ARRAY['promo_code'],
    format_version   = 2
);
