-- ============================================================================
-- Zomato Data Platform - Gold Layer: User Cohort Analysis
-- Format: Iceberg | Partitioned by: cohort_month
-- Weekly-refreshed cohort table for retention, LTV, and behavioral analysis
-- Used by growth, marketing, and product analytics teams
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.user_cohorts (
    user_id                     VARCHAR         NOT NULL,
    cohort_month                VARCHAR         NOT NULL,   -- 'YYYY-MM' registration month
    cohort_week                 VARCHAR,                    -- 'YYYY-WW' registration week
    city_id                     INTEGER         NOT NULL,
    city_name                   VARCHAR,
    signup_source               VARCHAR,
    signup_platform             VARCHAR,

    -- Cohort classification
    cohort_size                 BIGINT,         -- total users in this cohort+city
    user_segment                VARCHAR,        -- 'power', 'regular', 'casual', 'dormant', 'churned'
    lifecycle_stage             VARCHAR,        -- 'new', 'activated', 'retained', 'resurrected', 'churned'
    rfm_segment                 VARCHAR,        -- recency-frequency-monetary segment

    -- Retention flags (monthly)
    is_active_m0                BOOLEAN,        -- active in registration month
    is_active_m1                BOOLEAN,
    is_active_m2                BOOLEAN,
    is_active_m3                BOOLEAN,
    is_active_m6                BOOLEAN,
    is_active_m9                BOOLEAN,
    is_active_m12               BOOLEAN,

    -- Transaction retention (monthly)
    has_order_m0                BOOLEAN,
    has_order_m1                BOOLEAN,
    has_order_m2                BOOLEAN,
    has_order_m3                BOOLEAN,
    has_order_m6                BOOLEAN,
    has_order_m12               BOOLEAN,

    -- Cumulative metrics at snapshot
    days_since_registration     INTEGER,
    days_since_last_order       INTEGER,
    days_since_last_app_open    INTEGER,
    lifetime_order_count        INTEGER,
    lifetime_gmv                DECIMAL(14,2),
    lifetime_discount_received  DECIMAL(14,2),
    lifetime_net_spend          DECIMAL(14,2),
    estimated_ltv               DECIMAL(14,2),  -- predicted lifetime value (ML model output)

    -- Behavioral metrics
    avg_order_value             DECIMAL(10,2),
    avg_orders_per_month        DOUBLE,
    avg_days_between_orders     DOUBLE,
    preferred_cuisine           VARCHAR,
    preferred_payment_method    VARCHAR,
    preferred_order_hour        INTEGER,
    preferred_order_day         VARCHAR,        -- 'monday', 'tuesday', ...
    distinct_restaurants        INTEGER,
    restaurant_loyalty_score    DOUBLE,         -- concentration of orders across restaurants
    dietary_preference          VARCHAR,

    -- Engagement
    avg_monthly_sessions        DOUBLE,
    avg_session_duration_sec    DOUBLE,
    push_notification_ctr       DOUBLE,
    promo_response_rate         DOUBLE,         -- orders with coupon / total promos sent
    search_to_order_rate        DOUBLE,

    -- Pro membership
    is_pro_member               BOOLEAN,
    pro_tier                    VARCHAR,
    pro_conversion_month        VARCHAR,        -- month they became pro
    months_as_pro               INTEGER,

    -- Churn prediction
    churn_probability_30d       DOUBLE,         -- ML model output
    churn_risk_tier             VARCHAR,        -- 'low', 'medium', 'high', 'critical'
    recommended_intervention    VARCHAR,        -- 'discount_nudge', 'push_campaign', 'win_back_call'

    -- Metadata
    snapshot_date               DATE,
    last_refreshed_at           TIMESTAMP(6),
    pipeline_run_id             VARCHAR
)
WITH (
    format           = 'PARQUET',
    location         = 's3a://zomato-data-lake-prod/gold/user_cohorts/',
    partitioning     = ARRAY['cohort_month'],
    sorted_by        = ARRAY['city_id', 'user_id'],
    format_version   = 2
);
