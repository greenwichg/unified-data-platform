-- ============================================================================
-- Zomato Data Platform - Curated Layer: Restaurants
-- Format: Iceberg (Apache Iceberg v2) | Partitioned by: snapshot_date, city_id
-- Master restaurant dimension with daily-refreshed operational and quality metrics
-- ============================================================================

CREATE TABLE IF NOT EXISTS curated.restaurant_curated (
    restaurant_id               VARCHAR         NOT NULL,
    restaurant_name             VARCHAR         NOT NULL,
    city_id                     INTEGER         NOT NULL,
    city_name                   VARCHAR,
    zone_id                     INTEGER,
    zone_name                   VARCHAR,
    locality                    VARCHAR,
    address                     VARCHAR,
    latitude                    DOUBLE,
    longitude                   DOUBLE,
    pincode                     VARCHAR,

    -- Restaurant classification
    restaurant_type             VARCHAR,        -- 'cloud_kitchen', 'dine_in', 'qsr', 'fine_dining', 'cafe'
    chain_id                    VARCHAR,        -- null for independents
    chain_name                  VARCHAR,
    is_chain                    BOOLEAN,
    cuisine_primary             VARCHAR,
    cuisine_secondary           VARCHAR,
    cuisine_tags                VARCHAR,        -- comma-separated: 'north_indian,mughlai,biryani'
    cost_for_two                DECIMAL(10,2),
    price_segment               VARCHAR,        -- 'budget', 'mid_range', 'premium', 'fine_dining'
    dietary_options             VARCHAR,        -- 'veg_only', 'veg_nonveg', 'nonveg_only'

    -- Partnership
    onboarding_date             DATE,
    is_active                   BOOLEAN,
    is_pro_partner              BOOLEAN,
    commission_rate             DOUBLE,
    contract_type               VARCHAR,        -- 'standard', 'exclusive', 'premium'
    account_manager_id          VARCHAR,

    -- Ratings
    overall_rating              DOUBLE,
    food_rating                 DOUBLE,
    packaging_rating            DOUBLE,
    delivery_rating             DOUBLE,
    total_rating_count          BIGINT,
    rating_trend_30d            DOUBLE,         -- change in rating over 30 days
    sentiment_score             DOUBLE,         -- NLP-derived -1 to 1

    -- Operational metrics (rolling 30-day)
    total_orders_30d            BIGINT,
    completed_orders_30d        BIGINT,
    cancelled_orders_30d        BIGINT,
    rejected_orders_30d         BIGINT,
    completion_rate_30d         DOUBLE,
    rejection_rate_30d          DOUBLE,
    gmv_30d                     DECIMAL(16,2),
    avg_order_value_30d         DECIMAL(10,2),
    unique_customers_30d        BIGINT,
    new_customers_30d           BIGINT,
    repeat_customer_rate_30d    DOUBLE,

    -- Preparation and delivery
    avg_preparation_time_30d    DOUBLE,         -- minutes
    p90_preparation_time_30d    DOUBLE,
    avg_accept_time_sec_30d     DOUBLE,
    sla_breach_rate_30d         DOUBLE,
    avg_delivery_time_30d       DOUBLE,

    -- Menu
    total_menu_items            INTEGER,
    available_menu_items        INTEGER,
    menu_availability_rate      DOUBLE,
    bestseller_count            INTEGER,
    avg_item_price              DECIMAL(10,2),

    -- Availability
    avg_online_hours_30d        DOUBLE,
    availability_rate_30d       DOUBLE,
    unplanned_offline_count_30d INTEGER,

    -- Quality flags
    is_hygiene_certified        BOOLEAN,
    food_safety_score           DOUBLE,
    has_active_complaints       BOOLEAN,
    complaint_count_30d         INTEGER,

    -- Rankings
    city_rank_by_orders         INTEGER,
    city_rank_by_rating         INTEGER,
    city_rank_by_gmv            INTEGER,
    zone_rank_by_orders         INTEGER,
    cuisine_rank_in_city        INTEGER,

    -- Metadata
    snapshot_date               DATE            NOT NULL,
    last_updated_at             TIMESTAMP(6),
    processed_at                TIMESTAMP(6)
)
WITH (
    format           = 'PARQUET',
    location         = 's3a://zomato-data-lake-prod/curated/restaurant/',
    partitioning     = ARRAY['snapshot_date', 'city_id'],
    sorted_by        = ARRAY['restaurant_id'],
    format_version   = 2
);
