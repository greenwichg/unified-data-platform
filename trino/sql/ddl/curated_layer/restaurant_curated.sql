-- ============================================================================
-- Zomato Data Platform - Curated Layer: Restaurants (Athena / Glue Data Catalog)
-- Format: Iceberg (Apache Iceberg v2) | Partitioned by: snapshot_date, city_id
-- Master restaurant dimension with daily-refreshed operational and quality metrics
-- NOTE: Schema references use Glue Data Catalog (no catalog prefix needed).
-- ============================================================================

CREATE TABLE IF NOT EXISTS zomato_curated.restaurant_curated (
    restaurant_id               STRING,
    restaurant_name             STRING,
    city_id                     INT,
    city_name                   STRING,
    zone_id                     INT,
    zone_name                   STRING,
    locality                    STRING,
    address                     STRING,
    latitude                    DOUBLE,
    longitude                   DOUBLE,
    pincode                     STRING,

    -- Restaurant classification
    restaurant_type             STRING,        -- 'cloud_kitchen', 'dine_in', 'qsr', 'fine_dining', 'cafe'
    chain_id                    STRING,        -- null for independents
    chain_name                  STRING,
    is_chain                    BOOLEAN,
    cuisine_primary             STRING,
    cuisine_secondary           STRING,
    cuisine_tags                STRING,        -- comma-separated: 'north_indian,mughlai,biryani'
    cost_for_two                DECIMAL(10,2),
    price_segment               STRING,        -- 'budget', 'mid_range', 'premium', 'fine_dining'
    dietary_options             STRING,        -- 'veg_only', 'veg_nonveg', 'nonveg_only'

    -- Partnership
    onboarding_date             DATE,
    is_active                   BOOLEAN,
    is_pro_partner              BOOLEAN,
    commission_rate             DOUBLE,
    contract_type               STRING,        -- 'standard', 'exclusive', 'premium'
    account_manager_id          STRING,

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
    total_menu_items            INT,
    available_menu_items        INT,
    menu_availability_rate      DOUBLE,
    bestseller_count            INT,
    avg_item_price              DECIMAL(10,2),

    -- Availability
    avg_online_hours_30d        DOUBLE,
    availability_rate_30d       DOUBLE,
    unplanned_offline_count_30d INT,

    -- Quality flags
    is_hygiene_certified        BOOLEAN,
    food_safety_score           DOUBLE,
    has_active_complaints       BOOLEAN,
    complaint_count_30d         INT,

    -- Rankings
    city_rank_by_orders         INT,
    city_rank_by_rating         INT,
    city_rank_by_gmv            INT,
    zone_rank_by_orders         INT,
    cuisine_rank_in_city        INT,

    -- Metadata
    snapshot_date               DATE,
    last_updated_at             TIMESTAMP,
    processed_at                TIMESTAMP
)
PARTITIONED BY (snapshot_date, city_id)
WITH (
    table_type   = 'ICEBERG',
    format       = 'PARQUET',
    location     = 's3://zomato-data-lake-prod/curated/restaurant/',
    is_external  = false,
    write_compression = 'ZSTD'
);
