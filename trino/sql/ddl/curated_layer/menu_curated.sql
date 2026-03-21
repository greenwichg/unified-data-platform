-- ============================================================================
-- Zomato Data Platform - Curated Layer: Menu Items
-- Format: Iceberg (Apache Iceberg v2) | Partitioned by: snapshot_date, city_id
-- Cleaned and enriched menu data with pricing analytics and categorization
-- ============================================================================

CREATE TABLE IF NOT EXISTS curated.menu_curated (
    menu_item_id                VARCHAR         NOT NULL,
    restaurant_id               VARCHAR         NOT NULL,
    restaurant_name             VARCHAR,
    city_id                     INTEGER         NOT NULL,
    city_name                   VARCHAR,
    zone_id                     INTEGER,
    zone_name                   VARCHAR,

    -- Item details
    item_name                   VARCHAR         NOT NULL,
    item_description            VARCHAR,
    category                    VARCHAR,        -- 'starters', 'mains', 'desserts', 'beverages', etc.
    sub_category                VARCHAR,
    cuisine_type                VARCHAR,
    is_veg                      BOOLEAN,
    is_vegan                    BOOLEAN,
    is_jain                     BOOLEAN,
    is_eggetarian               BOOLEAN,
    spice_level                 VARCHAR,        -- 'mild', 'medium', 'hot', 'extra_hot'
    allergen_tags               VARCHAR,        -- comma-separated: 'gluten,dairy,nuts'
    dietary_tags                VARCHAR,        -- comma-separated: 'keto,low_carb,high_protein'

    -- Pricing
    base_price                  DECIMAL(10,2)   NOT NULL,
    current_price               DECIMAL(10,2)   NOT NULL,
    price_currency              VARCHAR         DEFAULT 'INR',
    discount_pct                DOUBLE,
    effective_price             DECIMAL(10,2),  -- after discount
    price_tier                  VARCHAR,        -- 'budget', 'mid_range', 'premium', 'luxury'
    price_change_7d_pct         DOUBLE,         -- price change vs 7 days ago
    price_change_30d_pct        DOUBLE,         -- price change vs 30 days ago

    -- Availability
    is_available                BOOLEAN,
    is_recommended              BOOLEAN,
    is_bestseller               BOOLEAN,
    is_new                      BOOLEAN,
    available_start_time        VARCHAR,        -- '11:00'
    available_end_time          VARCHAR,        -- '23:00'
    available_days              VARCHAR,        -- 'MON,TUE,WED,THU,FRI,SAT,SUN'

    -- Performance metrics (rolling 30-day)
    total_orders_30d            BIGINT,
    total_revenue_30d           DECIMAL(14,2),
    unique_customers_30d        BIGINT,
    avg_rating_30d              DOUBLE,
    rating_count_30d            BIGINT,
    reorder_rate_30d            DOUBLE,         -- customers who ordered again within 30d
    cart_add_rate_30d           DOUBLE,         -- cart adds / menu views
    conversion_rate_30d         DOUBLE,         -- orders / cart adds

    -- Rankings
    restaurant_rank_by_orders   INTEGER,        -- rank within restaurant
    city_category_rank          INTEGER,        -- rank within city+category
    overall_popularity_score    DOUBLE,         -- composite score 0-100

    -- Metadata
    first_listed_date           DATE,
    last_updated_at             TIMESTAMP(6),
    snapshot_date               DATE            NOT NULL,
    processed_at                TIMESTAMP(6)
)
WITH (
    format           = 'PARQUET',
    location         = 's3a://zomato-data-lake-prod/curated/menu/',
    partitioning     = ARRAY['snapshot_date', 'city_id'],
    sorted_by        = ARRAY['restaurant_id', 'menu_item_id'],
    format_version   = 2
);

-- Incremental refresh pattern:
-- MERGE INTO curated.menu_curated AS target
-- USING staging.menu_daily AS source
-- ON target.menu_item_id = source.menu_item_id
--    AND target.snapshot_date = source.snapshot_date
-- WHEN MATCHED THEN UPDATE SET
--    current_price = source.current_price,
--    is_available = source.is_available,
--    total_orders_30d = source.total_orders_30d,
--    ...
-- WHEN NOT MATCHED THEN INSERT (...)
-- VALUES (...);
