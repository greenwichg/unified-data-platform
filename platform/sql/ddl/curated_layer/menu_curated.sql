-- ============================================================================
-- Zomato Data Platform - Curated Layer: Menu Items (Athena / Glue Data Catalog)
-- Format: Iceberg (Apache Iceberg v2) | Partitioned by: snapshot_date, city_id
-- Cleaned and enriched menu data with pricing analytics and categorization
-- NOTE: Schema references use Glue Data Catalog (no catalog prefix needed).
-- ============================================================================

CREATE TABLE IF NOT EXISTS zomato_curated.menu_curated (
    menu_item_id                STRING,
    restaurant_id               STRING,
    restaurant_name             STRING,
    city_id                     INT,
    city_name                   STRING,
    zone_id                     INT,
    zone_name                   STRING,

    -- Item details
    item_name                   STRING,
    item_description            STRING,
    category                    STRING,        -- 'starters', 'mains', 'desserts', 'beverages', etc.
    sub_category                STRING,
    cuisine_type                STRING,
    is_veg                      BOOLEAN,
    is_vegan                    BOOLEAN,
    is_jain                     BOOLEAN,
    is_eggetarian               BOOLEAN,
    spice_level                 STRING,        -- 'mild', 'medium', 'hot', 'extra_hot'
    allergen_tags               STRING,        -- comma-separated: 'gluten,dairy,nuts'
    dietary_tags                STRING,        -- comma-separated: 'keto,low_carb,high_protein'

    -- Pricing
    base_price                  DECIMAL(10,2),
    current_price               DECIMAL(10,2),
    price_currency              STRING,
    discount_pct                DOUBLE,
    effective_price             DECIMAL(10,2),  -- after discount
    price_tier                  STRING,        -- 'budget', 'mid_range', 'premium', 'luxury'
    price_change_7d_pct         DOUBLE,         -- price change vs 7 days ago
    price_change_30d_pct        DOUBLE,         -- price change vs 30 days ago

    -- Availability
    is_available                BOOLEAN,
    is_recommended              BOOLEAN,
    is_bestseller               BOOLEAN,
    is_new                      BOOLEAN,
    available_start_time        STRING,        -- '11:00'
    available_end_time          STRING,        -- '23:00'
    available_days              STRING,        -- 'MON,TUE,WED,THU,FRI,SAT,SUN'

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
    restaurant_rank_by_orders   INT,        -- rank within restaurant
    city_category_rank          INT,        -- rank within city+category
    overall_popularity_score    DOUBLE,         -- composite score 0-100

    -- Metadata
    first_listed_date           DATE,
    last_updated_at             TIMESTAMP,
    snapshot_date               DATE,
    processed_at                TIMESTAMP
)
PARTITIONED BY (snapshot_date, city_id)
WITH (
    table_type   = 'ICEBERG',
    format       = 'PARQUET',
    location     = 's3://zomato-data-lake-prod/curated/menu/',
    is_external  = false,
    write_compression = 'ZSTD'
);

-- Incremental refresh pattern:
-- MERGE INTO zomato_curated.menu_curated AS target
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
