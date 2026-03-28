-- ============================================================================
-- Zomato Data Platform - Curated Layer: User Activity (Athena / Glue Data Catalog)
-- Format: Iceberg (Apache Iceberg v2) | Partitioned by: activity_date, city_id
-- Daily pre-aggregated user activity metrics for downstream analytics
-- NOTE: Schema references use Glue Data Catalog (no catalog prefix needed).
-- ============================================================================

CREATE TABLE IF NOT EXISTS zomato_curated.user_activity (
    user_id                     STRING,
    city_id                     INT,
    city_name                   STRING,
    activity_date               DATE,

    -- Order metrics
    order_count                 INT,
    completed_order_count       INT,
    cancelled_order_count       INT,
    total_gmv                   DECIMAL(14,2),
    total_discount              DECIMAL(14,2),
    net_spend                   DECIMAL(14,2),
    avg_order_value             DECIMAL(10,2),
    max_order_value             DECIMAL(10,2),
    min_order_value             DECIMAL(10,2),
    distinct_restaurants        INT,
    distinct_cuisines           INT,
    items_ordered               INT,

    -- Delivery experience
    avg_delivery_time_min       DOUBLE,
    max_delivery_time_min       DOUBLE,
    late_delivery_count         INT,
    avg_delivery_rating         DOUBLE,

    -- App engagement
    app_open_count              INT,
    search_count                INT,
    restaurant_view_count       INT,
    menu_view_count             INT,
    cart_add_count              INT,
    cart_abandon_count          INT,
    session_count               INT,
    total_session_duration_sec  BIGINT,
    avg_session_duration_sec    DOUBLE,

    -- Platform
    primary_platform            STRING,        -- most used platform that day
    primary_payment_method      STRING,

    -- Engagement scoring
    engagement_score            DOUBLE,         -- composite score 0-100
    is_active                   BOOLEAN,        -- had at least 1 app open
    is_transacting              BOOLEAN,        -- placed at least 1 order

    -- User attributes (snapshotted)
    is_pro_member               BOOLEAN,
    pro_tier                    STRING,
    lifetime_order_count        INT,
    days_since_registration     INT,
    days_since_last_order       INT,

    -- Metadata
    processed_at                TIMESTAMP,
    pipeline_version            STRING
)
PARTITIONED BY (activity_date, city_id)
WITH (
    table_type   = 'ICEBERG',
    format       = 'PARQUET',
    location     = 's3://zomato-data-lake-prod/curated/user_activity/',
    is_external  = false,
    write_compression = 'ZSTD'
);

-- Incremental merge pattern for late-arriving data:
-- MERGE INTO zomato_curated.user_activity AS target
-- USING staging.user_activity_daily AS source
-- ON target.user_id = source.user_id
--    AND target.activity_date = source.activity_date
--    AND target.city_id = source.city_id
-- WHEN MATCHED THEN UPDATE SET
--    order_count = source.order_count,
--    total_gmv = source.total_gmv,
--    ...
-- WHEN NOT MATCHED THEN INSERT (...)
-- VALUES (...);
