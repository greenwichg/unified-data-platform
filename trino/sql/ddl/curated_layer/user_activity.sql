-- ============================================================================
-- Zomato Data Platform - Curated Layer: User Activity Aggregations
-- Format: Iceberg (Apache Iceberg v2) | Partitioned by: activity_date, city_id
-- Daily pre-aggregated user activity metrics for downstream analytics
-- ============================================================================

CREATE TABLE IF NOT EXISTS curated.user_activity (
    user_id                     VARCHAR         NOT NULL,
    city_id                     INTEGER         NOT NULL,
    city_name                   VARCHAR,
    activity_date               DATE            NOT NULL,

    -- Order metrics
    order_count                 INTEGER         DEFAULT 0,
    completed_order_count       INTEGER         DEFAULT 0,
    cancelled_order_count       INTEGER         DEFAULT 0,
    total_gmv                   DECIMAL(14,2)   DEFAULT 0,
    total_discount              DECIMAL(14,2)   DEFAULT 0,
    net_spend                   DECIMAL(14,2)   DEFAULT 0,
    avg_order_value             DECIMAL(10,2),
    max_order_value             DECIMAL(10,2),
    min_order_value             DECIMAL(10,2),
    distinct_restaurants        INTEGER         DEFAULT 0,
    distinct_cuisines           INTEGER         DEFAULT 0,
    items_ordered               INTEGER         DEFAULT 0,

    -- Delivery experience
    avg_delivery_time_min       DOUBLE,
    max_delivery_time_min       DOUBLE,
    late_delivery_count         INTEGER         DEFAULT 0,
    avg_delivery_rating         DOUBLE,

    -- App engagement
    app_open_count              INTEGER         DEFAULT 0,
    search_count                INTEGER         DEFAULT 0,
    restaurant_view_count       INTEGER         DEFAULT 0,
    menu_view_count             INTEGER         DEFAULT 0,
    cart_add_count              INTEGER         DEFAULT 0,
    cart_abandon_count          INTEGER         DEFAULT 0,
    session_count               INTEGER         DEFAULT 0,
    total_session_duration_sec  BIGINT          DEFAULT 0,
    avg_session_duration_sec    DOUBLE,

    -- Platform
    primary_platform            VARCHAR,        -- most used platform that day
    primary_payment_method      VARCHAR,

    -- Engagement scoring
    engagement_score            DOUBLE,         -- composite score 0-100
    is_active                   BOOLEAN,        -- had at least 1 app open
    is_transacting              BOOLEAN,        -- placed at least 1 order

    -- User attributes (snapshotted)
    is_pro_member               BOOLEAN,
    pro_tier                    VARCHAR,
    lifetime_order_count        INTEGER,
    days_since_registration     INTEGER,
    days_since_last_order       INTEGER,

    -- Metadata
    processed_at                TIMESTAMP(6),
    pipeline_version            VARCHAR
)
WITH (
    format           = 'PARQUET',
    location         = 's3a://zomato-data-lake-prod/curated/user_activity/',
    partitioning     = ARRAY['activity_date', 'city_id'],
    sorted_by        = ARRAY['user_id'],
    format_version   = 2
);

-- Incremental merge pattern for late-arriving data:
-- MERGE INTO curated.user_activity AS target
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
