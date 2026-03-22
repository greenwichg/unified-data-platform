-- =============================================================================
-- Iceberg Table DDL: User Activity
-- Catalog: iceberg | Schema: zomato
--
-- Stores user activity events from Pipeline 4 (realtime events).
-- Captures all user interactions: logins, searches, page views,
-- location updates, ratings, and app sessions.
--
-- Partitioned by date and hour for efficient time-range queries on
-- high-volume event data (20B+ events/week).
-- Written in ORC format with Snappy compression.
-- =============================================================================

CREATE TABLE IF NOT EXISTS iceberg.zomato.user_activity (
    event_id            VARCHAR     NOT NULL,
    user_id             VARCHAR     NOT NULL,
    event_type          VARCHAR     NOT NULL  COMMENT 'USER_LOGIN, MENU_VIEWED, RESTAURANT_SEARCH, PAGE_VIEWED, etc.',
    source              VARCHAR     NOT NULL  COMMENT 'mobile_app, web, api',
    session_id          VARCHAR,
    city                VARCHAR,
    latitude            DOUBLE,
    longitude           DOUBLE,
    platform            VARCHAR               COMMENT 'android, ios, web',
    app_version         VARCHAR,
    os_version          VARCHAR,
    device_model        VARCHAR,
    -- Search-specific fields
    search_query        VARCHAR,
    search_results_count INTEGER,
    -- Restaurant/menu interaction fields
    restaurant_id       VARCHAR,
    item_id             VARCHAR,
    -- Order-related fields
    order_id            VARCHAR,
    total_amount        DOUBLE,
    -- Page view fields
    page_name           VARCHAR,
    referrer            VARCHAR,
    time_spent_seconds  INTEGER,
    -- Rating fields
    rating_value        REAL,
    rating_comment      VARCHAR,
    -- Event metadata
    event_timestamp     TIMESTAMP(6) NOT NULL,
    processing_time     TIMESTAMP(6),
    dt                  DATE         NOT NULL  COMMENT 'Partition column: event date',
    hour                INTEGER      NOT NULL  COMMENT 'Partition column: event hour (0-23)'
)
WITH (
    format = 'ORC',
    format_version = 2,
    location = 's3://zomato-data-platform-raw-data-lake/iceberg/zomato/user_activity',
    partitioning = ARRAY['dt', 'hour'],
    orc_compression = 'SNAPPY'
);
