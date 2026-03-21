-- ============================================================================
-- Zomato Data Platform - Raw Layer: Users
-- Format: ORC | Partitioned by: dt (ingestion date)
-- Source: Kafka topic 'users' via Flink sink (CDC from Aurora PostgreSQL)
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.users_raw (
    user_id             VARCHAR,
    phone_number_hash   VARCHAR,        -- SHA-256 hashed for PII compliance
    email_hash          VARCHAR,        -- SHA-256 hashed
    user_name           VARCHAR,
    registration_date   DATE,
    city_id             INTEGER,
    city_name           VARCHAR,
    state               VARCHAR,
    country             VARCHAR,
    pincode             VARCHAR,
    preferred_language  VARCHAR,
    is_pro_member       BOOLEAN,
    pro_tier            VARCHAR,        -- 'silver', 'gold', 'platinum'
    pro_expiry_date     DATE,
    lifetime_order_count    INTEGER,
    lifetime_gmv            DOUBLE,
    last_order_date         DATE,
    avg_order_value         DOUBLE,
    preferred_payment       VARCHAR,
    preferred_cuisine       ARRAY(VARCHAR),
    dietary_preference      VARCHAR,    -- 'veg', 'non_veg', 'egg'
    default_address         ROW(
        lat                 DOUBLE,
        lng                 DOUBLE,
        pincode             VARCHAR,
        locality            VARCHAR,
        city                VARCHAR
    ),
    referral_code           VARCHAR,
    referred_by             VARCHAR,
    signup_source           VARCHAR,    -- 'organic', 'referral', 'campaign', 'social'
    signup_platform         VARCHAR,    -- 'android', 'ios', 'web'
    device_model            VARCHAR,
    os_version              VARCHAR,
    app_install_date        DATE,
    last_app_open_date      DATE,
    push_notification_enabled   BOOLEAN,
    email_opt_in                BOOLEAN,
    sms_opt_in                  BOOLEAN,
    account_status          VARCHAR,    -- 'active', 'suspended', 'deleted', 'dormant'
    fraud_score             DOUBLE,
    cdc_operation           VARCHAR,
    cdc_timestamp           TIMESTAMP,
    source_updated_at       TIMESTAMP,
    kafka_offset            BIGINT,
    kafka_partition         INTEGER,
    kafka_timestamp         TIMESTAMP,
    ingested_at             TIMESTAMP
)
WITH (
    format           = 'ORC',
    external_location = 's3a://zomato-data-lake-prod/raw/users/',
    partitioned_by   = ARRAY['dt'],
    orc_compression  = 'ZSTD'
);
