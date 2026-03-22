-- ============================================================================
-- Zomato Data Platform - Raw Layer: Users (Athena / Glue Data Catalog)
-- Format: ORC | Partitioned by: dt (ingestion date)
-- Source: Kafka topic 'users' via Flink sink (CDC from Aurora PostgreSQL)
-- NOTE: Schema references use Glue Data Catalog (no catalog prefix needed).
-- ============================================================================

CREATE TABLE IF NOT EXISTS zomato_raw.users_raw (
    user_id             STRING,
    phone_number_hash   STRING,        -- SHA-256 hashed for PII compliance
    email_hash          STRING,        -- SHA-256 hashed
    user_name           STRING,
    registration_date   DATE,
    city_id             INT,
    city_name           STRING,
    state               STRING,
    country             STRING,
    pincode             STRING,
    preferred_language  STRING,
    is_pro_member       BOOLEAN,
    pro_tier            STRING,        -- 'silver', 'gold', 'platinum'
    pro_expiry_date     DATE,
    lifetime_order_count    INT,
    lifetime_gmv            DOUBLE,
    last_order_date         DATE,
    avg_order_value         DOUBLE,
    preferred_payment       STRING,
    preferred_cuisine       ARRAY<STRING>,
    dietary_preference      STRING,    -- 'veg', 'non_veg', 'egg'
    default_address         STRUCT<
        lat:                DOUBLE,
        lng:                DOUBLE,
        pincode:            STRING,
        locality:           STRING,
        city:               STRING
    >,
    referral_code           STRING,
    referred_by             STRING,
    signup_source           STRING,    -- 'organic', 'referral', 'campaign', 'social'
    signup_platform         STRING,    -- 'android', 'ios', 'web'
    device_model            STRING,
    os_version              STRING,
    app_install_date        DATE,
    last_app_open_date      DATE,
    push_notification_enabled   BOOLEAN,
    email_opt_in                BOOLEAN,
    sms_opt_in                  BOOLEAN,
    account_status          STRING,    -- 'active', 'suspended', 'deleted', 'dormant'
    fraud_score             DOUBLE,
    cdc_operation           STRING,
    cdc_timestamp           TIMESTAMP,
    source_updated_at       TIMESTAMP,
    kafka_offset            BIGINT,
    kafka_partition         INT,
    kafka_timestamp         TIMESTAMP,
    ingested_at             TIMESTAMP
)
WITH (
    table_type   = 'ICEBERG',
    format       = 'ORC',
    location     = 's3://zomato-data-lake-prod/raw/users/',
    is_external  = false
);
