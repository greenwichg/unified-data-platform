-- ============================================================================
-- Zomato Data Platform - Raw Layer DDL
-- Format: ORC | Partitioned by: dt (ingestion date)
-- Source: Kafka topics via Flink sink
-- All tables are EXTERNAL, pointing to S3 paths managed by the ingestion layer
-- ============================================================================


-- ============================================================================
-- 1. ORDERS RAW
-- Source: Kafka topic 'orders'
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.orders_raw (
    order_id            VARCHAR,
    customer_id         VARCHAR,
    restaurant_id       VARCHAR,
    city_id             INTEGER,
    city_name           VARCHAR,
    order_status        VARCHAR,
    order_type          VARCHAR,        -- 'delivery', 'pickup', 'dine_in'
    payment_method      VARCHAR,        -- 'upi', 'card', 'cod', 'wallet'
    payment_status      VARCHAR,
    coupon_code         VARCHAR,
    subtotal_amount     DOUBLE,
    tax_amount          DOUBLE,
    delivery_fee        DOUBLE,
    discount_amount     DOUBLE,
    total_amount        DOUBLE,
    currency            VARCHAR,
    item_count          INTEGER,
    items               ARRAY(ROW(
        item_id         VARCHAR,
        item_name       VARCHAR,
        quantity        INTEGER,
        unit_price      DOUBLE,
        category        VARCHAR,
        is_veg          BOOLEAN
    )),
    delivery_address    ROW(
        lat             DOUBLE,
        lng             DOUBLE,
        pincode         VARCHAR,
        locality        VARCHAR
    ),
    rider_id            VARCHAR,
    restaurant_lat      DOUBLE,
    restaurant_lng      DOUBLE,
    estimated_delivery_minutes  INTEGER,
    actual_delivery_minutes     INTEGER,
    order_placed_at     TIMESTAMP,
    order_accepted_at   TIMESTAMP,
    order_prepared_at   TIMESTAMP,
    order_picked_up_at  TIMESTAMP,
    order_delivered_at  TIMESTAMP,
    order_cancelled_at  TIMESTAMP,
    cancellation_reason VARCHAR,
    platform            VARCHAR,        -- 'android', 'ios', 'web'
    app_version         VARCHAR,
    device_id           VARCHAR,
    session_id          VARCHAR,
    kafka_offset        BIGINT,
    kafka_partition     INTEGER,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP,
    dt                  VARCHAR
)
WITH (
    format           = 'ORC',
    external_location = 's3a://zomato-data-lake-prod/raw/orders/',
    partitioned_by   = ARRAY['dt'],
    orc_compression  = 'ZSTD'
);


-- ============================================================================
-- 2. USERS RAW
-- Source: Kafka topic 'user_events' + CDC from users DB
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.users_raw (
    user_id             VARCHAR,
    email_hash          VARCHAR,        -- SHA-256 hashed for PII compliance
    phone_hash          VARCHAR,
    full_name_encrypted VARCHAR,        -- AES-256 encrypted
    signup_date         DATE,
    city_id             INTEGER,
    city_name           VARCHAR,
    pincode             VARCHAR,
    locality            VARCHAR,
    lat                 DOUBLE,
    lng                 DOUBLE,
    gender              VARCHAR,
    age_bucket          VARCHAR,        -- '18-24', '25-34', '35-44', '45+'
    is_pro_member       BOOLEAN,
    pro_membership_start DATE,
    pro_membership_end  DATE,
    preferred_language  VARCHAR,
    preferred_cuisine   ARRAY(VARCHAR),
    device_type         VARCHAR,        -- 'android', 'ios'
    app_install_date    DATE,
    last_active_at      TIMESTAMP,
    total_orders        INTEGER,
    total_gmv           DOUBLE,
    avg_order_value     DOUBLE,
    lifetime_discount   DOUBLE,
    referral_code       VARCHAR,
    referred_by         VARCHAR,
    account_status      VARCHAR,        -- 'active', 'suspended', 'deleted'
    cdc_operation       VARCHAR,        -- 'INSERT', 'UPDATE', 'DELETE'
    cdc_timestamp       TIMESTAMP,
    kafka_offset        BIGINT,
    kafka_partition     INTEGER,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP,
    dt                  VARCHAR
)
WITH (
    format           = 'ORC',
    external_location = 's3a://zomato-data-lake-prod/raw/users/',
    partitioned_by   = ARRAY['dt'],
    orc_compression  = 'ZSTD'
);


-- ============================================================================
-- 3. RESTAURANTS RAW
-- Source: Kafka topic 'restaurant_events' + CDC from restaurants DB
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.restaurants_raw (
    restaurant_id       VARCHAR,
    restaurant_name     VARCHAR,
    restaurant_type     VARCHAR,        -- 'restaurant', 'cloud_kitchen', 'qsr', 'cafe'
    cuisine_tags        ARRAY(VARCHAR), -- ['north_indian', 'chinese', 'biryani']
    city_id             INTEGER,
    city_name           VARCHAR,
    zone_id             INTEGER,
    zone_name           VARCHAR,
    locality            VARCHAR,
    pincode             VARCHAR,
    lat                 DOUBLE,
    lng                 DOUBLE,
    owner_id            VARCHAR,
    is_active           BOOLEAN,
    is_accepting_orders BOOLEAN,
    avg_prep_time_min   INTEGER,
    avg_rating          DOUBLE,
    total_ratings       INTEGER,
    price_range         INTEGER,        -- 1 (budget) to 4 (premium)
    is_pro_enabled      BOOLEAN,
    commission_rate     DOUBLE,
    opening_hours       VARCHAR,        -- JSON string of operating hours
    fssai_license       VARCHAR,
    gst_number          VARCHAR,
    onboarding_date     DATE,
    last_order_at       TIMESTAMP,
    menu_item_count     INTEGER,
    images_count        INTEGER,
    has_delivery         BOOLEAN,
    has_pickup          BOOLEAN,
    has_dine_in         BOOLEAN,
    cdc_operation       VARCHAR,
    cdc_timestamp       TIMESTAMP,
    kafka_offset        BIGINT,
    kafka_partition     INTEGER,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP,
    dt                  VARCHAR
)
WITH (
    format           = 'ORC',
    external_location = 's3a://zomato-data-lake-prod/raw/restaurants/',
    partitioned_by   = ARRAY['dt'],
    orc_compression  = 'ZSTD'
);


-- ============================================================================
-- 4. MENU RAW
-- Source: Kafka topic 'menu_updates' + CDC from menu DB
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.menu_raw (
    menu_item_id        VARCHAR,
    restaurant_id       VARCHAR,
    item_name           VARCHAR,
    item_description    VARCHAR,
    category            VARCHAR,        -- 'starters', 'mains', 'desserts', 'beverages'
    sub_category        VARCHAR,
    cuisine_type        VARCHAR,
    is_veg              BOOLEAN,
    is_egg              BOOLEAN,
    is_vegan            BOOLEAN,
    price               DOUBLE,
    discounted_price    DOUBLE,
    currency            VARCHAR,
    is_available        BOOLEAN,
    is_recommended      BOOLEAN,
    is_bestseller       BOOLEAN,
    spice_level         INTEGER,        -- 0-3
    serves              INTEGER,        -- number of people
    prep_time_min       INTEGER,
    calories            INTEGER,
    allergens           ARRAY(VARCHAR),
    tags                ARRAY(VARCHAR), -- ['must_try', 'new', 'chefs_special']
    image_url           VARCHAR,
    display_order       INTEGER,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    cdc_operation       VARCHAR,
    cdc_timestamp       TIMESTAMP,
    kafka_offset        BIGINT,
    kafka_partition     INTEGER,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP,
    dt                  VARCHAR
)
WITH (
    format           = 'ORC',
    external_location = 's3a://zomato-data-lake-prod/raw/menu/',
    partitioned_by   = ARRAY['dt'],
    orc_compression  = 'ZSTD'
);


-- ============================================================================
-- 5. DELIVERIES RAW
-- Source: Kafka topic 'delivery_events' (real-time rider tracking)
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.deliveries_raw (
    delivery_id         VARCHAR,
    order_id            VARCHAR,
    rider_id            VARCHAR,
    restaurant_id       VARCHAR,
    customer_id         VARCHAR,
    city_id             INTEGER,
    zone_id             INTEGER,
    delivery_status     VARCHAR,        -- 'assigned', 'en_route_pickup', 'at_restaurant',
                                        -- 'picked_up', 'en_route_delivery', 'delivered', 'failed'
    assignment_type     VARCHAR,        -- 'auto', 'manual', 'reassigned'
    rider_lat           DOUBLE,
    rider_lng           DOUBLE,
    restaurant_lat      DOUBLE,
    restaurant_lng      DOUBLE,
    customer_lat        DOUBLE,
    customer_lng        DOUBLE,
    pickup_distance_km  DOUBLE,
    delivery_distance_km DOUBLE,
    total_distance_km   DOUBLE,
    estimated_pickup_time   TIMESTAMP,
    actual_pickup_time      TIMESTAMP,
    estimated_delivery_time TIMESTAMP,
    actual_delivery_time    TIMESTAMP,
    assigned_at         TIMESTAMP,
    accepted_at         TIMESTAMP,
    reached_restaurant_at TIMESTAMP,
    picked_up_at        TIMESTAMP,
    delivered_at        TIMESTAMP,
    failed_at           TIMESTAMP,
    failure_reason      VARCHAR,
    delivery_fee        DOUBLE,
    rider_payout        DOUBLE,
    tip_amount          DOUBLE,
    surge_multiplier    DOUBLE,
    is_batched_delivery BOOLEAN,
    batch_id            VARCHAR,
    batch_sequence      INTEGER,
    weather_condition   VARCHAR,
    traffic_level       VARCHAR,        -- 'low', 'moderate', 'heavy'
    rider_rating        INTEGER,
    customer_feedback   VARCHAR,
    gps_trace_s3_path   VARCHAR,        -- S3 path to full GPS trace
    kafka_offset        BIGINT,
    kafka_partition     INTEGER,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP,
    dt                  VARCHAR
)
WITH (
    format           = 'ORC',
    external_location = 's3a://zomato-data-lake-prod/raw/deliveries/',
    partitioned_by   = ARRAY['dt'],
    orc_compression  = 'ZSTD'
);


-- ============================================================================
-- 6. PAYMENTS RAW
-- Source: Kafka topic 'payment_events' (from payment gateway callbacks)
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw.payments_raw (
    payment_id          VARCHAR,
    order_id            VARCHAR,
    customer_id         VARCHAR,
    payment_method      VARCHAR,        -- 'upi', 'credit_card', 'debit_card', 'net_banking',
                                        -- 'wallet', 'cod', 'emi', 'pay_later'
    payment_gateway     VARCHAR,        -- 'razorpay', 'paytm', 'phonepe', 'stripe'
    gateway_txn_id      VARCHAR,
    payment_status      VARCHAR,        -- 'initiated', 'authorized', 'captured', 'settled',
                                        -- 'failed', 'refund_initiated', 'refunded'
    amount              DOUBLE,
    currency            VARCHAR,
    tax_amount          DOUBLE,
    convenience_fee     DOUBLE,
    gateway_fee         DOUBLE,
    net_amount          DOUBLE,
    refund_amount       DOUBLE,
    refund_reason       VARCHAR,
    bank_name           VARCHAR,
    card_network        VARCHAR,        -- 'visa', 'mastercard', 'rupay', 'amex'
    card_type           VARCHAR,        -- 'credit', 'debit'
    card_last_four      VARCHAR,
    upi_vpa_hash        VARCHAR,        -- hashed VPA for PII compliance
    wallet_type         VARCHAR,        -- 'paytm', 'phonepe', 'amazon_pay'
    emi_tenure          INTEGER,
    emi_bank            VARCHAR,
    is_international    BOOLEAN,
    risk_score          DOUBLE,
    is_flagged          BOOLEAN,
    fraud_check_status  VARCHAR,        -- 'passed', 'review', 'blocked'
    initiated_at        TIMESTAMP,
    authorized_at       TIMESTAMP,
    captured_at         TIMESTAMP,
    settled_at          TIMESTAMP,
    failed_at           TIMESTAMP,
    refunded_at         TIMESTAMP,
    error_code          VARCHAR,
    error_message       VARCHAR,
    retry_count         INTEGER,
    idempotency_key     VARCHAR,
    kafka_offset        BIGINT,
    kafka_partition     INTEGER,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP,
    dt                  VARCHAR
)
WITH (
    format           = 'ORC',
    external_location = 's3a://zomato-data-lake-prod/raw/payments/',
    partitioned_by   = ARRAY['dt'],
    orc_compression  = 'ZSTD'
);


-- ============================================================================
-- Partition Repair Commands (run after new data lands)
-- ============================================================================
-- CALL system.sync_partition_metadata('raw', 'orders_raw', 'FULL');
-- CALL system.sync_partition_metadata('raw', 'users_raw', 'FULL');
-- CALL system.sync_partition_metadata('raw', 'restaurants_raw', 'FULL');
-- CALL system.sync_partition_metadata('raw', 'menu_raw', 'FULL');
-- CALL system.sync_partition_metadata('raw', 'deliveries_raw', 'FULL');
-- CALL system.sync_partition_metadata('raw', 'payments_raw', 'FULL');

-- ============================================================================
-- Example Queries
-- ============================================================================
-- Partition pruning: SELECT * FROM raw.orders_raw WHERE dt = '2026-03-21';
-- Latest snapshot:   SELECT * FROM raw.users_raw WHERE dt = (SELECT MAX(dt) FROM raw.users_raw);
-- Row counts:        SELECT dt, COUNT(*) FROM raw.payments_raw GROUP BY dt ORDER BY dt DESC LIMIT 7;
