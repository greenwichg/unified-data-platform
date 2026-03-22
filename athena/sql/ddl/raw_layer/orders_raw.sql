-- ============================================================================
-- Zomato Data Platform - Raw Layer DDL (Athena / Glue Data Catalog)
-- Format: ORC | Partitioned by: dt (ingestion date)
-- Source: Kafka topics via Flink sink
-- All tables are EXTERNAL, pointing to S3 paths managed by the ingestion layer
-- NOTE: Schema references use Glue Data Catalog (no catalog prefix needed).
-- ============================================================================


-- ============================================================================
-- 1. ORDERS RAW
-- Source: Kafka topic 'orders'
-- ============================================================================
CREATE TABLE IF NOT EXISTS zomato_raw.orders_raw (
    order_id            STRING,
    customer_id         STRING,
    restaurant_id       STRING,
    city_id             INT,
    city_name           STRING,
    order_status        STRING,
    order_type          STRING,        -- 'delivery', 'pickup', 'dine_in'
    payment_method      STRING,        -- 'upi', 'card', 'cod', 'wallet'
    payment_status      STRING,
    coupon_code         STRING,
    subtotal_amount     DOUBLE,
    tax_amount          DOUBLE,
    delivery_fee        DOUBLE,
    discount_amount     DOUBLE,
    total_amount        DOUBLE,
    currency            STRING,
    item_count          INT,
    items               ARRAY<STRUCT<
        item_id:        STRING,
        item_name:      STRING,
        quantity:       INT,
        unit_price:     DOUBLE,
        category:       STRING,
        is_veg:         BOOLEAN
    >>,
    delivery_address    STRUCT<
        lat:            DOUBLE,
        lng:            DOUBLE,
        pincode:        STRING,
        locality:       STRING
    >,
    rider_id            STRING,
    restaurant_lat      DOUBLE,
    restaurant_lng      DOUBLE,
    estimated_delivery_minutes  INT,
    actual_delivery_minutes     INT,
    order_placed_at     TIMESTAMP,
    order_accepted_at   TIMESTAMP,
    order_prepared_at   TIMESTAMP,
    order_picked_up_at  TIMESTAMP,
    order_delivered_at  TIMESTAMP,
    order_cancelled_at  TIMESTAMP,
    cancellation_reason STRING,
    platform            STRING,        -- 'android', 'ios', 'web'
    app_version         STRING,
    device_id           STRING,
    session_id          STRING,
    kafka_offset        BIGINT,
    kafka_partition     INT,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP,
    dt                  STRING
)
WITH (
    table_type   = 'ICEBERG',
    format       = 'ORC',
    location     = 's3://zomato-data-lake-prod/raw/orders/',
    is_external  = false
);


-- ============================================================================
-- 2. USERS RAW
-- Source: Kafka topic 'user_events' + CDC from users DB
-- ============================================================================
CREATE TABLE IF NOT EXISTS zomato_raw.users_raw (
    user_id             STRING,
    email_hash          STRING,        -- SHA-256 hashed for PII compliance
    phone_hash          STRING,
    full_name_encrypted STRING,        -- AES-256 encrypted
    signup_date         DATE,
    city_id             INT,
    city_name           STRING,
    pincode             STRING,
    locality            STRING,
    lat                 DOUBLE,
    lng                 DOUBLE,
    gender              STRING,
    age_bucket          STRING,        -- '18-24', '25-34', '35-44', '45+'
    is_pro_member       BOOLEAN,
    pro_membership_start DATE,
    pro_membership_end  DATE,
    preferred_language  STRING,
    preferred_cuisine   ARRAY<STRING>,
    device_type         STRING,        -- 'android', 'ios'
    app_install_date    DATE,
    last_active_at      TIMESTAMP,
    total_orders        INT,
    total_gmv           DOUBLE,
    avg_order_value     DOUBLE,
    lifetime_discount   DOUBLE,
    referral_code       STRING,
    referred_by         STRING,
    account_status      STRING,        -- 'active', 'suspended', 'deleted'
    cdc_operation       STRING,        -- 'INSERT', 'UPDATE', 'DELETE'
    cdc_timestamp       TIMESTAMP,
    kafka_offset        BIGINT,
    kafka_partition     INT,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP,
    dt                  STRING
)
WITH (
    table_type   = 'ICEBERG',
    format       = 'ORC',
    location     = 's3://zomato-data-lake-prod/raw/users/',
    is_external  = false
);


-- ============================================================================
-- 3. RESTAURANTS RAW
-- Source: Kafka topic 'restaurant_events' + CDC from restaurants DB
-- ============================================================================
CREATE TABLE IF NOT EXISTS zomato_raw.restaurants_raw (
    restaurant_id       STRING,
    restaurant_name     STRING,
    restaurant_type     STRING,        -- 'restaurant', 'cloud_kitchen', 'qsr', 'cafe'
    cuisine_tags        ARRAY<STRING>, -- ['north_indian', 'chinese', 'biryani']
    city_id             INT,
    city_name           STRING,
    zone_id             INT,
    zone_name           STRING,
    locality            STRING,
    pincode             STRING,
    lat                 DOUBLE,
    lng                 DOUBLE,
    owner_id            STRING,
    is_active           BOOLEAN,
    is_accepting_orders BOOLEAN,
    avg_prep_time_min   INT,
    avg_rating          DOUBLE,
    total_ratings       INT,
    price_range         INT,        -- 1 (budget) to 4 (premium)
    is_pro_enabled      BOOLEAN,
    commission_rate     DOUBLE,
    opening_hours       STRING,        -- JSON string of operating hours
    fssai_license       STRING,
    gst_number          STRING,
    onboarding_date     DATE,
    last_order_at       TIMESTAMP,
    menu_item_count     INT,
    images_count        INT,
    has_delivery        BOOLEAN,
    has_pickup          BOOLEAN,
    has_dine_in         BOOLEAN,
    cdc_operation       STRING,
    cdc_timestamp       TIMESTAMP,
    kafka_offset        BIGINT,
    kafka_partition     INT,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP,
    dt                  STRING
)
WITH (
    table_type   = 'ICEBERG',
    format       = 'ORC',
    location     = 's3://zomato-data-lake-prod/raw/restaurants/',
    is_external  = false
);


-- ============================================================================
-- 4. MENU RAW
-- Source: Kafka topic 'menu_updates' + CDC from menu DB
-- ============================================================================
CREATE TABLE IF NOT EXISTS zomato_raw.menu_raw (
    menu_item_id        STRING,
    restaurant_id       STRING,
    item_name           STRING,
    item_description    STRING,
    category            STRING,        -- 'starters', 'mains', 'desserts', 'beverages'
    sub_category        STRING,
    cuisine_type        STRING,
    is_veg              BOOLEAN,
    is_egg              BOOLEAN,
    is_vegan            BOOLEAN,
    price               DOUBLE,
    discounted_price    DOUBLE,
    currency            STRING,
    is_available        BOOLEAN,
    is_recommended      BOOLEAN,
    is_bestseller       BOOLEAN,
    spice_level         INT,        -- 0-3
    serves              INT,        -- number of people
    prep_time_min       INT,
    calories            INT,
    allergens           ARRAY<STRING>,
    tags                ARRAY<STRING>, -- ['must_try', 'new', 'chefs_special']
    image_url           STRING,
    display_order       INT,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    cdc_operation       STRING,
    cdc_timestamp       TIMESTAMP,
    kafka_offset        BIGINT,
    kafka_partition     INT,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP,
    dt                  STRING
)
WITH (
    table_type   = 'ICEBERG',
    format       = 'ORC',
    location     = 's3://zomato-data-lake-prod/raw/menu/',
    is_external  = false
);


-- ============================================================================
-- 5. DELIVERIES RAW
-- Source: Kafka topic 'delivery_events' (real-time rider tracking)
-- ============================================================================
CREATE TABLE IF NOT EXISTS zomato_raw.deliveries_raw (
    delivery_id         STRING,
    order_id            STRING,
    rider_id            STRING,
    restaurant_id       STRING,
    customer_id         STRING,
    city_id             INT,
    zone_id             INT,
    delivery_status     STRING,        -- 'assigned', 'en_route_pickup', 'at_restaurant',
                                        -- 'picked_up', 'en_route_delivery', 'delivered', 'failed'
    assignment_type     STRING,        -- 'auto', 'manual', 'reassigned'
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
    failure_reason      STRING,
    delivery_fee        DOUBLE,
    rider_payout        DOUBLE,
    tip_amount          DOUBLE,
    surge_multiplier    DOUBLE,
    is_batched_delivery BOOLEAN,
    batch_id            STRING,
    batch_sequence      INT,
    weather_condition   STRING,
    traffic_level       STRING,        -- 'low', 'moderate', 'heavy'
    rider_rating        INT,
    customer_feedback   STRING,
    gps_trace_s3_path   STRING,        -- S3 path to full GPS trace
    kafka_offset        BIGINT,
    kafka_partition     INT,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP,
    dt                  STRING
)
WITH (
    table_type   = 'ICEBERG',
    format       = 'ORC',
    location     = 's3://zomato-data-lake-prod/raw/deliveries/',
    is_external  = false
);


-- ============================================================================
-- 6. PAYMENTS RAW
-- Source: Kafka topic 'payment_events' (from payment gateway callbacks)
-- ============================================================================
CREATE TABLE IF NOT EXISTS zomato_raw.payments_raw (
    payment_id          STRING,
    order_id            STRING,
    customer_id         STRING,
    payment_method      STRING,        -- 'upi', 'credit_card', 'debit_card', 'net_banking',
                                        -- 'wallet', 'cod', 'emi', 'pay_later'
    payment_gateway     STRING,        -- 'razorpay', 'paytm', 'phonepe', 'stripe'
    gateway_txn_id      STRING,
    payment_status      STRING,        -- 'initiated', 'authorized', 'captured', 'settled',
                                        -- 'failed', 'refund_initiated', 'refunded'
    amount              DOUBLE,
    currency            STRING,
    tax_amount          DOUBLE,
    convenience_fee     DOUBLE,
    gateway_fee         DOUBLE,
    net_amount          DOUBLE,
    refund_amount       DOUBLE,
    refund_reason       STRING,
    bank_name           STRING,
    card_network        STRING,        -- 'visa', 'mastercard', 'rupay', 'amex'
    card_type           STRING,        -- 'credit', 'debit'
    card_last_four      STRING,
    upi_vpa_hash        STRING,        -- hashed VPA for PII compliance
    wallet_type         STRING,        -- 'paytm', 'phonepe', 'amazon_pay'
    emi_tenure          INT,
    emi_bank            STRING,
    is_international    BOOLEAN,
    risk_score          DOUBLE,
    is_flagged          BOOLEAN,
    fraud_check_status  STRING,        -- 'passed', 'review', 'blocked'
    initiated_at        TIMESTAMP,
    authorized_at       TIMESTAMP,
    captured_at         TIMESTAMP,
    settled_at          TIMESTAMP,
    failed_at           TIMESTAMP,
    refunded_at         TIMESTAMP,
    error_code          STRING,
    error_message       STRING,
    retry_count         INT,
    idempotency_key     STRING,
    kafka_offset        BIGINT,
    kafka_partition     INT,
    kafka_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP,
    dt                  STRING
)
WITH (
    table_type   = 'ICEBERG',
    format       = 'ORC',
    location     = 's3://zomato-data-lake-prod/raw/payments/',
    is_external  = false
);


-- ============================================================================
-- Partition Repair Commands
-- NOTE: With Athena Iceberg tables registered in Glue Data Catalog, partition
-- management is automatic. Manual MSCK REPAIR TABLE is not needed for Iceberg.
-- ============================================================================

-- ============================================================================
-- Example Queries (Athena-compatible)
-- ============================================================================
-- Partition pruning: SELECT * FROM zomato_raw.orders_raw WHERE dt = '2026-03-21';
-- Latest snapshot:   SELECT * FROM zomato_raw.users_raw WHERE dt = (SELECT MAX(dt) FROM zomato_raw.users_raw);
-- Row counts:        SELECT dt, COUNT(*) FROM zomato_raw.payments_raw GROUP BY dt ORDER BY dt DESC LIMIT 7;
