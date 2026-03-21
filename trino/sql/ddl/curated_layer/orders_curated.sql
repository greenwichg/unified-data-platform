-- ============================================================================
-- Zomato Data Platform - Curated Layer: Orders
-- Format: Iceberg (Apache Iceberg v2) | Partitioned by: order_date, city_id
-- Deduped, cleaned, enriched orders from raw layer
-- ============================================================================


-- ============================================================================
-- Table Definition
-- ============================================================================
CREATE TABLE IF NOT EXISTS curated.orders_curated (
    order_id                VARCHAR         NOT NULL,
    customer_id             VARCHAR         NOT NULL,
    restaurant_id           VARCHAR         NOT NULL,
    city_id                 INTEGER         NOT NULL,
    city_name               VARCHAR,
    zone_id                 INTEGER,
    zone_name               VARCHAR,
    order_status            VARCHAR,
    is_completed            BOOLEAN,
    is_cancelled            BOOLEAN,
    order_type              VARCHAR,
    payment_method          VARCHAR,
    payment_status          VARCHAR,
    coupon_code             VARCHAR,
    coupon_type             VARCHAR,        -- 'flat', 'percentage', 'freebie', 'bogo'
    subtotal_amount         DECIMAL(12,2),
    tax_amount              DECIMAL(12,2),
    delivery_fee            DECIMAL(12,2),
    discount_amount         DECIMAL(12,2),
    platform_commission     DECIMAL(12,2),
    restaurant_payout       DECIMAL(12,2),
    total_amount            DECIMAL(12,2),
    net_revenue             DECIMAL(12,2),  -- total - discount - restaurant_payout
    currency                VARCHAR,
    item_count              INTEGER,
    unique_item_count       INTEGER,
    has_veg_items           BOOLEAN,
    has_non_veg_items       BOOLEAN,
    cuisine_primary         VARCHAR,
    rider_id                VARCHAR,
    delivery_distance_km    DOUBLE,
    estimated_delivery_min  INTEGER,
    actual_delivery_min     INTEGER,
    delivery_delay_min      INTEGER,        -- actual - estimated
    is_late_delivery        BOOLEAN,
    sla_breach              BOOLEAN,
    order_placed_at         TIMESTAMP(6),
    order_accepted_at       TIMESTAMP(6),
    food_ready_at           TIMESTAMP(6),
    order_picked_up_at      TIMESTAMP(6),
    order_delivered_at       TIMESTAMP(6),
    order_cancelled_at      TIMESTAMP(6),
    cancellation_reason     VARCHAR,
    cancellation_source     VARCHAR,        -- 'customer', 'restaurant', 'system', 'rider'
    accept_to_deliver_min   DOUBLE,         -- end-to-end fulfillment time
    platform                VARCHAR,
    app_version             VARCHAR,
    customer_lat            DOUBLE,
    customer_lng            DOUBLE,
    restaurant_lat          DOUBLE,
    restaurant_lng          DOUBLE,
    is_first_order          BOOLEAN,
    is_pro_member           BOOLEAN,
    customer_lifetime_orders INTEGER,
    restaurant_rating       DOUBLE,
    order_rating            INTEGER,
    order_feedback          VARCHAR,
    order_date              DATE            NOT NULL,
    order_hour              INTEGER,
    is_weekend              BOOLEAN,
    is_peak_hour            BOOLEAN,        -- 12-14, 19-22
    processed_at            TIMESTAMP(6)
)
WITH (
    format           = 'PARQUET',
    location         = 's3a://zomato-data-lake-prod/curated/orders/',
    partitioning     = ARRAY['order_date', 'city_id'],
    sorted_by        = ARRAY['order_placed_at'],
    format_version   = 2
);


-- ============================================================================
-- CTAS / INSERT: Build curated orders with joins, dedup, and quality filters
-- Run incrementally for each day's data
-- ============================================================================
INSERT INTO curated.orders_curated
WITH deduplicated_orders AS (
    -- Deduplicate raw orders: keep the latest record per order_id
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY kafka_timestamp DESC, kafka_offset DESC
        ) AS row_num
    FROM raw.orders_raw
    WHERE dt = CAST(CURRENT_DATE - INTERVAL '1' DAY AS VARCHAR)
      -- Data quality gate: reject records missing critical fields
      AND order_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND restaurant_id IS NOT NULL
      AND total_amount IS NOT NULL
      AND order_placed_at IS NOT NULL
),
clean_orders AS (
    SELECT * FROM deduplicated_orders WHERE row_num = 1
),
restaurant_info AS (
    -- Latest restaurant snapshot for enrichment
    SELECT
        restaurant_id,
        zone_id,
        zone_name,
        avg_rating AS restaurant_rating,
        commission_rate
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY restaurant_id
                ORDER BY cdc_timestamp DESC
            ) AS rn
        FROM raw.restaurants_raw
        WHERE dt >= CAST(CURRENT_DATE - INTERVAL '7' DAY AS VARCHAR)
    )
    WHERE rn = 1
),
user_info AS (
    -- Latest user snapshot for enrichment
    SELECT
        user_id,
        is_pro_member,
        total_orders AS customer_lifetime_orders
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY cdc_timestamp DESC
            ) AS rn
        FROM raw.users_raw
        WHERE dt >= CAST(CURRENT_DATE - INTERVAL '7' DAY AS VARCHAR)
    )
    WHERE rn = 1
),
delivery_info AS (
    -- Delivery distance and details
    SELECT
        order_id,
        total_distance_km AS delivery_distance_km
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY kafka_timestamp DESC
            ) AS rn
        FROM raw.deliveries_raw
        WHERE dt = CAST(CURRENT_DATE - INTERVAL '1' DAY AS VARCHAR)
    )
    WHERE rn = 1
),
payment_info AS (
    -- Settled payment status
    SELECT
        order_id,
        payment_status AS final_payment_status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY kafka_timestamp DESC
            ) AS rn
        FROM raw.payments_raw
        WHERE dt = CAST(CURRENT_DATE - INTERVAL '1' DAY AS VARCHAR)
    )
    WHERE rn = 1
),
first_order_check AS (
    -- Determine if this is the customer's first order ever
    SELECT
        customer_id,
        MIN(order_placed_at) AS first_order_time
    FROM raw.orders_raw
    WHERE dt <= CAST(CURRENT_DATE - INTERVAL '1' DAY AS VARCHAR)
    GROUP BY customer_id
)
SELECT
    -- Order identifiers
    o.order_id,
    o.customer_id,
    o.restaurant_id,
    o.city_id,
    o.city_name,
    r.zone_id,
    r.zone_name,

    -- Order status flags
    o.order_status,
    o.order_status = 'delivered'                    AS is_completed,
    o.order_status IN ('cancelled', 'rejected')     AS is_cancelled,
    o.order_type,
    o.payment_method,
    COALESCE(p.final_payment_status, o.payment_status) AS payment_status,

    -- Coupon details
    o.coupon_code,
    CASE
        WHEN o.coupon_code IS NULL THEN NULL
        WHEN o.discount_amount = 0 THEN NULL
        WHEN o.discount_amount <= 100 THEN 'flat'
        WHEN o.discount_amount / NULLIF(o.subtotal_amount, 0) < 0.5 THEN 'percentage'
        ELSE 'flat'
    END                                             AS coupon_type,

    -- Financial fields (cast to DECIMAL for precision)
    CAST(o.subtotal_amount AS DECIMAL(12,2))        AS subtotal_amount,
    CAST(o.tax_amount AS DECIMAL(12,2))             AS tax_amount,
    CAST(o.delivery_fee AS DECIMAL(12,2))           AS delivery_fee,
    CAST(o.discount_amount AS DECIMAL(12,2))        AS discount_amount,
    CAST(o.total_amount * COALESCE(r.commission_rate, 0.20) AS DECIMAL(12,2))
                                                    AS platform_commission,
    CAST(o.total_amount * (1.0 - COALESCE(r.commission_rate, 0.20)) AS DECIMAL(12,2))
                                                    AS restaurant_payout,
    CAST(o.total_amount AS DECIMAL(12,2))           AS total_amount,
    CAST(
        o.total_amount
        - COALESCE(o.discount_amount, 0)
        - (o.total_amount * (1.0 - COALESCE(r.commission_rate, 0.20)))
        AS DECIMAL(12,2)
    )                                               AS net_revenue,
    COALESCE(o.currency, 'INR')                     AS currency,

    -- Item details
    o.item_count,
    CARDINALITY(
        FILTER(
            TRANSFORM(o.items, x -> x.item_id),
            x -> x IS NOT NULL
        )
    )                                               AS unique_item_count,
    ANY_MATCH(o.items, x -> x.is_veg = TRUE)       AS has_veg_items,
    ANY_MATCH(o.items, x -> x.is_veg = FALSE)      AS has_non_veg_items,
    COALESCE(
        ELEMENT_AT(o.items, 1).category,
        'unknown'
    )                                               AS cuisine_primary,

    -- Delivery details
    o.rider_id,
    d.delivery_distance_km,
    o.estimated_delivery_minutes                    AS estimated_delivery_min,
    o.actual_delivery_minutes                       AS actual_delivery_min,
    o.actual_delivery_minutes - o.estimated_delivery_minutes
                                                    AS delivery_delay_min,
    o.actual_delivery_minutes > o.estimated_delivery_minutes
                                                    AS is_late_delivery,
    o.actual_delivery_minutes > (o.estimated_delivery_minutes * 1.5)
                                                    AS sla_breach,

    -- Timestamps
    o.order_placed_at,
    o.order_accepted_at,
    o.order_prepared_at                             AS food_ready_at,
    o.order_picked_up_at,
    o.order_delivered_at,
    o.order_cancelled_at,
    o.cancellation_reason,
    CASE
        WHEN o.cancellation_reason LIKE '%customer%'    THEN 'customer'
        WHEN o.cancellation_reason LIKE '%restaurant%'  THEN 'restaurant'
        WHEN o.cancellation_reason LIKE '%rider%'       THEN 'rider'
        WHEN o.order_cancelled_at IS NOT NULL            THEN 'system'
        ELSE NULL
    END                                             AS cancellation_source,
    CAST(
        DATE_DIFF('second', o.order_accepted_at, o.order_delivered_at) / 60.0
        AS DOUBLE
    )                                               AS accept_to_deliver_min,

    -- Device and session
    o.platform,
    o.app_version,
    o.delivery_address.lat                          AS customer_lat,
    o.delivery_address.lng                          AS customer_lng,
    o.restaurant_lat,
    o.restaurant_lng,

    -- Customer enrichment
    CAST(o.order_placed_at AS DATE) = CAST(fo.first_order_time AS DATE)
                                                    AS is_first_order,
    COALESCE(u.is_pro_member, FALSE)                AS is_pro_member,
    COALESCE(u.customer_lifetime_orders, 0)         AS customer_lifetime_orders,
    r.restaurant_rating,
    NULL                                            AS order_rating,
    NULL                                            AS order_feedback,

    -- Time dimensions
    CAST(o.order_placed_at AS DATE)                 AS order_date,
    HOUR(o.order_placed_at)                         AS order_hour,
    DAY_OF_WEEK(o.order_placed_at) IN (6, 7)       AS is_weekend,
    HOUR(o.order_placed_at) BETWEEN 12 AND 14
        OR HOUR(o.order_placed_at) BETWEEN 19 AND 22
                                                    AS is_peak_hour,

    -- Processing metadata
    CURRENT_TIMESTAMP                               AS processed_at

FROM clean_orders o
LEFT JOIN restaurant_info r   ON o.restaurant_id = r.restaurant_id
LEFT JOIN user_info u         ON o.customer_id = u.user_id
LEFT JOIN delivery_info d     ON o.order_id = d.order_id
LEFT JOIN payment_info p      ON o.order_id = p.order_id
LEFT JOIN first_order_check fo ON o.customer_id = fo.customer_id

-- Data quality filters: reject implausible records
WHERE o.total_amount > 0
  AND o.total_amount < 100000       -- reject amounts > 1 lakh (likely test orders)
  AND o.item_count > 0
  AND o.item_count <= 100           -- reject unreasonable item counts
  AND o.order_placed_at > TIMESTAMP '2020-01-01 00:00:00'  -- reject future/ancient dates
  AND o.order_placed_at < CURRENT_TIMESTAMP + INTERVAL '1' DAY
;


-- ============================================================================
-- Iceberg Table Maintenance Procedures
-- ============================================================================

-- OPTIMIZE: compact small files produced by streaming ingestion
-- ALTER TABLE curated.orders_curated EXECUTE optimize
--     WHERE order_date >= CURRENT_DATE - INTERVAL '3' DAY;

-- EXPIRE SNAPSHOTS: clean up old metadata (retain 7 days)
-- ALTER TABLE curated.orders_curated EXECUTE expire_snapshots(retention_threshold => '7d');

-- REMOVE ORPHAN FILES
-- ALTER TABLE curated.orders_curated EXECUTE remove_orphan_files(retention_threshold => '7d');

-- ANALYZE for query optimizer statistics
-- ANALYZE curated.orders_curated
--     WHERE order_date >= CURRENT_DATE - INTERVAL '7' DAY;
