-- ============================================================================
-- Zomato Data Platform - Ad-Hoc Analytical Queries
-- Collection of commonly used analytical queries for data science and
-- product analytics teams
-- ============================================================================


-- ============================================================================
-- 1. Real-time order funnel analysis (last 24 hours)
-- ============================================================================
SELECT
    DATE_TRUNC('hour', order_placed_at)              AS hour_bucket,
    COUNT(*)                                          AS orders_placed,
    COUNT(*) FILTER (WHERE order_accepted_at IS NOT NULL) AS orders_accepted,
    COUNT(*) FILTER (WHERE food_ready_at IS NOT NULL)     AS orders_prepared,
    COUNT(*) FILTER (WHERE order_picked_up_at IS NOT NULL) AS orders_picked_up,
    COUNT(*) FILTER (WHERE order_delivered_at IS NOT NULL) AS orders_delivered,
    COUNT(*) FILTER (WHERE order_cancelled_at IS NOT NULL) AS orders_cancelled,
    ROUND(
        COUNT(*) FILTER (WHERE order_delivered_at IS NOT NULL) * 100.0
        / NULLIF(COUNT(*), 0), 2
    )                                                 AS delivery_rate_pct,
    ROUND(AVG(actual_delivery_min), 1)                AS avg_delivery_min
FROM curated.orders_curated
WHERE order_date = CURRENT_DATE
    AND order_placed_at >= NOW() - INTERVAL '24' HOUR
GROUP BY DATE_TRUNC('hour', order_placed_at)
ORDER BY hour_bucket DESC;


-- ============================================================================
-- 2. Restaurant cannibalization analysis
-- Which new restaurants are stealing orders from existing ones in the same zone?
-- ============================================================================
WITH restaurant_first_order AS (
    SELECT
        restaurant_id,
        city_id,
        zone_id,
        MIN(order_date) AS first_order_date
    FROM curated.orders_curated
    WHERE order_date >= CURRENT_DATE - INTERVAL '90' DAY
    GROUP BY restaurant_id, city_id, zone_id
),
new_restaurants AS (
    SELECT restaurant_id, city_id, zone_id, first_order_date
    FROM restaurant_first_order
    WHERE first_order_date >= CURRENT_DATE - INTERVAL '30' DAY
),
zone_orders_before AS (
    SELECT
        o.zone_id,
        o.city_id,
        COUNT(*)        AS orders_before,
        COUNT(DISTINCT o.restaurant_id) AS restaurants_before
    FROM curated.orders_curated o
    INNER JOIN new_restaurants nr ON o.zone_id = nr.zone_id AND o.city_id = nr.city_id
    WHERE o.order_date BETWEEN nr.first_order_date - INTERVAL '30' DAY
                            AND nr.first_order_date - INTERVAL '1' DAY
      AND o.restaurant_id NOT IN (SELECT restaurant_id FROM new_restaurants)
    GROUP BY o.zone_id, o.city_id
),
zone_orders_after AS (
    SELECT
        o.zone_id,
        o.city_id,
        COUNT(*) FILTER (WHERE o.restaurant_id IN (SELECT restaurant_id FROM new_restaurants))
            AS new_restaurant_orders,
        COUNT(*) FILTER (WHERE o.restaurant_id NOT IN (SELECT restaurant_id FROM new_restaurants))
            AS existing_restaurant_orders
    FROM curated.orders_curated o
    INNER JOIN new_restaurants nr ON o.zone_id = nr.zone_id AND o.city_id = nr.city_id
    WHERE o.order_date BETWEEN nr.first_order_date AND nr.first_order_date + INTERVAL '30' DAY
    GROUP BY o.zone_id, o.city_id
)
SELECT
    b.zone_id,
    b.city_id,
    b.orders_before                                     AS existing_orders_before,
    a.existing_restaurant_orders                         AS existing_orders_after,
    a.new_restaurant_orders,
    ROUND(
        (a.existing_restaurant_orders - b.orders_before) * 100.0
        / NULLIF(b.orders_before, 0), 2
    )                                                    AS existing_order_change_pct,
    ROUND(
        a.new_restaurant_orders * 100.0
        / NULLIF(a.existing_restaurant_orders + a.new_restaurant_orders, 0), 2
    )                                                    AS new_restaurant_share_pct
FROM zone_orders_before b
JOIN zone_orders_after a ON b.zone_id = a.zone_id AND b.city_id = a.city_id
ORDER BY new_restaurant_share_pct DESC;


-- ============================================================================
-- 3. Customer reactivation opportunity
-- Dormant users who were previously high-value
-- ============================================================================
SELECT
    u.user_id,
    u.city_name,
    u.lifetime_order_count,
    u.lifetime_gmv,
    u.avg_order_value,
    u.days_since_last_order,
    u.preferred_cuisine,
    u.preferred_payment_method,
    u.churn_probability_30d,
    u.churn_risk_tier,
    u.recommended_intervention,
    u.pro_tier
FROM gold.user_cohorts u
WHERE u.lifecycle_stage = 'churned'
    AND u.lifetime_order_count >= 10
    AND u.lifetime_gmv >= 5000
    AND u.days_since_last_order BETWEEN 30 AND 90
    AND u.snapshot_date = (SELECT MAX(snapshot_date) FROM gold.user_cohorts)
ORDER BY u.estimated_ltv DESC
LIMIT 10000;


-- ============================================================================
-- 4. Peak hour capacity planning
-- Identify zones approaching rider capacity during peak hours
-- ============================================================================
WITH peak_demand AS (
    SELECT
        city_name,
        zone_name,
        order_date,
        order_hour,
        COUNT(*)                                    AS orders_in_hour,
        COUNT(DISTINCT rider_id)                    AS active_riders,
        COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT rider_id), 0) AS orders_per_rider,
        AVG(actual_delivery_min)                    AS avg_delivery_min,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY actual_delivery_min) AS p90_delivery_min
    FROM curated.orders_curated
    WHERE order_date >= CURRENT_DATE - INTERVAL '7' DAY
        AND is_completed = true
        AND is_peak_hour = true
    GROUP BY city_name, zone_name, order_date, order_hour
)
SELECT
    city_name,
    zone_name,
    AVG(orders_in_hour)                             AS avg_peak_orders,
    AVG(active_riders)                              AS avg_peak_riders,
    AVG(orders_per_rider)                           AS avg_utilization,
    MAX(orders_per_rider)                           AS max_utilization,
    AVG(avg_delivery_min)                           AS avg_delivery_min,
    AVG(p90_delivery_min)                           AS avg_p90_delivery,
    CASE
        WHEN AVG(orders_per_rider) >= 4.0 THEN 'CRITICAL - needs more riders'
        WHEN AVG(orders_per_rider) >= 3.0 THEN 'HIGH - approaching capacity'
        WHEN AVG(orders_per_rider) >= 2.0 THEN 'MODERATE'
        ELSE 'LOW'
    END                                             AS capacity_status
FROM peak_demand
GROUP BY city_name, zone_name
HAVING AVG(orders_per_rider) >= 2.0
ORDER BY avg_utilization DESC;


-- ============================================================================
-- 5. Cuisine trend analysis - rising and declining cuisines by city
-- ============================================================================
WITH monthly_cuisine AS (
    SELECT
        city_name,
        cuisine_primary,
        DATE_TRUNC('month', order_date)             AS order_month,
        COUNT(*)                                     AS order_count,
        SUM(total_amount)                            AS gmv,
        COUNT(DISTINCT customer_id)                  AS unique_customers
    FROM curated.orders_curated
    WHERE order_date >= CURRENT_DATE - INTERVAL '6' MONTH
        AND is_completed = true
        AND cuisine_primary IS NOT NULL
    GROUP BY city_name, cuisine_primary, DATE_TRUNC('month', order_date)
),
cuisine_growth AS (
    SELECT
        city_name,
        cuisine_primary,
        FIRST_VALUE(order_count) OVER w              AS earliest_month_orders,
        LAST_VALUE(order_count) OVER w               AS latest_month_orders,
        SUM(order_count) OVER (PARTITION BY city_name, cuisine_primary) AS total_orders,
        SUM(gmv) OVER (PARTITION BY city_name, cuisine_primary) AS total_gmv,
        ROW_NUMBER() OVER w                          AS rn,
        COUNT(*) OVER (PARTITION BY city_name, cuisine_primary) AS month_count
    FROM monthly_cuisine
    WINDOW w AS (
        PARTITION BY city_name, cuisine_primary
        ORDER BY order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)
SELECT
    city_name,
    cuisine_primary,
    total_orders,
    total_gmv,
    earliest_month_orders,
    latest_month_orders,
    ROUND(
        (latest_month_orders - earliest_month_orders) * 100.0
        / NULLIF(earliest_month_orders, 0), 2
    )                                               AS growth_pct,
    CASE
        WHEN latest_month_orders > earliest_month_orders * 1.5 THEN 'RISING'
        WHEN latest_month_orders < earliest_month_orders * 0.7 THEN 'DECLINING'
        ELSE 'STABLE'
    END                                             AS trend
FROM cuisine_growth
WHERE rn = 1
    AND total_orders >= 1000
ORDER BY city_name, growth_pct DESC;


-- ============================================================================
-- 6. Iceberg table maintenance check
-- Monitor table health: file counts, snapshot counts, partition sizes
-- ============================================================================
SELECT
    *
FROM curated."orders_curated$snapshots"
ORDER BY committed_at DESC
LIMIT 20;

SELECT
    partition,
    file_count,
    record_count,
    file_size_in_bytes / (1024*1024*1024) AS size_gb
FROM curated."orders_curated$partitions"
ORDER BY record_count DESC
LIMIT 50;
