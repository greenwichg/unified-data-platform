-- ============================================================================
-- Zomato Data Platform - Sample Druid SQL Queries for Analytics
-- Datasources: zomato_orders, zomato_realtime_events
-- These queries are used by dashboards, alerting, and ad-hoc analysis.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Real-time Orders Dashboard: Orders per minute by city (last 1 hour)
-- ---------------------------------------------------------------------------
SELECT
  FLOOR(__time TO MINUTE) AS minute_bucket,
  city,
  SUM("count") AS order_count,
  SUM(total_revenue) AS revenue,
  SUM(total_revenue) / SUM("count") AS avg_order_value,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_users)) AS distinct_users
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY 1, 2
ORDER BY 1 DESC, 4 DESC;

-- ---------------------------------------------------------------------------
-- 2. Delivery Performance: Average delivery time by city and zone (today)
-- ---------------------------------------------------------------------------
SELECT
  city,
  zone,
  SUM("count") AS total_orders,
  SUM(sum_delivery_time) / SUM("count") AS avg_delivery_time_min,
  MIN(min_delivery_time) AS fastest_delivery_min,
  MAX(max_delivery_time) AS slowest_delivery_min,
  SUM(total_revenue) AS total_revenue
FROM zomato_orders
WHERE __time >= CURRENT_DATE
  AND order_status = 'delivered'
GROUP BY city, zone
HAVING SUM("count") > 10
ORDER BY avg_delivery_time_min DESC;

-- ---------------------------------------------------------------------------
-- 3. Revenue Trend: Hourly revenue breakdown by payment method (last 24h)
-- ---------------------------------------------------------------------------
SELECT
  FLOOR(__time TO HOUR) AS hour_bucket,
  payment_method,
  SUM(total_revenue) AS revenue,
  SUM("count") AS order_count,
  SUM(total_discounts) AS discount_given,
  SUM(total_delivery_fees) AS delivery_fee_collected
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

-- ---------------------------------------------------------------------------
-- 4. Pro User Analysis: Pro vs non-pro user order metrics (last 7 days)
-- ---------------------------------------------------------------------------
SELECT
  is_pro_user,
  platform,
  SUM("count") AS total_orders,
  SUM(total_revenue) AS total_revenue,
  SUM(total_revenue) / SUM("count") AS avg_order_value,
  SUM(total_discounts) / SUM("count") AS avg_discount_per_order,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_users)) AS distinct_users
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY is_pro_user, platform
ORDER BY total_revenue DESC;

-- ---------------------------------------------------------------------------
-- 5. Cuisine Popularity: Top cuisines by city (last 7 days)
-- ---------------------------------------------------------------------------
SELECT
  city,
  cuisine_type,
  SUM("count") AS order_count,
  SUM(total_revenue) AS revenue,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_restaurants)) AS active_restaurants,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_users)) AS ordering_users
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
  AND cuisine_type IS NOT NULL
GROUP BY city, cuisine_type
ORDER BY city, order_count DESC;

-- ---------------------------------------------------------------------------
-- 6. Real-time Event Funnel: App engagement funnel (last 1 hour)
-- ---------------------------------------------------------------------------
SELECT
  event_type,
  platform,
  SUM("count") AS event_count,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_users)) AS distinct_users,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_sessions)) AS distinct_sessions
FROM zomato_realtime_events
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
  AND event_type IN ('app_open', 'search', 'restaurant_view', 'add_to_cart', 'checkout', 'order_placed')
GROUP BY event_type, platform
ORDER BY event_count DESC;

-- ---------------------------------------------------------------------------
-- 7. Search Analytics: Top search queries by city (last 24 hours)
-- ---------------------------------------------------------------------------
SELECT
  city,
  search_query,
  SUM("count") AS search_count,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_users)) AS distinct_searchers
FROM zomato_realtime_events
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
  AND event_type = 'search'
  AND search_query IS NOT NULL
GROUP BY city, search_query
ORDER BY search_count DESC
LIMIT 100;

-- ---------------------------------------------------------------------------
-- 8. Spike Detection: Minute-level order counts for anomaly detection
-- ---------------------------------------------------------------------------
SELECT
  FLOOR(__time TO MINUTE) AS minute_bucket,
  city,
  SUM("count") AS order_count,
  SUM(total_revenue) AS revenue
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '30' MINUTE
GROUP BY 1, 2
HAVING SUM("count") > 0
ORDER BY 1 DESC, 3 DESC;

-- ---------------------------------------------------------------------------
-- 9. Platform Distribution: Device and app version breakdown (last 7 days)
-- ---------------------------------------------------------------------------
SELECT
  platform,
  app_version,
  os_version,
  SUM("count") AS event_count,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_users)) AS distinct_users
FROM zomato_realtime_events
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY platform, app_version, os_version
ORDER BY distinct_users DESC
LIMIT 50;

-- ---------------------------------------------------------------------------
-- 10. Order Failure Monitoring: Failed/cancelled orders by city (last 6h)
-- ---------------------------------------------------------------------------
SELECT
  FLOOR(__time TO 10 MINUTE) AS time_bucket,
  city,
  order_status,
  SUM("count") AS order_count,
  SUM(total_revenue) AS lost_revenue
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '6' HOUR
  AND order_status IN ('cancelled', 'failed', 'refunded')
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC;


-- ===========================================================================
-- ADDITIONAL PRODUCTION ANALYTICS QUERIES
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 11. Order Trends: Daily order volume and GMV trend with WoW comparison
--     Used by: Executive dashboard, weekly business review
-- ---------------------------------------------------------------------------
SELECT
  FLOOR(__time TO DAY) AS day_bucket,
  city,
  SUM("count") AS order_count,
  SUM(total_revenue) AS gmv,
  SUM(total_revenue) / SUM("count") AS avg_order_value,
  SUM(total_discounts) AS total_discounts,
  SUM(total_delivery_fees) AS delivery_fees,
  SUM(total_revenue) - SUM(total_discounts) AS net_revenue,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_users)) AS distinct_customers,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_restaurants)) AS active_restaurants
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '14' DAY
GROUP BY 1, 2
ORDER BY 1 DESC, 4 DESC;

-- ---------------------------------------------------------------------------
-- 12. Restaurant Performance Scorecard: Top/bottom restaurants by metrics
--     Used by: Restaurant partner ops team, merchant quality monitoring
-- ---------------------------------------------------------------------------
SELECT
  city,
  restaurant_id,
  restaurant_name,
  cuisine_type,
  SUM("count") AS total_orders,
  SUM(total_revenue) AS total_gmv,
  SUM(total_revenue) / SUM("count") AS avg_order_value,
  SUM(sum_delivery_time) / SUM("count") AS avg_delivery_time_min,
  SUM(CASE WHEN order_status = 'cancelled' THEN "count" ELSE 0 END) AS cancelled_orders,
  CAST(SUM(CASE WHEN order_status = 'cancelled' THEN "count" ELSE 0 END) AS DOUBLE)
    / SUM("count") * 100 AS cancellation_rate_pct,
  SUM(sum_rating) / NULLIF(SUM(rating_count), 0) AS avg_rating,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_users)) AS distinct_customers
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
GROUP BY city, restaurant_id, restaurant_name, cuisine_type
HAVING SUM("count") >= 50
ORDER BY total_gmv DESC
LIMIT 500;

-- ---------------------------------------------------------------------------
-- 13. Delivery Metrics Deep Dive: P50/P90/P99 delivery times by zone
--     Used by: Logistics/rider ops team, delivery SLA monitoring
-- ---------------------------------------------------------------------------
SELECT
  FLOOR(__time TO HOUR) AS hour_bucket,
  city,
  zone,
  SUM("count") AS delivered_orders,
  SUM(sum_delivery_time) / SUM("count") AS avg_delivery_min,
  DS_QUANTILES_SKETCH(delivery_time_sketch, 0.5) AS p50_delivery_min,
  DS_QUANTILES_SKETCH(delivery_time_sketch, 0.9) AS p90_delivery_min,
  DS_QUANTILES_SKETCH(delivery_time_sketch, 0.99) AS p99_delivery_min,
  SUM(CASE WHEN is_late_delivery = 1 THEN "count" ELSE 0 END) AS late_deliveries,
  CAST(SUM(CASE WHEN is_late_delivery = 1 THEN "count" ELSE 0 END) AS DOUBLE)
    / SUM("count") * 100 AS late_delivery_pct,
  SUM(sum_rider_distance_km) / SUM("count") AS avg_rider_distance_km
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
  AND order_status = 'delivered'
GROUP BY 1, 2, 3
ORDER BY 1 DESC, late_delivery_pct DESC;

-- ---------------------------------------------------------------------------
-- 14. User Behavior Cohort Analysis: New vs returning users by week
--     Used by: Growth team, user retention dashboards
-- ---------------------------------------------------------------------------
SELECT
  FLOOR(__time TO DAY) AS day_bucket,
  city,
  platform,
  user_cohort,
  SUM("count") AS order_count,
  SUM(total_revenue) AS revenue,
  SUM(total_revenue) / SUM("count") AS avg_order_value,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_users)) AS distinct_users,
  SUM(total_discounts) / SUM("count") AS avg_discount_per_order,
  SUM(CASE WHEN is_repeat_order = 1 THEN "count" ELSE 0 END) AS repeat_orders,
  CAST(SUM(CASE WHEN is_repeat_order = 1 THEN "count" ELSE 0 END) AS DOUBLE)
    / SUM("count") * 100 AS repeat_order_pct
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 6 DESC;

-- ---------------------------------------------------------------------------
-- 15. Promo/Coupon Effectiveness: Discount ROI by promo code (last 7 days)
--     Used by: Marketing team, promo budget optimization
-- ---------------------------------------------------------------------------
SELECT
  promo_code,
  promo_type,
  city,
  SUM("count") AS orders_with_promo,
  SUM(total_revenue) AS gross_gmv,
  SUM(total_discounts) AS total_discount_cost,
  SUM(total_revenue) - SUM(total_discounts) AS net_revenue,
  SUM(total_revenue) / SUM(total_discounts) AS revenue_per_discount_dollar,
  SUM(total_revenue) / SUM("count") AS avg_order_value,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_users)) AS promo_users,
  SUM(CASE WHEN is_first_order = 1 THEN "count" ELSE 0 END) AS new_user_orders,
  CAST(SUM(CASE WHEN is_first_order = 1 THEN "count" ELSE 0 END) AS DOUBLE)
    / SUM("count") * 100 AS new_user_acquisition_pct
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
  AND promo_code IS NOT NULL
GROUP BY promo_code, promo_type, city
HAVING SUM("count") >= 10
ORDER BY total_discount_cost DESC
LIMIT 200;

-- ---------------------------------------------------------------------------
-- 16. Peak Hour Capacity Planning: Hourly order heatmap by city and day-of-week
--     Used by: Operations team, rider supply planning
-- ---------------------------------------------------------------------------
SELECT
  TIME_EXTRACT(__time, 'DOW') AS day_of_week,
  TIME_EXTRACT(__time, 'HOUR') AS hour_of_day,
  city,
  SUM("count") AS order_count,
  SUM(total_revenue) AS revenue,
  SUM(sum_delivery_time) / SUM("count") AS avg_delivery_time_min,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_riders)) AS active_riders,
  SUM("count") / THETA_SKETCH_ESTIMATE(DS_THETA(unique_riders)) AS orders_per_rider
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '28' DAY
  AND order_status = 'delivered'
GROUP BY 1, 2, 3
ORDER BY 3, 1, 2;

-- ---------------------------------------------------------------------------
-- 17. Cart Abandonment Funnel: Conversion rates through checkout stages
--     Used by: Product team, checkout UX optimization
-- ---------------------------------------------------------------------------
SELECT
  FLOOR(__time TO HOUR) AS hour_bucket,
  city,
  platform,
  SUM(CASE WHEN event_type = 'restaurant_view' THEN "count" ELSE 0 END) AS restaurant_views,
  SUM(CASE WHEN event_type = 'add_to_cart' THEN "count" ELSE 0 END) AS add_to_carts,
  SUM(CASE WHEN event_type = 'checkout' THEN "count" ELSE 0 END) AS checkouts,
  SUM(CASE WHEN event_type = 'payment_initiated' THEN "count" ELSE 0 END) AS payments_initiated,
  SUM(CASE WHEN event_type = 'order_placed' THEN "count" ELSE 0 END) AS orders_placed,
  CAST(SUM(CASE WHEN event_type = 'add_to_cart' THEN "count" ELSE 0 END) AS DOUBLE)
    / NULLIF(SUM(CASE WHEN event_type = 'restaurant_view' THEN "count" ELSE 0 END), 0) * 100 AS view_to_cart_pct,
  CAST(SUM(CASE WHEN event_type = 'order_placed' THEN "count" ELSE 0 END) AS DOUBLE)
    / NULLIF(SUM(CASE WHEN event_type = 'checkout' THEN "count" ELSE 0 END), 0) * 100 AS checkout_to_order_pct,
  CAST(SUM(CASE WHEN event_type = 'order_placed' THEN "count" ELSE 0 END) AS DOUBLE)
    / NULLIF(SUM(CASE WHEN event_type = 'restaurant_view' THEN "count" ELSE 0 END), 0) * 100 AS overall_conversion_pct
FROM zomato_realtime_events
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
  AND event_type IN ('restaurant_view', 'add_to_cart', 'checkout', 'payment_initiated', 'order_placed')
GROUP BY 1, 2, 3
ORDER BY 1 DESC, overall_conversion_pct DESC;

-- ---------------------------------------------------------------------------
-- 18. Restaurant Supply Health: Restaurants going online/offline over time
--     Used by: Merchant ops, supply health monitoring alerts
-- ---------------------------------------------------------------------------
SELECT
  FLOOR(__time TO 15 MINUTE) AS time_bucket,
  city,
  zone,
  SUM(CASE WHEN event_type = 'restaurant_online' THEN "count" ELSE 0 END) AS went_online,
  SUM(CASE WHEN event_type = 'restaurant_offline' THEN "count" ELSE 0 END) AS went_offline,
  SUM(CASE WHEN event_type = 'restaurant_online' THEN "count" ELSE 0 END)
    - SUM(CASE WHEN event_type = 'restaurant_offline' THEN "count" ELSE 0 END) AS net_change,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_restaurants)) AS affected_restaurants
FROM zomato_realtime_events
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '6' HOUR
  AND event_type IN ('restaurant_online', 'restaurant_offline', 'restaurant_busy', 'restaurant_paused')
GROUP BY 1, 2, 3
ORDER BY 1 DESC, net_change ASC;

-- ---------------------------------------------------------------------------
-- 19. Payment Method Trends: Payment split and failure rates
--     Used by: Payments team, finance reconciliation
-- ---------------------------------------------------------------------------
SELECT
  FLOOR(__time TO HOUR) AS hour_bucket,
  city,
  payment_method,
  payment_gateway,
  SUM("count") AS total_attempts,
  SUM(CASE WHEN payment_status = 'success' THEN "count" ELSE 0 END) AS successful_payments,
  SUM(CASE WHEN payment_status = 'failed' THEN "count" ELSE 0 END) AS failed_payments,
  CAST(SUM(CASE WHEN payment_status = 'failed' THEN "count" ELSE 0 END) AS DOUBLE)
    / SUM("count") * 100 AS failure_rate_pct,
  SUM(total_revenue) AS total_amount_processed,
  SUM(total_revenue) / SUM("count") AS avg_transaction_value
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, failure_rate_pct DESC;

-- ---------------------------------------------------------------------------
-- 20. Rider Utilization & Earnings: Rider performance metrics by zone
--     Used by: Rider ops, fleet management, earnings transparency
-- ---------------------------------------------------------------------------
SELECT
  FLOOR(__time TO HOUR) AS hour_bucket,
  city,
  zone,
  THETA_SKETCH_ESTIMATE(DS_THETA(unique_riders)) AS active_riders,
  SUM("count") AS deliveries_completed,
  SUM("count") / THETA_SKETCH_ESTIMATE(DS_THETA(unique_riders)) AS deliveries_per_rider,
  SUM(sum_rider_earnings) / THETA_SKETCH_ESTIMATE(DS_THETA(unique_riders)) AS avg_hourly_earnings,
  SUM(sum_rider_distance_km) / SUM("count") AS avg_distance_per_delivery_km,
  SUM(sum_delivery_time) / SUM("count") AS avg_time_per_delivery_min,
  SUM(sum_rider_idle_time) / THETA_SKETCH_ESTIMATE(DS_THETA(unique_riders)) AS avg_idle_time_min,
  SUM(sum_rider_tip) / SUM("count") AS avg_tip_per_order
FROM zomato_orders
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
  AND order_status = 'delivered'
GROUP BY 1, 2, 3
HAVING THETA_SKETCH_ESTIMATE(DS_THETA(unique_riders)) > 0
ORDER BY 1 DESC, deliveries_per_rider DESC;
