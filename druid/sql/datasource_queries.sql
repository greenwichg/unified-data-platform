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
