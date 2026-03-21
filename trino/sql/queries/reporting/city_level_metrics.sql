-- ============================================================================
-- Zomato Data Platform - Reporting Query: City-Level Metrics
-- Used by: City ops managers, Regional heads, Strategy team
-- Provides city-level KPIs with trends and benchmarking
-- ============================================================================

-- City scorecard with rolling metrics and ranking
WITH city_daily AS (
    SELECT
        city_id,
        city_name,
        metric_date,
        SUM(total_orders)               AS total_orders,
        SUM(completed_orders)           AS completed_orders,
        SUM(cancelled_orders)           AS cancelled_orders,
        SUM(gross_merchandise_value)    AS gmv,
        SUM(net_revenue)                AS net_revenue,
        SUM(total_discount_given)       AS discounts,
        SUM(unique_customers)           AS unique_customers,
        SUM(new_customers)              AS new_customers,
        SUM(active_restaurants)         AS active_restaurants,
        SUM(active_riders)              AS active_riders,
        AVG(avg_delivery_time_min)      AS avg_delivery_time,
        AVG(avg_order_rating)           AS avg_rating,
        SUM(sla_breach_count)           AS sla_breaches,
        SUM(total_orders) * 1.0
            / NULLIF(SUM(active_riders), 0) AS orders_per_rider
    FROM gold.daily_order_metrics
    WHERE metric_date BETWEEN CURRENT_DATE - INTERVAL '30' DAY AND CURRENT_DATE
    GROUP BY city_id, city_name, metric_date
),
city_30d AS (
    SELECT
        city_id,
        city_name,
        SUM(total_orders)               AS orders_30d,
        SUM(gmv)                        AS gmv_30d,
        SUM(net_revenue)                AS revenue_30d,
        SUM(discounts)                  AS discounts_30d,
        SUM(new_customers)              AS new_customers_30d,
        AVG(unique_customers)           AS avg_daily_customers,
        AVG(active_restaurants)         AS avg_active_restaurants,
        AVG(active_riders)              AS avg_active_riders,
        AVG(avg_delivery_time)          AS avg_delivery_time_30d,
        AVG(avg_rating)                 AS avg_rating_30d,
        SUM(sla_breaches) * 1.0
            / NULLIF(SUM(completed_orders), 0)  AS sla_breach_rate_30d,
        AVG(orders_per_rider)           AS avg_orders_per_rider
    FROM city_daily
    GROUP BY city_id, city_name
),
city_7d AS (
    SELECT
        city_id,
        SUM(total_orders)               AS orders_7d,
        SUM(gmv)                        AS gmv_7d,
        AVG(avg_delivery_time)          AS avg_delivery_time_7d
    FROM city_daily
    WHERE metric_date >= CURRENT_DATE - INTERVAL '7' DAY
    GROUP BY city_id
),
city_prev_30d AS (
    SELECT
        city_id,
        SUM(total_orders)               AS orders_prev_30d,
        SUM(gross_merchandise_value)    AS gmv_prev_30d
    FROM gold.daily_order_metrics
    WHERE metric_date BETWEEN CURRENT_DATE - INTERVAL '60' DAY
                          AND CURRENT_DATE - INTERVAL '31' DAY
    GROUP BY city_id
)
SELECT
    c.city_name,

    -- 30-day volume
    c.orders_30d,
    c.gmv_30d,
    c.revenue_30d,
    c.new_customers_30d,
    ROUND(c.avg_daily_customers, 0)                 AS avg_daily_customers,

    -- Week-over-week trend
    ROUND(c7.gmv_7d, 2)                             AS gmv_last_7d,
    ROUND(c7.orders_7d, 0)                           AS orders_last_7d,

    -- Month-over-month growth
    ROUND((c.gmv_30d - cp.gmv_prev_30d)
        / NULLIF(cp.gmv_prev_30d, 0) * 100, 2)      AS gmv_mom_growth_pct,
    ROUND((c.orders_30d - cp.orders_prev_30d)
        / NULLIF(cp.orders_prev_30d, 0) * 100, 2)    AS orders_mom_growth_pct,

    -- Unit economics
    ROUND(c.gmv_30d / NULLIF(c.orders_30d, 0), 2)   AS aov_30d,
    ROUND(c.discounts_30d / NULLIF(c.gmv_30d, 0) * 100, 2)
                                                      AS discount_rate_pct,
    ROUND(c.revenue_30d / NULLIF(c.orders_30d, 0), 2)
                                                      AS revenue_per_order,

    -- Supply health
    ROUND(c.avg_active_restaurants, 0)                AS avg_active_restaurants,
    ROUND(c.avg_active_riders, 0)                     AS avg_active_riders,
    ROUND(c.avg_orders_per_rider, 1)                  AS avg_orders_per_rider,

    -- Quality
    ROUND(c.avg_delivery_time_30d, 1)                 AS avg_delivery_time_min,
    ROUND(c.avg_rating_30d, 2)                        AS avg_rating,
    ROUND(c.sla_breach_rate_30d * 100, 2)             AS sla_breach_rate_pct,

    -- Rankings
    RANK() OVER (ORDER BY c.gmv_30d DESC)             AS rank_by_gmv,
    RANK() OVER (ORDER BY c.orders_30d DESC)          AS rank_by_orders,
    RANK() OVER (ORDER BY c.avg_rating_30d DESC)      AS rank_by_rating,
    RANK() OVER (ORDER BY c.avg_delivery_time_30d ASC) AS rank_by_delivery_speed,

    -- City tier classification
    CASE
        WHEN c.orders_30d >= 5000000   THEN 'Tier 1'
        WHEN c.orders_30d >= 1000000   THEN 'Tier 2'
        WHEN c.orders_30d >= 200000    THEN 'Tier 3'
        ELSE 'Tier 4'
    END                                               AS city_tier

FROM city_30d c
LEFT JOIN city_7d c7 ON c.city_id = c7.city_id
LEFT JOIN city_prev_30d cp ON c.city_id = cp.city_id
ORDER BY c.gmv_30d DESC;
