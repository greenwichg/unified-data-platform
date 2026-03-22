-- ============================================================================
-- Zomato Data Platform - Reporting Query: Restaurant Performance Analytics
-- Used by: Restaurant Ops team, Partner Analytics Portal, Account Managers
-- Schedule: Refreshed every 2 hours via Superset scheduled report
-- ============================================================================

-- Restaurant performance scorecard with peer benchmarking
WITH restaurant_current AS (
    SELECT
        r.restaurant_id,
        r.restaurant_name,
        r.city_id,
        r.city_name,
        r.zone_name,
        r.cuisine_primary,
        r.price_segment,
        r.is_pro_partner,
        r.is_chain,
        r.overall_rating,
        r.food_rating,
        r.total_orders_30d,
        r.completed_orders_30d,
        r.cancelled_orders_30d,
        r.rejected_orders_30d,
        r.completion_rate_30d,
        r.rejection_rate_30d,
        r.gmv_30d,
        r.avg_order_value_30d,
        r.unique_customers_30d,
        r.new_customers_30d,
        r.repeat_customer_rate_30d,
        r.avg_preparation_time_30d,
        r.p90_preparation_time_30d,
        r.sla_breach_rate_30d,
        r.menu_availability_rate,
        r.availability_rate_30d,
        r.city_rank_by_orders,
        r.city_rank_by_rating,
        r.city_rank_by_gmv
    FROM zomato_curated.restaurant_curated r
    WHERE r.snapshot_date = CURRENT_DATE
),
city_benchmarks AS (
    SELECT
        city_id,
        cuisine_primary,
        price_segment,
        AVG(total_orders_30d)           AS avg_orders_30d,
        AVG(gmv_30d)                    AS avg_gmv_30d,
        AVG(completion_rate_30d)        AS avg_completion_rate,
        AVG(avg_preparation_time_30d)   AS avg_prep_time,
        AVG(overall_rating)             AS avg_rating,
        AVG(repeat_customer_rate_30d)   AS avg_repeat_rate,
        AVG(sla_breach_rate_30d)        AS avg_sla_breach_rate,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_orders_30d)
                                        AS p75_orders_30d,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY gmv_30d)
                                        AS p75_gmv_30d
    FROM zomato_curated.restaurant_curated
    WHERE snapshot_date = CURRENT_DATE
      AND is_active = true
    GROUP BY city_id, cuisine_primary, price_segment
),
daily_trend AS (
    SELECT
        restaurant_id,
        metric_date,
        total_orders,
        completed_orders,
        gross_merchandise_value,
        avg_order_value,
        avg_order_rating,
        sla_breach_rate,
        unique_customers,
        LAG(gross_merchandise_value, 7) OVER (
            PARTITION BY restaurant_id ORDER BY metric_date
        ) AS gmv_7d_ago,
        LAG(total_orders, 7) OVER (
            PARTITION BY restaurant_id ORDER BY metric_date
        ) AS orders_7d_ago
    FROM zomato_gold.restaurant_performance
    WHERE metric_date >= CURRENT_DATE - INTERVAL '30' DAY
)
SELECT
    rc.restaurant_id,
    rc.restaurant_name,
    rc.city_name,
    rc.zone_name,
    rc.cuisine_primary,
    rc.price_segment,
    rc.is_pro_partner,

    -- Current performance
    rc.total_orders_30d,
    rc.completed_orders_30d,
    rc.gmv_30d,
    rc.avg_order_value_30d,
    rc.overall_rating,
    rc.food_rating,
    rc.completion_rate_30d,
    rc.rejection_rate_30d,
    rc.repeat_customer_rate_30d,

    -- Delivery performance
    rc.avg_preparation_time_30d,
    rc.p90_preparation_time_30d,
    rc.sla_breach_rate_30d,
    rc.menu_availability_rate,
    rc.availability_rate_30d,

    -- Rankings
    rc.city_rank_by_orders,
    rc.city_rank_by_rating,
    rc.city_rank_by_gmv,

    -- vs Peer benchmarks (same city, cuisine, price segment)
    ROUND((rc.total_orders_30d - cb.avg_orders_30d) / NULLIF(cb.avg_orders_30d, 0) * 100, 1)
        AS orders_vs_peer_pct,
    ROUND((rc.gmv_30d - cb.avg_gmv_30d) / NULLIF(cb.avg_gmv_30d, 0) * 100, 1)
        AS gmv_vs_peer_pct,
    ROUND(rc.overall_rating - cb.avg_rating, 2)
        AS rating_vs_peer,
    ROUND(rc.completion_rate_30d - cb.avg_completion_rate, 2)
        AS completion_vs_peer,
    ROUND(rc.avg_preparation_time_30d - cb.avg_prep_time, 1)
        AS prep_time_vs_peer_min,

    -- Week-over-week trend (latest day vs 7 days ago)
    ROUND((dt_latest.gross_merchandise_value - dt_latest.gmv_7d_ago)
        / NULLIF(dt_latest.gmv_7d_ago, 0) * 100, 1)
        AS gmv_wow_change_pct,
    ROUND((dt_latest.total_orders - dt_latest.orders_7d_ago)
        / NULLIF(CAST(dt_latest.orders_7d_ago AS DOUBLE), 0) * 100, 1)
        AS orders_wow_change_pct,

    -- Health score (composite)
    ROUND(
        (COALESCE(rc.completion_rate_30d, 0) * 25) +
        (LEAST(rc.overall_rating / 5.0, 1.0) * 25) +
        (COALESCE(rc.repeat_customer_rate_30d, 0) * 25) +
        ((1.0 - COALESCE(rc.sla_breach_rate_30d, 0)) * 25)
    , 1) AS health_score,

    -- Actionable flags
    CASE
        WHEN rc.sla_breach_rate_30d > 0.15 THEN 'HIGH_SLA_BREACH'
        WHEN rc.rejection_rate_30d > 0.10 THEN 'HIGH_REJECTION'
        WHEN rc.overall_rating < 3.5 THEN 'LOW_RATING'
        WHEN rc.availability_rate_30d < 0.70 THEN 'LOW_AVAILABILITY'
        WHEN rc.menu_availability_rate < 0.80 THEN 'MENU_GAPS'
        ELSE 'HEALTHY'
    END AS primary_flag

FROM restaurant_current rc
LEFT JOIN city_benchmarks cb
    ON rc.city_id = cb.city_id
    AND rc.cuisine_primary = cb.cuisine_primary
    AND rc.price_segment = cb.price_segment
LEFT JOIN daily_trend dt_latest
    ON rc.restaurant_id = dt_latest.restaurant_id
    AND dt_latest.metric_date = CURRENT_DATE - INTERVAL '1' DAY
ORDER BY rc.gmv_30d DESC;
