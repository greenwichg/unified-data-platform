-- ============================================================================
-- Zomato Data Platform - Reporting Query: Daily GMV Report
-- Used by: Finance team, Executive dashboards
-- Schedule: Runs every 30 minutes via Superset scheduled report
-- ============================================================================

-- Daily GMV summary with city breakdown and comparisons
WITH daily_current AS (
    SELECT
        metric_date,
        city_id,
        city_name,
        SUM(total_orders)                           AS total_orders,
        SUM(completed_orders)                       AS completed_orders,
        SUM(gross_merchandise_value)                 AS gmv,
        SUM(net_revenue)                             AS net_revenue,
        SUM(total_discount_given)                    AS total_discounts,
        SUM(total_delivery_fee)                      AS delivery_fees,
        SUM(platform_commission)                     AS commission,
        SUM(unique_customers)                        AS unique_customers,
        SUM(new_customers)                           AS new_customers,
        AVG(avg_order_value)                         AS avg_order_value,
        AVG(avg_delivery_time_min)                   AS avg_delivery_time,
        SUM(completed_orders) * 1.0
            / NULLIF(SUM(total_orders), 0)           AS completion_rate
    FROM zomato_gold.daily_order_metrics
    WHERE metric_date = CURRENT_DATE
    GROUP BY metric_date, city_id, city_name
),
daily_previous AS (
    SELECT
        city_id,
        SUM(gross_merchandise_value)    AS gmv_prev,
        SUM(total_orders)               AS orders_prev,
        SUM(net_revenue)                AS revenue_prev,
        SUM(unique_customers)           AS customers_prev
    FROM zomato_gold.daily_order_metrics
    WHERE metric_date = CURRENT_DATE - INTERVAL '1' DAY
    GROUP BY city_id
),
weekly_avg AS (
    SELECT
        city_id,
        AVG(gross_merchandise_value)    AS gmv_weekly_avg,
        AVG(total_orders)               AS orders_weekly_avg
    FROM (
        SELECT
            city_id,
            metric_date,
            SUM(gross_merchandise_value) AS gross_merchandise_value,
            SUM(total_orders)            AS total_orders
        FROM zomato_gold.daily_order_metrics
        WHERE metric_date BETWEEN CURRENT_DATE - INTERVAL '7' DAY AND CURRENT_DATE - INTERVAL '1' DAY
        GROUP BY city_id, metric_date
    ) t
    GROUP BY city_id
)
SELECT
    c.metric_date,
    c.city_name,
    c.total_orders,
    c.completed_orders,
    c.gmv,
    c.net_revenue,
    c.total_discounts,
    c.avg_order_value,
    c.unique_customers,
    c.new_customers,
    c.completion_rate,
    c.avg_delivery_time,

    -- Day-over-day change
    ROUND((c.gmv - p.gmv_prev) / NULLIF(p.gmv_prev, 0) * 100, 2)
        AS gmv_dod_change_pct,
    ROUND((c.total_orders - p.orders_prev) * 1.0 / NULLIF(p.orders_prev, 0) * 100, 2)
        AS orders_dod_change_pct,

    -- vs 7-day average
    ROUND((c.gmv - w.gmv_weekly_avg) / NULLIF(w.gmv_weekly_avg, 0) * 100, 2)
        AS gmv_vs_weekly_avg_pct,

    -- Discount burn rate
    ROUND(c.total_discounts / NULLIF(c.gmv, 0) * 100, 2)
        AS discount_burn_rate_pct,

    -- Unit economics
    ROUND(c.net_revenue / NULLIF(c.completed_orders, 0), 2)
        AS revenue_per_order,
    ROUND(c.commission / NULLIF(c.completed_orders, 0), 2)
        AS commission_per_order

FROM daily_current c
LEFT JOIN daily_previous p ON c.city_id = p.city_id
LEFT JOIN weekly_avg w ON c.city_id = w.city_id
ORDER BY c.gmv DESC;

-- National aggregate
SELECT
    metric_date,
    'ALL_INDIA'                                     AS scope,
    SUM(total_orders)                               AS total_orders,
    SUM(completed_orders)                           AS completed_orders,
    SUM(gross_merchandise_value)                     AS total_gmv,
    SUM(net_revenue)                                 AS total_net_revenue,
    SUM(total_discount_given)                        AS total_discounts,
    SUM(unique_customers)                            AS total_unique_customers,
    SUM(new_customers)                               AS total_new_customers,
    SUM(pro_member_gmv)                              AS pro_member_gmv,
    ROUND(SUM(pro_member_gmv) / NULLIF(SUM(gross_merchandise_value), 0) * 100, 2)
        AS pro_gmv_share_pct
FROM zomato_gold.daily_order_metrics
WHERE metric_date = CURRENT_DATE
GROUP BY metric_date;
