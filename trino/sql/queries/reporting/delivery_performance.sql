-- ============================================================================
-- Zomato Data Platform - Reporting Query: Delivery Performance
-- Used by: Logistics Ops, City Managers, Rider Ops dashboards
-- Schedule: Refreshed every 15 minutes via Superset scheduled report
-- ============================================================================

-- Hourly delivery SLA and rider utilization with city/zone breakdown
WITH current_day_hourly AS (
    SELECT
        metric_date,
        metric_hour,
        city_id,
        city_name,
        zone_id,
        zone_name,
        total_deliveries,
        completed_deliveries,
        failed_deliveries,
        completion_rate,
        avg_total_delivery_time,
        median_total_delivery_time,
        p90_total_delivery_time,
        p99_total_delivery_time,
        avg_accept_time,
        avg_preparation_time,
        avg_pickup_time,
        avg_transit_time,
        sla_met_count,
        sla_breach_count,
        sla_breach_rate,
        sla_breach_by_restaurant,
        sla_breach_by_rider,
        sla_breach_by_demand,
        total_active_riders,
        riders_on_delivery,
        rider_utilization_rate,
        avg_deliveries_per_rider,
        batched_orders,
        batch_rate,
        avg_delivery_rating,
        total_delivery_cost,
        avg_cost_per_delivery,
        surge_pricing_active,
        surge_multiplier_avg,
        rain_affected_deliveries
    FROM gold.delivery_metrics
    WHERE metric_date = CURRENT_DATE
      AND metric_hour IS NOT NULL
),
previous_day AS (
    SELECT
        metric_hour,
        city_id,
        SUM(total_deliveries)           AS deliveries_prev,
        AVG(avg_total_delivery_time)    AS avg_time_prev,
        AVG(sla_breach_rate)            AS sla_breach_rate_prev,
        AVG(rider_utilization_rate)     AS rider_util_prev
    FROM gold.delivery_metrics
    WHERE metric_date = CURRENT_DATE - INTERVAL '1' DAY
      AND metric_hour IS NOT NULL
    GROUP BY metric_hour, city_id
),
weekly_baseline AS (
    SELECT
        metric_hour,
        city_id,
        AVG(total_deliveries)           AS avg_deliveries_weekly,
        AVG(avg_total_delivery_time)    AS avg_time_weekly,
        AVG(sla_breach_rate)            AS avg_sla_breach_weekly,
        AVG(rider_utilization_rate)     AS avg_rider_util_weekly
    FROM (
        SELECT
            metric_hour,
            city_id,
            metric_date,
            SUM(total_deliveries)           AS total_deliveries,
            AVG(avg_total_delivery_time)    AS avg_total_delivery_time,
            AVG(sla_breach_rate)            AS sla_breach_rate,
            AVG(rider_utilization_rate)     AS rider_utilization_rate
        FROM gold.delivery_metrics
        WHERE metric_date BETWEEN CURRENT_DATE - INTERVAL '7' DAY
                               AND CURRENT_DATE - INTERVAL '1' DAY
          AND metric_hour IS NOT NULL
        GROUP BY metric_hour, city_id, metric_date
    ) t
    GROUP BY metric_hour, city_id
)
SELECT
    c.metric_date,
    c.metric_hour,
    c.city_name,
    c.zone_name,

    -- Volume
    c.total_deliveries,
    c.completed_deliveries,
    c.failed_deliveries,
    c.completion_rate,

    -- Delivery time breakdown
    ROUND(c.avg_total_delivery_time, 1)     AS avg_delivery_min,
    ROUND(c.median_total_delivery_time, 1)  AS median_delivery_min,
    ROUND(c.p90_total_delivery_time, 1)     AS p90_delivery_min,
    ROUND(c.p99_total_delivery_time, 1)     AS p99_delivery_min,
    ROUND(c.avg_accept_time, 1)             AS avg_accept_min,
    ROUND(c.avg_preparation_time, 1)        AS avg_prep_min,
    ROUND(c.avg_pickup_time, 1)             AS avg_pickup_min,
    ROUND(c.avg_transit_time, 1)            AS avg_transit_min,

    -- SLA
    c.sla_met_count,
    c.sla_breach_count,
    ROUND(c.sla_breach_rate * 100, 2)       AS sla_breach_pct,
    c.sla_breach_by_restaurant,
    c.sla_breach_by_rider,
    c.sla_breach_by_demand,

    -- SLA breach attribution
    ROUND(c.sla_breach_by_restaurant * 100.0
        / NULLIF(c.sla_breach_count, 0), 1) AS breach_restaurant_pct,
    ROUND(c.sla_breach_by_rider * 100.0
        / NULLIF(c.sla_breach_count, 0), 1) AS breach_rider_pct,
    ROUND(c.sla_breach_by_demand * 100.0
        / NULLIF(c.sla_breach_count, 0), 1) AS breach_demand_pct,

    -- Rider supply
    c.total_active_riders,
    c.riders_on_delivery,
    ROUND(c.rider_utilization_rate * 100, 1)    AS rider_utilization_pct,
    ROUND(c.avg_deliveries_per_rider, 1)        AS avg_deliveries_per_rider,

    -- Batching
    c.batched_orders,
    ROUND(c.batch_rate * 100, 1)            AS batch_rate_pct,

    -- Quality
    ROUND(c.avg_delivery_rating, 2)         AS avg_delivery_rating,

    -- Cost
    ROUND(c.avg_cost_per_delivery, 2)       AS cost_per_delivery,

    -- External factors
    c.surge_pricing_active,
    ROUND(c.surge_multiplier_avg, 2)        AS surge_multiplier,
    c.rain_affected_deliveries,

    -- Day-over-day comparison
    ROUND((c.total_deliveries - p.deliveries_prev) * 100.0
        / NULLIF(p.deliveries_prev, 0), 1)      AS deliveries_dod_pct,
    ROUND(c.avg_total_delivery_time - p.avg_time_prev, 1)
                                                 AS delivery_time_dod_delta,
    ROUND((c.sla_breach_rate - p.sla_breach_rate_prev) * 100, 2)
                                                 AS sla_breach_dod_delta_pp,

    -- vs Weekly baseline
    ROUND((c.total_deliveries - w.avg_deliveries_weekly) * 100.0
        / NULLIF(w.avg_deliveries_weekly, 0), 1) AS deliveries_vs_weekly_pct,
    ROUND(c.avg_total_delivery_time - w.avg_time_weekly, 1)
                                                  AS delivery_time_vs_weekly_delta,

    -- Alert flags
    CASE
        WHEN c.sla_breach_rate > 0.20 THEN 'CRITICAL_SLA'
        WHEN c.sla_breach_rate > 0.15 THEN 'HIGH_SLA'
        WHEN c.rider_utilization_rate > 0.90 THEN 'RIDER_SHORTAGE'
        WHEN c.rider_utilization_rate < 0.30 THEN 'RIDER_SURPLUS'
        WHEN c.avg_total_delivery_time > 45 THEN 'SLOW_DELIVERY'
        WHEN c.surge_pricing_active AND c.surge_multiplier_avg > 1.5 THEN 'HIGH_SURGE'
        ELSE 'NORMAL'
    END AS ops_alert

FROM current_day_hourly c
LEFT JOIN previous_day p
    ON c.metric_hour = p.metric_hour AND c.city_id = p.city_id
LEFT JOIN weekly_baseline w
    ON c.metric_hour = w.metric_hour AND c.city_id = w.city_id
ORDER BY c.city_name, c.zone_name, c.metric_hour;

-- ============================================================================
-- City-level daily summary (for executive delivery scorecard)
-- ============================================================================
SELECT
    metric_date,
    city_name,
    SUM(total_deliveries)                               AS total_deliveries,
    SUM(completed_deliveries)                           AS completed_deliveries,
    ROUND(AVG(avg_total_delivery_time), 1)              AS avg_delivery_min,
    ROUND(AVG(median_total_delivery_time), 1)           AS median_delivery_min,
    ROUND(AVG(p90_total_delivery_time), 1)              AS p90_delivery_min,
    ROUND(SUM(sla_breach_count) * 100.0
        / NULLIF(SUM(total_deliveries), 0), 2)          AS overall_sla_breach_pct,
    ROUND(AVG(rider_utilization_rate) * 100, 1)         AS avg_rider_utilization_pct,
    ROUND(SUM(total_delivery_cost)
        / NULLIF(SUM(completed_deliveries), 0), 2)      AS cost_per_delivery,
    ROUND(AVG(avg_delivery_rating), 2)                  AS avg_rating,
    SUM(rain_affected_deliveries)                       AS rain_affected
FROM gold.delivery_metrics
WHERE metric_date = CURRENT_DATE
  AND metric_hour IS NOT NULL
GROUP BY metric_date, city_name
ORDER BY total_deliveries DESC;
