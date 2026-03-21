-- ============================================================================
-- Zomato Data Platform - Reporting Query: User Retention Analysis
-- Used by: Growth team, Product Analytics, Marketing
-- Schedule: Weekly refresh (Monday 06:00 UTC) via Superset scheduled report
-- ============================================================================

-- Classic cohort retention matrix (monthly cohorts, monthly retention)
WITH cohort_retention AS (
    SELECT
        cohort_month,
        city_name,
        COUNT(DISTINCT user_id)                                         AS cohort_size,
        COUNT(DISTINCT CASE WHEN has_order_m0 THEN user_id END)        AS active_m0,
        COUNT(DISTINCT CASE WHEN has_order_m1 THEN user_id END)        AS active_m1,
        COUNT(DISTINCT CASE WHEN has_order_m2 THEN user_id END)        AS active_m2,
        COUNT(DISTINCT CASE WHEN has_order_m3 THEN user_id END)        AS active_m3,
        COUNT(DISTINCT CASE WHEN has_order_m6 THEN user_id END)        AS active_m6,
        COUNT(DISTINCT CASE WHEN has_order_m12 THEN user_id END)       AS active_m12
    FROM gold.user_cohorts
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) FROM gold.user_cohorts
    )
    GROUP BY cohort_month, city_name
)
SELECT
    cohort_month,
    city_name,
    cohort_size,
    active_m0,
    ROUND(active_m0 * 100.0 / NULLIF(cohort_size, 0), 2)   AS activation_rate_pct,
    ROUND(active_m1 * 100.0 / NULLIF(active_m0, 0), 2)     AS retention_m1_pct,
    ROUND(active_m2 * 100.0 / NULLIF(active_m0, 0), 2)     AS retention_m2_pct,
    ROUND(active_m3 * 100.0 / NULLIF(active_m0, 0), 2)     AS retention_m3_pct,
    ROUND(active_m6 * 100.0 / NULLIF(active_m0, 0), 2)     AS retention_m6_pct,
    ROUND(active_m12 * 100.0 / NULLIF(active_m0, 0), 2)    AS retention_m12_pct
FROM cohort_retention
WHERE cohort_month >= DATE_FORMAT(CURRENT_DATE - INTERVAL '18' MONTH, '%Y-%m')
ORDER BY cohort_month DESC, cohort_size DESC;


-- ============================================================================
-- User segment distribution and LTV analysis
-- ============================================================================
WITH segment_metrics AS (
    SELECT
        city_name,
        user_segment,
        lifecycle_stage,
        COUNT(DISTINCT user_id)                     AS user_count,
        AVG(lifetime_order_count)                   AS avg_lifetime_orders,
        AVG(lifetime_gmv)                           AS avg_lifetime_gmv,
        AVG(estimated_ltv)                          AS avg_estimated_ltv,
        AVG(avg_order_value)                        AS avg_aov,
        AVG(avg_orders_per_month)                   AS avg_monthly_frequency,
        AVG(avg_days_between_orders)                AS avg_order_gap_days,
        AVG(distinct_restaurants)                   AS avg_restaurant_diversity,
        AVG(avg_monthly_sessions)                   AS avg_monthly_sessions,
        AVG(promo_response_rate)                    AS avg_promo_response_rate,
        COUNT(DISTINCT CASE WHEN is_pro_member THEN user_id END)
                                                    AS pro_member_count,
        AVG(churn_probability_30d)                  AS avg_churn_probability
    FROM gold.user_cohorts
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) FROM gold.user_cohorts
    )
    GROUP BY city_name, user_segment, lifecycle_stage
)
SELECT
    city_name,
    user_segment,
    lifecycle_stage,
    user_count,
    ROUND(avg_lifetime_orders, 1)                       AS avg_lifetime_orders,
    ROUND(avg_lifetime_gmv, 2)                          AS avg_lifetime_gmv,
    ROUND(avg_estimated_ltv, 2)                         AS avg_estimated_ltv,
    ROUND(avg_aov, 2)                                   AS avg_aov,
    ROUND(avg_monthly_frequency, 2)                     AS avg_monthly_frequency,
    ROUND(avg_order_gap_days, 1)                        AS avg_days_between_orders,
    ROUND(avg_restaurant_diversity, 1)                  AS avg_distinct_restaurants,
    ROUND(avg_monthly_sessions, 1)                      AS avg_monthly_sessions,
    ROUND(avg_promo_response_rate * 100, 2)             AS promo_response_pct,
    pro_member_count,
    ROUND(pro_member_count * 100.0
        / NULLIF(user_count, 0), 2)                     AS pro_penetration_pct,
    ROUND(avg_churn_probability * 100, 2)               AS avg_churn_risk_pct
FROM segment_metrics
ORDER BY city_name, user_count DESC;


-- ============================================================================
-- Churn risk report - actionable user list for intervention campaigns
-- ============================================================================
SELECT
    user_id,
    city_name,
    cohort_month,
    user_segment,
    lifecycle_stage,
    churn_risk_tier,
    ROUND(churn_probability_30d * 100, 1)       AS churn_probability_pct,
    days_since_last_order,
    days_since_last_app_open,
    lifetime_order_count,
    ROUND(lifetime_gmv, 2)                      AS lifetime_gmv,
    ROUND(estimated_ltv, 2)                     AS estimated_ltv,
    ROUND(avg_order_value, 2)                   AS avg_order_value,
    preferred_cuisine,
    preferred_payment_method,
    is_pro_member,
    pro_tier,
    ROUND(promo_response_rate * 100, 1)         AS promo_response_pct,
    recommended_intervention,

    -- Prioritization score: high LTV users at high churn risk
    ROUND(
        churn_probability_30d * estimated_ltv, 2
    ) AS intervention_priority_score

FROM gold.user_cohorts
WHERE snapshot_date = (
    SELECT MAX(snapshot_date) FROM gold.user_cohorts
)
AND churn_risk_tier IN ('high', 'critical')
AND lifecycle_stage != 'churned'
AND estimated_ltv > 500
ORDER BY intervention_priority_score DESC
LIMIT 10000;


-- ============================================================================
-- Week-over-week retention trend (for tracking retention improvements)
-- ============================================================================
WITH weekly_retention AS (
    SELECT
        snapshot_date,
        COUNT(DISTINCT user_id)                                     AS total_users,
        COUNT(DISTINCT CASE WHEN is_active_m1 THEN user_id END)    AS retained_m1,
        COUNT(DISTINCT CASE WHEN lifecycle_stage = 'churned'
                             THEN user_id END)                      AS churned,
        COUNT(DISTINCT CASE WHEN lifecycle_stage = 'resurrected'
                             THEN user_id END)                      AS resurrected,
        AVG(estimated_ltv)                                          AS avg_ltv,
        AVG(churn_probability_30d)                                  AS avg_churn_prob
    FROM gold.user_cohorts
    WHERE snapshot_date >= CURRENT_DATE - INTERVAL '12' WEEK
    GROUP BY snapshot_date
)
SELECT
    snapshot_date,
    total_users,
    ROUND(retained_m1 * 100.0 / NULLIF(total_users, 0), 2)     AS m1_retention_pct,
    churned,
    resurrected,
    ROUND(churned * 100.0 / NULLIF(total_users, 0), 2)         AS churn_rate_pct,
    ROUND(resurrected * 100.0 / NULLIF(churned, 0), 2)         AS resurrection_rate_pct,
    ROUND(avg_ltv, 2)                                           AS avg_estimated_ltv,
    ROUND(avg_churn_prob * 100, 2)                              AS avg_churn_risk_pct,

    -- Week-over-week delta
    ROUND(
        (retained_m1 * 100.0 / NULLIF(total_users, 0)) -
        LAG(retained_m1 * 100.0 / NULLIF(total_users, 0))
            OVER (ORDER BY snapshot_date), 2
    ) AS retention_wow_delta_pp

FROM weekly_retention
ORDER BY snapshot_date DESC;
