-- Submitted BY - Neeraj Kumar (8882078050)
 

/* 
   B1: Average ROAS across slots (08:00-10:00, 10:00-12:00, 12:00-14:00)
   for the last 14 days.
*/

with daily_revenue AS (
    SELECT
        DATE(order_ts) AS date,
        campaign_id,
        SUM(price * quantity) AS revenue
    FROM orders
    WHERE campaign_id IS NOT NULL
    GROUP BY 1, 2
),
daily_spend AS (
    SELECT
        date,
        campaign_id,
        SUM(spend) AS spend
    FROM costs
    GROUP BY 1, 2
),
daily_roas AS (
    SELECT
        COALESCE(r.date, s.date)          AS date,
        COALESCE(r.campaign_id, s.campaign_id) AS campaign_id,
        COALESCE(r.revenue, 0)           AS revenue,
        COALESCE(s.spend, 0)             AS spend,
        CASE
            WHEN COALESCE(s.spend, 0) > 0
                THEN COALESCE(r.revenue, 0) / s.spend
            ELSE 0
        END AS roas
    FROM daily_revenue r
    FULL OUTER JOIN daily_spend s
      ON r.date = s.date
     AND r.campaign_id = s.campaign_id
),
with_slot AS (
    SELECT
        dr.date,
        c.slot,
        dr.roas
    FROM daily_roas dr
    JOIN campaigns c
      ON dr.campaign_id = c.campaign_id
    WHERE dr.date >= CURRENT_DATE - INTERVAL '14 days'
      AND dr.date <  CURRENT_DATE
      AND c.slot IN ('08:00-10:00', '10:00-12:00', '12:00-14:00')
)
SELECT
    slot,
    AVG(roas) AS avg_roas
FROM with_slot
GROUP BY slot
ORDER BY slot;



/* B2: Top 5 campaign-days where actual impressions exceed allocated_impression_cap,
   ranked by percentage overage.
*/
with daily_impressions AS (
    SELECT
        DATE(timestamp) AS date,
        campaign_id,
        COUNT(*) AS impressions  -- count of impression events
    FROM ad_events
    WHERE event_type = 'impression'
    GROUP BY 1, 2
),
impressions_vs_cap AS (
    SELECT
        i.date,
        i.campaign_id,
        i.impressions,
        c.allocated_impression_cap,
        CASE
            WHEN c.allocated_impression_cap > 0 THEN
                (i.impressions - c.allocated_impression_cap)::DECIMAL
                / c.allocated_impression_cap
            ELSE NULL
        END AS pct_over_cap
    FROM daily_impressions i
    JOIN campaign_caps c
      ON i.date = c.date
     AND i.campaign_id = c.campaign_id
)
SELECT
    date,
    campaign_id,
    impressions,
    allocated_impression_cap,
    pct_over_cap
FROM impressions_vs_cap
WHERE pct_over_cap > 0
ORDER BY pct_over_cap DESC
LIMIT 5;

-- Troubleshooting notes:
-- 1) Verify caps in campaign_caps (units, dates, campaign mapping).
-- 2) Check if ad server enforces caps per campaign per day vs. other grain.
-- 3) Look for sudden traffic spikes or new placements for overshooting campaigns.
-- 4) Confirm impression logging is de-duplicated and not double-counting.
