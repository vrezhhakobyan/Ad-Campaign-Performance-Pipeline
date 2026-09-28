-- Run these in the Athena console (AWS mode only).
-- Database: ad_analytics   (created in the Glue catalog by the Spark jobs)

-- 1. Best campaigns by click-through rate, last 7 days
SELECT campaign_id,
       SUM(impressions)                              AS impressions,
       SUM(clicks)                                   AS clicks,
       ROUND(SUM(clicks) * 1.0 / SUM(impressions), 4) AS ctr,
       ROUND(SUM(spend), 2)                          AS spend
FROM ad_analytics.campaign_daily_metrics
WHERE event_date >= date_add('day', -7, current_date)
GROUP BY campaign_id
ORDER BY ctr DESC;

-- 2. Cheapest clicks (cost per click), all days
SELECT campaign_id,
       ROUND(SUM(spend) / NULLIF(SUM(clicks), 0), 4) AS cost_per_click
FROM ad_analytics.campaign_daily_metrics
GROUP BY campaign_id
ORDER BY cost_per_click;

-- 3. Daily spend trend
SELECT event_date, ROUND(SUM(spend), 2) AS total_spend
FROM ad_analytics.campaign_daily_metrics
GROUP BY event_date
ORDER BY event_date;

-- 4. Which device converts best? (uses the clean events table)
SELECT device,
       COUNT_IF(event_type = 'impression') AS impressions,
       COUNT_IF(event_type = 'click')      AS clicks,
       ROUND(COUNT_IF(event_type = 'click') * 1.0
             / COUNT_IF(event_type = 'impression'), 4) AS ctr
FROM ad_analytics.events_clean
GROUP BY device
ORDER BY ctr DESC;

-- 5. Iceberg feature: see the history of the table (each write is a snapshot)
SELECT * FROM "ad_analytics"."campaign_daily_metrics$snapshots"
ORDER BY committed_at DESC;

-- 6. Iceberg feature: time travel - look at the table as it was earlier
-- (replace the timestamp with one from the query above)
SELECT * FROM ad_analytics.campaign_daily_metrics
FOR TIMESTAMP AS OF TIMESTAMP '2026-09-02 12:00:00 UTC'
LIMIT 20;
