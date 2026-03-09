-- Q05: Daily USD transfer volume over time
-- "Show me total USD moved per day"
-- Category: ASOF JOIN + GROUP BY
-- Showcases: ASOF JOIN feeding into DATE_TRUNC GROUP BY pipeline

SELECT
    DATE_TRUNC('day', t.block_timestamp)       AS day,
    COUNT(*)                                   AS num_transactions,
    SUM(t.value / 1e18 * p.price_usd)          AS daily_usd_volume,
    AVG(t.value / 1e18 * p.price_usd)          AS avg_usd_per_tx
FROM eth_transactions t
ASOF JOIN prices p
    ON t.block_timestamp >= p.ts
GROUP BY 1
ORDER BY 1;
