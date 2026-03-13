-- Q06: Top 50 addresses by total USD volume sent
-- "Who moved the most money?"
-- Category: ASOF JOIN + high-cardinality GROUP BY
-- Showcases: ASOF JOIN + GROUP BY on VARCHAR — the full compliance query pipeline

SELECT
    t.from_address,
    COUNT(*)                           AS tx_count,
    SUM(t.value / 1e18 * p.price_usd) AS total_usd_sent,
    AVG(t.value / 1e18 * p.price_usd) AS avg_usd_per_tx
FROM eth_transactions t
ASOF JOIN prices p
    ON t.block_timestamp >= p.ts
GROUP BY 1
ORDER BY total_usd_sent DESC
LIMIT 50;
