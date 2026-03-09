-- Q04: Match all transactions to the prevailing ETH/USD price
-- "What was the USD value of every transaction?"
-- Category: ASOF JOIN
-- Showcases: GPU ASOF JOIN (no other GPU database supports this)
-- The key query: match each tx to the most recent price quote before its timestamp.

SELECT
    COUNT(*)                              AS num_transactions,
    AVG(t.value / 1e18 * p.price_usd)    AS avg_usd_value,
    SUM(t.value / 1e18 * p.price_usd)    AS total_usd_volume
FROM eth_transactions t
ASOF JOIN prices p
    ON t.block_timestamp >= p.ts;
