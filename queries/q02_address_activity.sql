-- Q02: Activity summary for a specific address
-- "Show me everything this address did"
-- Category: Scan / Filter
-- Showcases: selective filter on high-cardinality address column
--
-- NOTE: Replace the address below with a real high-activity address from your dataset.
-- To find one, run:
--   SELECT from_address, COUNT(*) AS cnt FROM eth_transactions
--   GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
-- Good candidates: Uniswap Router, Tether Treasury, Binance hot wallet

SELECT
    COUNT(*)                      AS tx_count,
    SUM(value / 1e18)             AS total_eth_sent,
    AVG(value / 1e18)             AS avg_eth_per_tx,
    MIN(block_timestamp)          AS first_seen,
    MAX(block_timestamp)          AS last_seen,
    COUNT(DISTINCT to_address)    AS unique_recipients
FROM eth_transactions
WHERE from_address = '0x__REPLACE_WITH_REAL_ADDRESS__';
