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
-- Top sender in 2024-Q1: 0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88 (1.4M txns, likely Binance)
WHERE from_address = '0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88';
