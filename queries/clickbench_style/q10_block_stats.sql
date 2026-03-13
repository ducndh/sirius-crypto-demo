-- Q10: Block-level statistics
-- "What are the busiest blocks?"
-- Category: Aggregation
-- Showcases: numeric GROUP BY (block_number) with multiple aggregates

SELECT
    block_number,
    COUNT(*)           AS tx_count,
    SUM(value / 1e18)  AS total_eth,
    AVG(gas_price)     AS avg_gas_price,
    MAX(value / 1e18)  AS largest_tx_eth
FROM eth_transactions
GROUP BY block_number
ORDER BY tx_count DESC
LIMIT 100;
