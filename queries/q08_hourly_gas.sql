-- Q08: Hourly gas price trends
-- "How did gas prices fluctuate?"
-- Category: Aggregation
-- Showcases: DATE_TRUNC GROUP BY with multiple aggregates on large table

SELECT
    DATE_TRUNC('hour', block_timestamp)  AS hour,
    AVG(gas_price)                        AS avg_gas_price,
    MIN(gas_price)                        AS min_gas_price,
    MAX(gas_price)                        AS max_gas_price,
    COUNT(*)                              AS tx_count
FROM eth_transactions
GROUP BY 1
ORDER BY 1;
