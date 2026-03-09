-- Q09: Most active address pairs (from → to)
-- "Which address pairs have the most direct interactions?"
-- Category: Aggregation
-- Showcases: two-column GROUP BY on VARCHAR — typical compliance graph-seed query

SELECT
    from_address,
    to_address,
    COUNT(*)          AS tx_count,
    SUM(value / 1e18) AS total_eth
FROM eth_transactions
GROUP BY from_address, to_address
ORDER BY tx_count DESC
LIMIT 100;
