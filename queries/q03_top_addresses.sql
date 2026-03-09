-- Q03: Top 100 addresses by transaction count
-- "Who are the most active senders?"
-- Category: Scan / Filter + GROUP BY
-- Showcases: high-cardinality GROUP BY on VARCHAR addresses

SELECT
    from_address,
    COUNT(*)         AS tx_count,
    SUM(value/1e18)  AS total_eth_sent
FROM eth_transactions
GROUP BY from_address
ORDER BY tx_count DESC
LIMIT 100;
