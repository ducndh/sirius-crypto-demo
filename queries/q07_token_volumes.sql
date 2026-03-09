-- Q07: Top token contracts by transfer activity
-- "Which ERC-20 tokens are most active?"
-- Category: Aggregation
-- Showcases: high-cardinality GROUP BY on token_address + multi-column aggregation

SELECT
    token_address,
    COUNT(*)                       AS num_transfers,
    COUNT(DISTINCT from_address)   AS unique_senders,
    COUNT(DISTINCT to_address)     AS unique_receivers,
    SUM(value)                     AS total_value_transferred
FROM token_transfers
GROUP BY token_address
ORDER BY num_transfers DESC
LIMIT 50;
