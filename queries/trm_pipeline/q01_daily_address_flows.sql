-- Q01: Aggregate raw token transfers to daily address flows
-- This is the foundational aggregation step in TRM's pipeline
SELECT
    DATE(block_timestamp) AS date,
    token_address         AS asset,
    from_address,
    to_address,
    SUM(value)            AS amount,
    COUNT(*)              AS tx_count
FROM token_transfers
WHERE block_timestamp >= '2024-01-01'
  AND block_timestamp <  '2024-02-01'
GROUP BY 1, 2, 3, 4
ORDER BY amount DESC
LIMIT 1000;
