-- Q06: Category-level flow matrix
-- Aggregates entity flows to category level (exchange → exchange, exchange → defi, etc.)
WITH address_flows AS (
    SELECT
        from_address,
        to_address,
        SUM(value) AS amount
    FROM token_transfers
    GROUP BY 1, 2
)
SELECT
    COALESCE(src.category, 'unknown') AS from_category,
    COALESCE(dst.category, 'unknown') AS to_category,
    SUM(f.amount)                     AS total_flow,
    COUNT(DISTINCT src.entity)        AS num_src_entities,
    COUNT(DISTINCT dst.entity)        AS num_dst_entities
FROM address_flows f
LEFT JOIN entity_address_map src ON f.from_address = src.address
LEFT JOIN entity_address_map dst ON f.to_address   = dst.address
WHERE src.category IS NOT NULL
  AND dst.category IS NOT NULL
GROUP BY 1, 2
ORDER BY total_flow DESC;
