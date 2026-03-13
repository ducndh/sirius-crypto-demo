-- Q03: Top entity counterparty pairs by total flow volume
-- Aggregates across all assets and dates
WITH daily_flows AS (
    SELECT
        from_address,
        to_address,
        SUM(value) AS amount
    FROM token_transfers
    GROUP BY 1, 2
)
SELECT
    COALESCE(src.entity, 'unknown') AS from_entity,
    COALESCE(dst.entity, 'unknown') AS to_entity,
    SUM(f.amount)                   AS total_flow,
    COUNT(*)                        AS num_address_pairs
FROM daily_flows f
LEFT JOIN entity_address_map src ON f.from_address = src.address
LEFT JOIN entity_address_map dst ON f.to_address   = dst.address
WHERE src.entity IS NOT NULL
  AND dst.entity IS NOT NULL
GROUP BY 1, 2
ORDER BY total_flow DESC
LIMIT 100;
