-- Q02: Entity flow rollup — the core TRM query
-- Double hash-join: address_flows × entity_map (sender) × entity_map (receiver)
-- Then GROUP BY to produce entity-to-entity flows
WITH daily_flows AS (
    SELECT
        DATE(block_timestamp) AS date,
        token_address         AS asset,
        from_address,
        to_address,
        SUM(value)            AS amount
    FROM token_transfers
    WHERE block_timestamp >= '2024-01-01'
      AND block_timestamp <  '2024-02-01'
    GROUP BY 1, 2, 3, 4
)
SELECT
    f.date,
    f.asset,
    COALESCE(src.entity, 'unknown') AS from_entity,
    COALESCE(dst.entity, 'unknown') AS to_entity,
    SUM(f.amount)                   AS amount
FROM daily_flows f
LEFT JOIN entity_address_map src ON f.from_address = src.address
LEFT JOIN entity_address_map dst ON f.to_address   = dst.address
GROUP BY 1, 2, 3, 4
HAVING from_entity != 'unknown' AND to_entity != 'unknown'
ORDER BY amount DESC
LIMIT 1000;
