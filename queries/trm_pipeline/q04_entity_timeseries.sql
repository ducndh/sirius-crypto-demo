-- Q04: Time-series flow between two specific entities
-- Typical interactive query: "show me Binance → Coinbase flow over time"
WITH daily_flows AS (
    SELECT
        DATE(block_timestamp) AS date,
        from_address,
        to_address,
        SUM(value)            AS amount
    FROM token_transfers
    GROUP BY 1, 2, 3
)
SELECT
    f.date,
    src.entity AS from_entity,
    dst.entity AS to_entity,
    SUM(f.amount) AS daily_flow
FROM daily_flows f
JOIN entity_address_map src ON f.from_address = src.address
JOIN entity_address_map dst ON f.to_address   = dst.address
WHERE src.entity = 'Binance'
  AND dst.entity = 'Coinbase'
GROUP BY 1, 2, 3
ORDER BY f.date;
