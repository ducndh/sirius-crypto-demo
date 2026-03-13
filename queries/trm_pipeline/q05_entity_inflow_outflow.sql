-- Q05: Entity inflow/outflow summary
-- For each known entity, compute total inflow, outflow, and net flow
WITH address_flows AS (
    SELECT
        from_address,
        to_address,
        SUM(value) AS amount
    FROM token_transfers
    GROUP BY 1, 2
),
outflows AS (
    SELECT
        src.entity,
        SUM(f.amount) AS total_outflow
    FROM address_flows f
    JOIN entity_address_map src ON f.from_address = src.address
    WHERE src.entity IS NOT NULL
    GROUP BY 1
),
inflows AS (
    SELECT
        dst.entity,
        SUM(f.amount) AS total_inflow
    FROM address_flows f
    JOIN entity_address_map dst ON f.to_address = dst.address
    WHERE dst.entity IS NOT NULL
    GROUP BY 1
)
SELECT
    COALESCE(o.entity, i.entity) AS entity,
    COALESCE(i.total_inflow, 0)  AS total_inflow,
    COALESCE(o.total_outflow, 0) AS total_outflow,
    COALESCE(i.total_inflow, 0) - COALESCE(o.total_outflow, 0) AS net_flow
FROM outflows o
FULL OUTER JOIN inflows i ON o.entity = i.entity
ORDER BY net_flow DESC
LIMIT 100;
