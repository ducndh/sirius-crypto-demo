-- =============================================================================
-- TRM Core Query: Entity Flow Rollup (Sankey)
-- =============================================================================
-- The canonical TRM serving-layer query.
-- Reads from v_flows and v_entity_map (defined in setup_views.sql).
--
-- Run via Sirius gpu_execution:
--   CALL gpu_execution('SELECT * FROM entity_flow_rollup(''2025-10-01'', ''2025-12-31'')');
-- Or directly:
--   CALL gpu_execution('<this query with dates substituted>');
-- =============================================================================

-- Parameters: substitute $start_date and $end_date before passing to gpu_execution
SELECT
    f.date,
    f.asset_id,
    CASE WHEN src.entity IS NOT NULL THEN src.entity ELSE 'unknown' END AS from_entity,
    CASE WHEN dst.entity IS NOT NULL THEN dst.entity ELSE 'unknown' END AS to_entity,
    SUM(f.amount) AS amount,
    COUNT(*)      AS num_address_pairs
FROM v_flows f
JOIN v_entity_map src ON f.from_addr_id = src.addr_id
JOIN v_entity_map dst ON f.to_addr_id   = dst.addr_id
-- Note: LEFT JOIN + HAVING entity != 'unknown' rewrites to INNER JOIN by DuckDB optimizer.
-- We write INNER JOIN directly for GPU path clarity.
GROUP BY 1, 2, 3, 4
ORDER BY amount DESC
LIMIT 1000;
