-- =============================================================================
-- TRM Compliance Query: N-Hop Counterparty Trace
-- =============================================================================
-- "Find all entities reachable within N hops from a flagged address."
-- Uses iterative semi-join BFS (not naive path enumeration).
-- See docs/2hop_problem_explain.md for why naive JOIN is O(|E|^k).
--
-- Uses v_edges and v_entity_map (defined in setup_views.sql).
--
-- Current GPU support status:
--   - Each per-hop join+groupby runs via gpu_processing (GPU)
--   - Anti-join (EXCEPT) runs on CPU — GPU anti-join not yet implemented
--   - When Sirius adds anti-join support, this becomes a single gpu_execution call
--
-- NOTE: Uses CREATE VIEW for hop frontiers (no disk writes) instead of
--       CREATE TABLE. Views are re-evaluated each reference but hop sets
--       are small (millions of vertices, not billions of paths).
-- =============================================================================

-- Step 0: set the source address id (replace with actual addr_id from address_dictionary)
-- Example: SELECT addr_id FROM address_dictionary WHERE address = '0x28c6c...'
-- Here we use addr_id = 12345 as placeholder.

-- Hop 1: direct neighbors of source
CREATE OR REPLACE VIEW v_hop1 AS
SELECT dst AS vertex
FROM v_edges
WHERE src = 12345   -- replace with source addr_id
GROUP BY dst;

-- Hop 2: neighbors of hop1, excluding already-visited
CREATE OR REPLACE VIEW v_hop2 AS
SELECT e.dst AS vertex
FROM v_edges e
JOIN v_hop1 ON e.src = v_hop1.vertex
GROUP BY e.dst
EXCEPT SELECT vertex FROM v_hop1;

-- Hop 3: neighbors of hop2, excluding already-visited
CREATE OR REPLACE VIEW v_hop3 AS
SELECT e.dst AS vertex
FROM v_edges e
JOIN v_hop2 ON e.src = v_hop2.vertex
GROUP BY e.dst
EXCEPT SELECT vertex FROM v_hop1
EXCEPT SELECT vertex FROM v_hop2;

-- Final: resolve vertices to entity names
-- Run this query via gpu_execution or gpu_processing
SELECT
    h.hop,
    CASE WHEN em.entity IS NOT NULL THEN em.entity ELSE 'unknown' END AS entity,
    em.category,
    COUNT(*) AS address_count
FROM (
    SELECT 1 AS hop, vertex FROM v_hop1
    UNION ALL
    SELECT 2 AS hop, vertex FROM v_hop2
    UNION ALL
    SELECT 3 AS hop, vertex FROM v_hop3
) h
LEFT JOIN v_entity_map em ON h.vertex = em.addr_id
GROUP BY 1, 2, 3
ORDER BY hop, address_count DESC;

-- =============================================================================
-- GPU execution path (when UNION ALL + anti-join land in Sirius):
-- CALL gpu_execution('<above query as single string>');
-- =============================================================================
