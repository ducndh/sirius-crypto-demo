-- =============================================================================
-- TRM Sankey: Multi-Hop Entity Flow Trace (Weighted Edges)
-- =============================================================================
-- Produces (hop, from_entity_id, to_entity_id, total_amount) edges for Sankey.
-- Traces 3 hops outward from a seed entity through entity_address_map_int.
--
-- GPU constraints & workarounds:
--   - All joins on INTEGER (VARCHAR hash join not supported — type 12 error)
--   - GROUP BY instead of DISTINCT (LOGICAL_DISTINCT not supported)
--   - NOT IN for frontier dedup (MARK join — supported)
--   - UNION ALL to combine hops (supported on gpu_processing)
--   - No ORDER BY + LIMIT (TopN with NULLs falls back to CPU)
--
-- Tables:
--   v_flows     = address_flows_daily_dict (116M+ rows)
--   v_emap      = entity_address_map_int   (3.2M rows: entity_id BIGINT, addr_id INT)
--   entity_lookup = entity_id → entity name (322 rows, for decoding after GPU)
--
-- Seed: fixedfloat = entity_id 143 (no-KYC exchange, ~43K addresses)
--
-- Usage:
--   CREATE OR REPLACE VIEW v_flows AS SELECT * FROM address_flows_daily_dict;
--   CREATE OR REPLACE VIEW v_emap AS SELECT entity_id, addr_id FROM entity_address_map_int;
--   CALL gpu_processing('<this query>');
--   -- Then join result with entity_lookup to decode entity_ids to names.
--
-- Benchmark (A100, warm):  GPU ~22ms  |  CPU ~152ms  |  ~7x speedup
-- =============================================================================

WITH seed_addrs AS (
    SELECT addr_id
    FROM v_emap
    WHERE entity_id = 143
),

-- =========================================================================
-- HOP 1: seed entity → direct counterparties
-- =========================================================================
hop1_edges AS (
    SELECT
        143 AS from_eid,
        dst.entity_id AS to_eid,
        SUM(f.amount) AS total_amount
    FROM v_flows f
    JOIN seed_addrs s ON f.from_addr_id = s.addr_id
    JOIN v_emap dst ON f.to_addr_id = dst.addr_id
    WHERE dst.entity_id != 143
    GROUP BY 1, 2
),

-- Frontier: entities reached at hop 1 (GROUP BY, not DISTINCT — GPU-safe)
hop1_frontier AS (
    SELECT to_eid AS eid, COUNT(*) AS _c FROM hop1_edges GROUP BY 1
),

-- =========================================================================
-- HOP 2: hop1 entities → their outgoing counterparties (excluding visited)
-- =========================================================================
hop2_edges AS (
    SELECT
        src.entity_id AS from_eid,
        dst.entity_id AS to_eid,
        SUM(f.amount) AS total_amount
    FROM v_flows f
    JOIN v_emap src ON f.from_addr_id = src.addr_id
    JOIN hop1_frontier h1 ON src.entity_id = h1.eid
    JOIN v_emap dst ON f.to_addr_id = dst.addr_id
    WHERE dst.entity_id != 143
      AND dst.entity_id NOT IN (SELECT eid FROM hop1_frontier)
    GROUP BY 1, 2
),

hop2_frontier AS (
    SELECT to_eid AS eid, COUNT(*) AS _c FROM hop2_edges GROUP BY 1
),

-- =========================================================================
-- HOP 3: hop2 entities → their outgoing counterparties (excluding visited)
-- =========================================================================
hop3_edges AS (
    SELECT
        src.entity_id AS from_eid,
        dst.entity_id AS to_eid,
        SUM(f.amount) AS total_amount
    FROM v_flows f
    JOIN v_emap src ON f.from_addr_id = src.addr_id
    JOIN hop2_frontier h2 ON src.entity_id = h2.eid
    JOIN v_emap dst ON f.to_addr_id = dst.addr_id
    WHERE dst.entity_id != 143
      AND dst.entity_id NOT IN (SELECT eid FROM hop1_frontier)
      AND dst.entity_id NOT IN (SELECT eid FROM hop2_frontier)
    GROUP BY 1, 2
)

-- =========================================================================
-- FINAL: UNION ALL edges from all hops — decode entity_ids via entity_lookup
-- =========================================================================
SELECT 1 AS hop, from_eid, to_eid, total_amount FROM hop1_edges
UNION ALL
SELECT 2 AS hop, from_eid, to_eid, total_amount FROM hop2_edges
UNION ALL
SELECT 3 AS hop, from_eid, to_eid, total_amount FROM hop3_edges;
