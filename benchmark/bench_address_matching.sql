-- ============================================================
-- Benchmark: Three address matching strategies
-- Run each on: DuckDB CPU, Sirius GPU (gpu_processing)
-- ============================================================

-- ============================================================
-- V1: VARCHAR JOIN (baseline, no optimization)
-- ============================================================
-- .timer on
SELECT
    f.date,
    f.asset,
    COALESCE(src.entity, 'unknown') AS from_entity,
    COALESCE(dst.entity, 'unknown') AS to_entity,
    SUM(f.amount) AS amount
FROM address_flows_daily f
LEFT JOIN entity_address_map src ON f.from_address = src.address
LEFT JOIN entity_address_map dst ON f.to_address = dst.address
WHERE f.date >= '2026-01-01' AND f.date < '2026-01-08'
GROUP BY 1, 2, 3, 4
HAVING from_entity != 'unknown' AND to_entity != 'unknown'
ORDER BY amount DESC
LIMIT 20;

-- ============================================================
-- V2: INT64 HASH JOIN (probabilistic, ~1/2^63 collision risk)
-- Uses pre-computed hash columns on address_flows_daily_hashed
-- and entity_address_map_hashed
-- ============================================================
SELECT
    f.date,
    f.asset,
    COALESCE(src.entity, 'unknown') AS from_entity,
    COALESCE(dst.entity, 'unknown') AS to_entity,
    SUM(f.amount) AS amount
FROM address_flows_daily_hashed f
LEFT JOIN entity_address_map_hashed src ON f.from_addr_hash = src.addr_hash
LEFT JOIN entity_address_map_hashed dst ON f.to_addr_hash = dst.addr_hash
WHERE f.date >= '2026-01-01' AND f.date < '2026-01-08'
GROUP BY 1, 2, 3, 4
HAVING from_entity != 'unknown' AND to_entity != 'unknown'
ORDER BY amount DESC
LIMIT 20;

-- ============================================================
-- V3: INT32 DICTIONARY JOIN (deterministic, zero collision)
-- Uses pre-built address_dict to map addresses to integer IDs
-- ============================================================
SELECT
    f.date,
    f.asset,
    COALESCE(src.entity, 'unknown') AS from_entity,
    COALESCE(dst.entity, 'unknown') AS to_entity,
    SUM(f.amount) AS amount
FROM address_flows_daily_dict f
LEFT JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id
LEFT JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id
WHERE f.date >= '2026-01-01' AND f.date < '2026-01-08'
GROUP BY 1, 2, 3, 4
HAVING from_entity != 'unknown' AND to_entity != 'unknown'
ORDER BY amount DESC
LIMIT 20;
