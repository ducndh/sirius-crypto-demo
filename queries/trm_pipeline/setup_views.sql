-- =============================================================================
-- TRM Demo: Data Source Views
-- =============================================================================
-- Replace these view definitions with your own data source.
-- Everything below this file reads from these views — no other changes needed.
--
-- Examples:
--   Parquet:  SELECT * FROM read_parquet('/path/to/flows/*.parquet')
--   Iceberg:  SELECT * FROM iceberg_scan('s3://bucket/address_flows_daily')
--   DuckDB:   SELECT * FROM address_flows_daily_dict   (default below)
-- =============================================================================

-- Address-level daily flows
-- Required columns: date DATE, asset_id INT, from_addr_id INT, to_addr_id INT, amount DOUBLE
CREATE OR REPLACE VIEW v_flows AS
    SELECT * FROM address_flows_daily_dict;

-- Entity address map
-- Required columns: addr_id INT, entity VARCHAR, category VARCHAR
CREATE OR REPLACE VIEW v_entity_map AS
    SELECT * FROM entity_address_map_dict;

-- Integer entity map (for GPU queries — no VARCHAR joins)
-- Required columns: entity_id BIGINT, addr_id INT
CREATE OR REPLACE VIEW v_emap AS
    SELECT entity_id, addr_id FROM entity_address_map_int;

-- Edge table for multi-hop graph queries (derived from flows)
-- Required columns: src INT, dst INT
CREATE OR REPLACE VIEW v_edges AS
    SELECT DISTINCT from_addr_id AS src, to_addr_id AS dst
    FROM address_flows_daily_dict;
