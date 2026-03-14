-- Schema for crypto benchmark tables (dict-encoded V3)
-- These are created by load.sql from raw parquet data

-- Raw address flows (pre-aggregated from token_transfers)
CREATE TABLE IF NOT EXISTS address_flows_daily (
    date DATE,
    asset VARCHAR,
    from_address VARCHAR,
    to_address VARCHAR,
    amount DOUBLE,
    tx_count BIGINT
);

-- Entity address mapping (from MBAL dataset)
CREATE TABLE IF NOT EXISTS entity_address_map (
    address VARCHAR,
    entity VARCHAR,
    category VARCHAR,
    attribution_source VARCHAR
);

-- Address dictionary for integer-key encoding
CREATE TABLE IF NOT EXISTS address_dictionary (
    address VARCHAR,
    addr_id INTEGER
);

-- Dict-encoded flows (V3 — integer join keys)
CREATE TABLE IF NOT EXISTS address_flows_daily_dict (
    date DATE,
    asset VARCHAR,
    amount DOUBLE,
    tx_count BIGINT,
    from_addr_id INTEGER,
    to_addr_id INTEGER
);

-- Dict-encoded entity map (V3)
CREATE TABLE IF NOT EXISTS entity_address_map_dict (
    entity VARCHAR,
    category VARCHAR,
    attribution_source VARCHAR,
    addr_id INTEGER
);
