-- Load crypto benchmark data from parquet files
-- Expects: data/bench_flows_dict.parquet, data/bench_entity_dict.parquet
--          data/bench_flows_varchar.parquet, data/bench_entity_varchar.parquet

-- Load V1 (VARCHAR) tables
INSERT INTO address_flows_daily
  SELECT * FROM read_parquet('data/bench_flows_varchar.parquet');

INSERT INTO entity_address_map
  SELECT * FROM read_parquet('data/bench_entity_varchar.parquet');

-- Build address dictionary from V1 data
INSERT INTO address_dictionary
  WITH all_addrs AS (
    SELECT DISTINCT from_address AS address FROM address_flows_daily
    UNION
    SELECT DISTINCT to_address AS address FROM address_flows_daily
  )
  SELECT address, ROW_NUMBER() OVER (ORDER BY address)::INTEGER AS addr_id
  FROM all_addrs;

-- Build V3 (dict-encoded) tables
INSERT INTO address_flows_daily_dict
  SELECT f.date, f.asset, f.amount, f.tx_count, d1.addr_id, d2.addr_id
  FROM address_flows_daily f
  JOIN address_dictionary d1 ON f.from_address = d1.address
  JOIN address_dictionary d2 ON f.to_address = d2.address;

INSERT INTO entity_address_map_dict
  SELECT e.entity, e.category, e.attribution_source, d.addr_id
  FROM entity_address_map e
  JOIN address_dictionary d ON e.address = d.address;
