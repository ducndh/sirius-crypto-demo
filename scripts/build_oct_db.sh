#!/usr/bin/env bash
# Build 1-month October 2025 DuckDB for gpu_processing Q01 benchmarking
set -euo pipefail

export LD_LIBRARY_PATH="/home/cc/sirius-asof/.pixi/envs/cuda12/lib"
DUCKDB="/home/cc/sirius-asof/build/release/duckdb"
DB="/home/cc/sirius-crypto-demo/data/crypto_demo_2025_oct.duckdb"
ETH_DIR="/home/cc/sirius-crypto-demo/data/eth_transfers"
MBAL_CSV="/home/cc/sirius-crypto-demo/data/mbal/dataset_10m_ads.csv"

# Clean up
rm -f "$DB" "${DB}.wal"

echo "=== Building crypto_demo_2025_oct.duckdb ==="
echo "Binary: $DUCKDB"
$DUCKDB "$DB" -c "SELECT version();"

# 1. token_transfers
echo ""
echo "[1/6] Loading token_transfers (October 2025)..."
$DUCKDB "$DB" -c "
CREATE TABLE token_transfers AS
SELECT *
FROM read_parquet('${ETH_DIR}/date=2025-10-*/*.parquet', hive_partitioning=true);
"
$DUCKDB "$DB" -c "SELECT COUNT(*) AS token_transfers_rows FROM token_transfers;"

# 2. entity_address_map
echo ""
echo "[2/6] Loading entity_address_map..."
$DUCKDB "$DB" -c "
CREATE TABLE entity_address_map AS
SELECT
    address,
    entity,
    categories AS category,
    source AS attribution_source
FROM read_csv('${MBAL_CSV}', auto_detect=true, quote='\"', ignore_errors=true)
WHERE chain = 'ethereum_mainnet'
  AND entity IS NOT NULL
  AND entity != '';
"
$DUCKDB "$DB" -c "SELECT COUNT(*) AS entity_address_map_rows FROM entity_address_map;"

# 3. address_flows_daily (with normalized addresses)
echo ""
echo "[3/6] Materializing address_flows_daily..."
$DUCKDB "$DB" -c "
CREATE TABLE address_flows_daily AS
SELECT
    date,
    token_address AS asset,
    '0x' || SUBSTR(from_address, 27) AS from_address,
    '0x' || SUBSTR(to_address, 27)   AS to_address,
    SUM(value) AS amount,
    CAST(COUNT(*) AS BIGINT) AS tx_count
FROM token_transfers
GROUP BY 1, 2, 3, 4;
"
$DUCKDB "$DB" -c "SELECT COUNT(*) AS address_flows_daily_rows FROM address_flows_daily;"

# 4. address_dictionary
echo ""
echo "[4/6] Creating address_dictionary..."
$DUCKDB "$DB" -c "
CREATE TABLE address_dictionary AS
SELECT address, ROW_NUMBER() OVER (ORDER BY address)::INTEGER AS addr_id
FROM (SELECT DISTINCT address FROM entity_address_map);
"
$DUCKDB "$DB" -c "SELECT COUNT(*) AS address_dictionary_rows FROM address_dictionary;"

# 5. entity_address_map_dict
echo ""
echo "[5/6] Creating entity_address_map_dict..."
$DUCKDB "$DB" -c "
CREATE TABLE entity_address_map_dict AS
SELECT e.entity, e.category, e.attribution_source, d.addr_id
FROM entity_address_map e
JOIN address_dictionary d ON e.address = d.address;
"
$DUCKDB "$DB" -c "SELECT COUNT(*) AS entity_address_map_dict_rows FROM entity_address_map_dict;"

# 6. address_flows_daily_dict
echo ""
echo "[6/6] Creating address_flows_daily_dict..."
$DUCKDB "$DB" -c "
CREATE TABLE address_flows_daily_dict AS
SELECT
    f.date, f.asset, f.amount, f.tx_count,
    COALESCE(d_from.addr_id, 0) AS from_addr_id,
    COALESCE(d_to.addr_id, 0)   AS to_addr_id
FROM address_flows_daily f
LEFT JOIN address_dictionary d_from ON f.from_address = d_from.address
LEFT JOIN address_dictionary d_to   ON f.to_address   = d_to.address;
"
$DUCKDB "$DB" -c "SELECT COUNT(*) AS address_flows_daily_dict_rows FROM address_flows_daily_dict;"

# Summary
echo ""
echo "=== Summary ==="
$DUCKDB "$DB" -c "
SELECT table_name, estimated_size, column_count
FROM duckdb_tables()
ORDER BY table_name;
"

DB_SIZE=$(du -h "$DB" | cut -f1)
echo ""
echo "Database: $DB"
echo "Size: $DB_SIZE"
echo "Done."
