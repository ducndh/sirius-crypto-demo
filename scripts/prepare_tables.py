#!/usr/bin/env python3
"""Prepare DuckDB tables from downloaded data.

Creates:
  - token_transfers: Ethereum token transfers from Parquet
  - entity_address_map: MBAL address labels (filtered to Ethereum)
  - address_flows_daily: Pre-aggregated daily flows (materialized)

Output: data/crypto_demo.duckdb
"""
import os
import sys
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "crypto_demo.duckdb")
ETH_DIR = os.path.join(DATA_DIR, "eth_transfers")
MBAL_CSV = os.path.join(DATA_DIR, "mbal", "dataset_10m_ads.csv")


def main():
    if not os.path.exists(ETH_DIR):
        print(f"ERROR: Ethereum data not found at {ETH_DIR}")
        print("Run: python scripts/download_eth_transfers.py")
        sys.exit(1)

    if not os.path.exists(MBAL_CSV):
        print(f"ERROR: MBAL data not found at {MBAL_CSV}")
        print("Run: python scripts/download_mbal.py")
        sys.exit(1)

    print(f"Creating database at {DB_PATH}")
    con = duckdb.connect(DB_PATH)

    # 1. Load token transfers from Parquet
    print("\n[1/3] Loading token transfers...")
    con.execute(f"""
        CREATE OR REPLACE TABLE token_transfers AS
        SELECT *
        FROM read_parquet('{ETH_DIR}/*/*.parquet', hive_partitioning=true)
    """)
    count = con.execute("SELECT COUNT(*) FROM token_transfers").fetchone()[0]
    print(f"  Loaded {count:,} token transfers")

    # 2. Load MBAL entity address map (Ethereum only)
    print("\n[2/3] Loading MBAL entity address map...")
    con.execute(f"""
        CREATE OR REPLACE TABLE entity_address_map AS
        SELECT
            address,
            entity,
            categories AS category,
            source AS attribution_source
        FROM read_csv('{MBAL_CSV}', auto_detect=true)
        WHERE chain = 'ethereum_mainnet'
          AND entity IS NOT NULL
          AND entity != ''
    """)
    count = con.execute("SELECT COUNT(*) FROM entity_address_map").fetchone()[0]
    print(f"  Loaded {count:,} Ethereum entity mappings")

    # Show entity distribution
    print("\n  Top entities by address count:")
    rows = con.execute("""
        SELECT entity, COUNT(*) as cnt
        FROM entity_address_map
        GROUP BY entity
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()
    for entity, cnt in rows:
        print(f"    {entity}: {cnt:,}")

    # 3. Materialize daily address flows
    print("\n[3/3] Materializing address_flows_daily...")
    con.execute("""
        CREATE OR REPLACE TABLE address_flows_daily AS
        SELECT
            date AS date,
            token_address AS asset,
            from_address,
            to_address,
            SUM(value) AS amount,
            COUNT(*) AS tx_count
        FROM token_transfers
        GROUP BY 1, 2, 3, 4
    """)
    count = con.execute("SELECT COUNT(*) FROM address_flows_daily").fetchone()[0]
    print(f"  Materialized {count:,} daily flow records")

    # Summary
    print("\n--- Database Summary ---")
    tables = con.execute("SHOW TABLES").fetchall()
    for (table,) in tables:
        cnt = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {cnt:,} rows")

    db_size = os.path.getsize(DB_PATH) / 1024 / 1024
    print(f"\n  Database size: {db_size:.0f} MB")

    con.close()
    print(f"\nDone. Database: {DB_PATH}")


if __name__ == "__main__":
    main()
