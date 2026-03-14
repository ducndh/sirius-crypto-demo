#!/usr/bin/env python3
"""Prepare DuckDB tables from downloaded data.

IMPORTANT: Uses the Sirius DuckDB binary (v1.4.4) to create the database,
NOT the system duckdb Python package, to ensure format compatibility.

Creates:
  - token_transfers: Ethereum token transfers from Parquet
  - entity_address_map: MBAL address labels (filtered to Ethereum)
  - address_flows_daily: Pre-aggregated daily flows (materialized)

Output: data/crypto_demo.duckdb
"""
import os
import sys
import subprocess
import argparse

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
ETH_DIR = os.path.join(DATA_DIR, "eth_transfers")
MBAL_CSV = os.path.join(DATA_DIR, "mbal", "dataset_10m_ads.csv")

# Default Sirius binary and pixi env
DEFAULT_SIRIUS_BIN = os.path.expanduser("~/sirius-dev/build/release/duckdb")
DEFAULT_PIXI_ENV = os.path.expanduser("~/sirius-dev/.pixi/envs/cuda12")


def run_sql(db_path, sql, sirius_bin, pixi_env):
    """Execute SQL using the Sirius DuckDB binary (v1.4.4)."""
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = f"{pixi_env}/lib:{env.get('LD_LIBRARY_PATH', '')}"
    result = subprocess.run(
        [sirius_bin, db_path, "-c", sql],
        capture_output=True, text=True, env=env, timeout=3600
    )
    if result.returncode != 0:
        print(f"SQL ERROR: {result.stderr}")
        sys.exit(1)
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description="Prepare DuckDB tables from downloaded data")
    parser.add_argument("--year", type=str, default="2026",
                        help="Year prefix to filter ETH data (default: 2026)")
    parser.add_argument("--db-name", type=str, default=None,
                        help="Database filename (default: crypto_demo_{year}.duckdb)")
    parser.add_argument("--sirius-bin", type=str,
                        default=os.environ.get("SIRIUS_BIN", DEFAULT_SIRIUS_BIN),
                        help="Path to Sirius DuckDB binary")
    parser.add_argument("--pixi-env", type=str,
                        default=os.environ.get("SIRIUS_PIXI_ENV", DEFAULT_PIXI_ENV),
                        help="Path to pixi env for LD_LIBRARY_PATH")
    args = parser.parse_args()

    db_name = args.db_name or f"crypto_demo_{args.year}.duckdb"
    db_path = os.path.join(DATA_DIR, db_name)
    parquet_glob = f"{ETH_DIR}/date={args.year}-*/*.parquet"

    if not os.path.exists(args.sirius_bin):
        print(f"ERROR: Sirius binary not found at {args.sirius_bin}")
        print("Build with: cd ~/sirius-dev && ~/.pixi/bin/pixi run -e cuda12 make release")
        sys.exit(1)

    if not os.path.exists(MBAL_CSV):
        print(f"ERROR: MBAL data not found at {MBAL_CSV}")
        print("Run: python scripts/download_mbal.py")
        sys.exit(1)

    # Verify binary version
    version = run_sql(":memory:", "SELECT version();", args.sirius_bin, args.pixi_env)
    print(f"Using DuckDB {version}")
    if "1.4.4" not in version:
        print(f"WARNING: Expected v1.4.4, got {version}. Database format may be incompatible!")

    # Remove old database if exists
    if os.path.exists(db_path):
        print(f"Removing existing database: {db_path}")
        os.remove(db_path)
        # Also remove WAL file
        wal = db_path + ".wal"
        if os.path.exists(wal):
            os.remove(wal)

    print(f"\nCreating database at {db_path}")
    print(f"ETH data filter: date={args.year}-*")

    # 1. Load token transfers
    print("\n[1/3] Loading token transfers...")
    run_sql(db_path, f"""
        CREATE TABLE token_transfers AS
        SELECT *
        FROM read_parquet('{parquet_glob}', hive_partitioning=true);
    """, args.sirius_bin, args.pixi_env)
    out = run_sql(db_path, "SELECT COUNT(*) FROM token_transfers;",
                  args.sirius_bin, args.pixi_env)
    print(f"  Loaded: {out}")

    # 2. Load MBAL entity address map (Ethereum only)
    print("\n[2/3] Loading MBAL entity address map...")
    run_sql(db_path, f"""
        CREATE TABLE entity_address_map AS
        SELECT
            address,
            entity,
            categories AS category,
            source AS attribution_source
        FROM read_csv('{MBAL_CSV}', auto_detect=true, quote='"', ignore_errors=true)
        WHERE chain = 'ethereum_mainnet'
          AND entity IS NOT NULL
          AND entity != '';
    """, args.sirius_bin, args.pixi_env)
    out = run_sql(db_path, "SELECT COUNT(*) FROM entity_address_map;",
                  args.sirius_bin, args.pixi_env)
    print(f"  Loaded: {out}")

    # Show top entities
    print("\n  Top entities:")
    out = run_sql(db_path, """
        SELECT entity, COUNT(*) as cnt
        FROM entity_address_map
        GROUP BY entity
        ORDER BY cnt DESC
        LIMIT 10;
    """, args.sirius_bin, args.pixi_env)
    print(f"  {out}")

    # 3. Materialize daily address flows
    print("\n[3/3] Materializing address_flows_daily...")
    run_sql(db_path, """
        CREATE TABLE address_flows_daily AS
        SELECT
            date,
            token_address AS asset,
            from_address,
            to_address,
            SUM(value) AS amount,
            CAST(COUNT(*) AS BIGINT) AS tx_count
        FROM token_transfers
        GROUP BY 1, 2, 3, 4;
    """, args.sirius_bin, args.pixi_env)
    out = run_sql(db_path, "SELECT COUNT(*) FROM address_flows_daily;",
                  args.sirius_bin, args.pixi_env)
    print(f"  Materialized: {out}")

    # 4. Normalize addresses (AWS uses 66-char zero-padded, MBAL uses 42-char)
    print("\n[4/6] Normalizing addresses (66→42 char)...")
    run_sql(db_path, """
        UPDATE address_flows_daily
        SET from_address = '0x' || SUBSTR(from_address, 27),
            to_address   = '0x' || SUBSTR(to_address, 27)
        WHERE LENGTH(from_address) = 66;
    """, args.sirius_bin, args.pixi_env)
    print("  Done")

    # 5. Create DICT (dictionary-encoded) tables for V3 benchmark
    print("\n[5/6] Creating dictionary-encoded tables...")
    run_sql(db_path, """
        -- Build address dictionary from entity_address_map
        CREATE TABLE address_dictionary AS
        SELECT address, ROW_NUMBER() OVER (ORDER BY address)::INTEGER AS addr_id
        FROM (SELECT DISTINCT address FROM entity_address_map);
    """, args.sirius_bin, args.pixi_env)
    out = run_sql(db_path, "SELECT COUNT(*) FROM address_dictionary;",
                  args.sirius_bin, args.pixi_env)
    print(f"  Dictionary: {out} addresses")

    run_sql(db_path, """
        CREATE TABLE entity_address_map_dict AS
        SELECT e.entity, e.category, e.attribution_source, d.addr_id
        FROM entity_address_map e
        JOIN address_dictionary d ON e.address = d.address;
    """, args.sirius_bin, args.pixi_env)

    run_sql(db_path, """
        CREATE TABLE address_flows_daily_dict AS
        SELECT
            f.date, f.asset, f.amount, f.tx_count,
            COALESCE(d_from.addr_id, 0) AS from_addr_id,
            COALESCE(d_to.addr_id, 0) AS to_addr_id
        FROM address_flows_daily f
        LEFT JOIN address_dictionary d_from ON f.from_address = d_from.address
        LEFT JOIN address_dictionary d_to   ON f.to_address   = d_to.address;
    """, args.sirius_bin, args.pixi_env)
    print("  Created: entity_address_map_dict, address_flows_daily_dict")

    # 6. Export parquets for gpu_execution
    print("\n[6/6] Exporting parquet files for gpu_execution...")
    pq_dir = DATA_DIR
    run_sql(db_path, f"""
        COPY token_transfers TO '{pq_dir}/bench_token_transfers.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        COPY address_flows_daily TO '{pq_dir}/bench_flows_varchar.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        COPY entity_address_map TO '{pq_dir}/bench_entity_varchar.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        COPY address_flows_daily_dict TO '{pq_dir}/bench_flows_dict.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        COPY entity_address_map_dict TO '{pq_dir}/bench_entity_dict.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD);
    """, args.sirius_bin, args.pixi_env)
    for name in ["bench_token_transfers", "bench_flows_varchar",
                  "bench_entity_varchar", "bench_flows_dict", "bench_entity_dict"]:
        pq = os.path.join(pq_dir, f"{name}.parquet")
        if os.path.exists(pq):
            sz = os.path.getsize(pq) / 1024 / 1024
            print(f"  {name}.parquet: {sz:.0f} MB")

    # Summary
    print("\n--- Database Summary ---")
    out = run_sql(db_path, """
        SELECT table_name, estimated_size, column_count
        FROM duckdb_tables();
    """, args.sirius_bin, args.pixi_env)
    print(out)

    db_size = os.path.getsize(db_path) / 1024 / 1024 / 1024
    print(f"\n  Database size: {db_size:.2f} GB")
    print(f"  Path: {db_path}")


if __name__ == "__main__":
    main()
