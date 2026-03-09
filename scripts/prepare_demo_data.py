#!/usr/bin/env python3
"""Consolidate raw AWS parquet downloads into demo-ready parquet files.

Reads from data/raw/{eth_transactions,token_transfers}/**/*.parquet
Writes to data/{eth_transactions,token_transfers}.parquet

Also inspects the raw schema and warns about column type issues
(e.g. value as VARCHAR/uint256 instead of BIGINT).

Usage:
  python scripts/prepare_demo_data.py [--raw-dir data/raw] [--out-dir data]
"""
import argparse
import os
import sys
import duckdb


def inspect_schema(con, pattern: str) -> dict:
    """Return column name → type mapping from a parquet glob."""
    rows = con.execute(f"DESCRIBE SELECT * FROM '{pattern}' LIMIT 0").fetchall()
    return {r[0]: r[1] for r in rows}


def report_table(con, path: str, label: str):
    if not os.path.exists(path):
        print(f"  WARNING: {path} not found")
        return
    size_mb = os.path.getsize(path) / 1e6
    count = con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
    ts_range = con.execute(
        f"SELECT MIN(block_timestamp)::DATE, MAX(block_timestamp)::DATE FROM '{path}'"
    ).fetchone()
    print(f"  {label}: {count:,} rows, {size_mb:.1f} MB, {ts_range[0]} → {ts_range[1]}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--raw-dir', default='data/raw')
    p.add_argument('--out-dir', default='data')
    args = p.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    con = duckdb.connect()

    # ── ETH Transactions ──────────────────────────────────────────────
    eth_pattern = os.path.join(args.raw_dir, 'eth_transactions', '**', '*.parquet')
    eth_out = os.path.join(args.out_dir, 'eth_transactions.parquet')

    print("Inspecting raw ETH transaction schema...")
    try:
        schema = inspect_schema(con, eth_pattern)
        print(f"  Columns: {list(schema.keys())}")
    except Exception as e:
        print(f"  ERROR reading raw transactions: {e}")
        print(f"  Run: bash scripts/download_eth_data.sh dev")
        sys.exit(1)

    # AWS schema (as of 2024): hash, nonce, transaction_index, from_address,
    # to_address, value (STRING — uint256), gas, gas_price, input, receipt_*,
    # block_timestamp (TIMESTAMP), block_number, block_hash, max_fee_per_gas, ...
    value_col = 'value'
    value_type = schema.get('value', 'UNKNOWN')
    gas_price_col = 'gas_price' if 'gas_price' in schema else 'gas'

    if 'STRING' in value_type.upper() or 'VARCHAR' in value_type.upper():
        print(f"  NOTE: 'value' column is {value_type} (uint256 as string). Casting to DOUBLE.")
        value_expr = "TRY_CAST(value AS DOUBLE)"
    else:
        value_expr = "CAST(value AS BIGINT)"

    print(f"Consolidating ETH transactions → {eth_out} ...")
    print(f"  (this may take a few minutes for large datasets)")
    con.execute(f"""
        COPY (
            SELECT
                hash AS tx_hash,
                block_timestamp,
                from_address,
                to_address,
                {value_expr} AS value,
                CAST({gas_price_col} AS BIGINT) AS gas_price,
                block_number
            FROM '{eth_pattern}'
            ORDER BY block_timestamp
        ) TO '{eth_out}' (FORMAT PARQUET, ROW_GROUP_SIZE 500000)
    """)

    # ── Token Transfers ───────────────────────────────────────────────
    tok_pattern = os.path.join(args.raw_dir, 'token_transfers', '**', '*.parquet')
    tok_out = os.path.join(args.out_dir, 'token_transfers.parquet')

    print("\nInspecting raw token transfer schema...")
    try:
        schema = inspect_schema(con, tok_pattern)
        print(f"  Columns: {list(schema.keys())}")
    except Exception as e:
        print(f"  WARNING: {e} — token transfers may not be downloaded yet")
        schema = {}

    if schema:
        tok_value_type = schema.get('value', 'UNKNOWN')
        if 'STRING' in tok_value_type.upper() or 'VARCHAR' in tok_value_type.upper():
            tok_value_expr = "TRY_CAST(value AS DOUBLE)"
        else:
            tok_value_expr = "CAST(value AS BIGINT)"

        print(f"Consolidating token transfers → {tok_out} ...")
        con.execute(f"""
            COPY (
                SELECT
                    token_address,
                    from_address,
                    to_address,
                    {tok_value_expr} AS value,
                    block_timestamp
                FROM '{tok_pattern}'
                ORDER BY block_timestamp
            ) TO '{tok_out}' (FORMAT PARQUET, ROW_GROUP_SIZE 500000)
        """)

    # ── Summary ───────────────────────────────────────────────────────
    print("\n=== Prepared files ===")
    report_table(con, eth_out, 'eth_transactions')
    if schema:
        report_table(con, tok_out, 'token_transfers')

    prices_path = os.path.join(args.out_dir, 'prices.parquet')
    if os.path.exists(prices_path):
        size_mb = os.path.getsize(prices_path) / 1e6
        count = con.execute(f"SELECT COUNT(*) FROM '{prices_path}'").fetchone()[0]
        symbols = [r[0] for r in con.execute(
            f"SELECT DISTINCT symbol FROM '{prices_path}' ORDER BY 1"
        ).fetchall()]
        print(f"  prices: {count:,} rows, {size_mb:.1f} MB, symbols: {', '.join(symbols)}")
    else:
        print(f"  prices: NOT FOUND — run: python scripts/download_prices.py")

    # ── Find hot address for Q02 ──────────────────────────────────────
    print("\nTop 5 addresses by transaction count (use one in queries/q02_address_activity.sql):")
    rows = con.execute(f"""
        SELECT from_address, COUNT(*) AS cnt
        FROM '{eth_out}'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 5
    """).fetchall()
    for addr, cnt in rows:
        print(f"  {addr}  ({cnt:,} transactions)")

    print("\nNext step: python scripts/validate_queries.py --mode cpu-only")


if __name__ == '__main__':
    main()
