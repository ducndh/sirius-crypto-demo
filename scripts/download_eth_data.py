#!/usr/bin/env python3
"""Download real Ethereum transaction data from AWS Public Blockchain Data.

No AWS account or CLI needed — the bucket is publicly accessible.
Uses DuckDB httpfs to read from S3 with column projection (only fetches
the 7 columns we need, not all 22).

Data source: s3://aws-public-blockchain/v1.0/eth/
Region:      us-east-2
Auth:        anonymous (public dataset)

Usage:
  python scripts/download_eth_data.py --scale dev
  python scripts/download_eth_data.py --scale rtx6000
  python scripts/download_eth_data.py --scale h100

Scale targets:
  dev      7 days  ~8-10M txns   ETH ~2GB download  token_transfers ~860MB
  rtx6000  30 days ~35-40M txns  ETH ~8GB download  token_transfers ~3.7GB
  h100     180 days ~200M+ txns  ETH ~50GB download token_transfers ~22GB
"""
import argparse
import os
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import duckdb
from datetime import date, timedelta

S3_BASE = "s3://aws-public-blockchain/v1.0/eth"
S3_HTTPS_BASE = "https://aws-public-blockchain.s3.us-east-2.amazonaws.com"
S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"

SCALES = {
    "dev":      ("2024-01-01", "2024-01-07"),   # 7 days
    "rtx6000":  ("2024-01-01", "2024-01-31"),   # 30 days
    "h100":     ("2024-01-01", "2024-06-30"),   # 180 days
    "h100-max": ("2024-01-01", "2024-12-31"),   # 365 days
}

# Columns we actually use — column projection avoids downloading unneeded data
ETH_TX_COLS = "hash, block_timestamp, from_address, to_address, value, gas_price, block_number"
TOKEN_TX_COLS = "token_address, from_address, to_address, value, block_timestamp"


def daterange(start: str, end: str):
    """Yield date strings from start to end (inclusive)."""
    d = date.fromisoformat(start)
    e = date.fromisoformat(end)
    while d <= e:
        yield d.isoformat()
        d += timedelta(days=1)


def list_s3_files(prefix: str) -> list[str]:
    """List object keys in the public S3 bucket under prefix. No auth needed."""
    encoded_prefix = urllib.parse.quote(prefix, safe="/=")
    url = f"{S3_HTTPS_BASE}/?list-type=2&prefix={encoded_prefix}"
    req = urllib.request.Request(url, headers={"User-Agent": "sirius-crypto-demo/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        tree = ET.parse(resp)
    keys = [k.text for k in tree.findall(f".//{{{S3_NS}}}Key")]
    return keys


def setup_duckdb() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with httpfs configured for anonymous S3."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region = 'us-east-2';")
    # Anonymous access: empty credentials
    con.execute("SET s3_access_key_id = '';")
    con.execute("SET s3_secret_access_key = '';")
    con.execute("SET s3_use_ssl = true;")
    return con


def download_table(
    con: duckdb.DuckDBPyConnection,
    table_type: str,    # "transactions" or "token_transfers"
    select_cols: str,
    rename_map: dict,   # {original_col: new_col_name}
    out_path: str,
    start: str,
    end: str,
):
    """Download one or more days of data from S3 → local parquet."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    all_s3_paths = []
    missing_days = []

    print(f"  Listing available files ({start} → {end})...", end=" ", flush=True)
    for day in daterange(start, end):
        prefix = f"v1.0/eth/{table_type}/date={day}/"
        try:
            keys = list_s3_files(prefix)
            for key in keys:
                if key.endswith(".parquet"):
                    # key is relative to bucket root, e.g. "v1.0/eth/transactions/date=.../part-xxx.parquet"
                    all_s3_paths.append(f"s3://aws-public-blockchain/{key}")
        except Exception as e:
            missing_days.append(day)

    print(f"{len(all_s3_paths)} parquet files found")
    if missing_days:
        print(f"  WARNING: No data for {len(missing_days)} days: {missing_days[:3]}...")

    if not all_s3_paths:
        print(f"  ERROR: No files found for {table_type} in range {start}–{end}")
        return 0

    # Build column expressions with renaming
    select_expr = ", ".join(
        f"{col} AS {rename_map.get(col, col)}" if col in rename_map else col
        for col in select_cols.split(", ")
    )

    # Build S3 glob list for DuckDB — use read_parquet([...]) for explicit file list
    file_list = "[" + ", ".join(f"'{p}'" for p in all_s3_paths) + "]"

    print(f"  Downloading {len(all_s3_paths)} files via DuckDB httpfs (column projection)...")
    print(f"  Output → {out_path}")

    row_count = con.execute(f"""
        COPY (
            SELECT {select_expr}
            FROM read_parquet({file_list})
            ORDER BY block_timestamp
        ) TO '{out_path}' (FORMAT PARQUET, ROW_GROUP_SIZE 500000)
    """).fetchone()

    count = con.execute(f"SELECT COUNT(*) FROM '{out_path}'").fetchone()[0]
    size_mb = os.path.getsize(out_path) / 1e6
    return count, size_mb


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scale", default="dev", choices=list(SCALES.keys()))
    p.add_argument("--data-dir", default="data")
    p.add_argument("--skip-token-transfers", action="store_true",
                   help="Only download transactions (faster)")
    args = p.parse_args()

    start, end = SCALES[args.scale]
    os.makedirs(args.data_dir, exist_ok=True)

    print(f"Sirius Crypto Demo — Download Real Ethereum Data")
    print(f"  Scale:  {args.scale} ({start} → {end})")
    print(f"  Source: AWS Public Blockchain (anonymous, no account needed)")
    print(f"  Output: {args.data_dir}/")
    print()

    con = setup_duckdb()

    # ── ETH Transactions ─────────────────────────────────────────────
    print("ETH Transactions")
    eth_out = os.path.join(args.data_dir, "eth_transactions.parquet")
    result = download_table(
        con,
        table_type="transactions",
        select_cols="hash, block_timestamp, from_address, to_address, value, gas_price, block_number",
        rename_map={"hash": "tx_hash"},
        out_path=eth_out,
        start=start, end=end,
    )
    if result:
        count, size_mb = result
        print(f"  ✓ {count:,} rows, {size_mb:.0f} MB local")
    print()

    # ── Token Transfers ───────────────────────────────────────────────
    if not args.skip_token_transfers:
        print("Token Transfers")
        # Check actual schema of token_transfers
        try:
            tok_cols_raw = con.execute(
                f"DESCRIBE SELECT * FROM '{S3_BASE}/token_transfers/date={start}/*.parquet' LIMIT 0"
            ).fetchall()
            tok_col_names = {r[0] for r in tok_cols_raw}

            # token_transfers may have 'contract_address' instead of 'token_address'
            token_addr_col = "contract_address" if "contract_address" in tok_col_names else "token_address"
            select_tok = f"{token_addr_col}, from_address, to_address, value, block_timestamp"
            rename_tok = {token_addr_col: "token_address"}
        except Exception:
            select_tok = "token_address, from_address, to_address, value, block_timestamp"
            rename_tok = {}

        tok_out = os.path.join(args.data_dir, "token_transfers.parquet")
        result = download_table(
            con,
            table_type="token_transfers",
            select_cols=select_tok,
            rename_map=rename_tok,
            out_path=tok_out,
            start=start, end=end,
        )
        if result:
            count, size_mb = result
            print(f"  ✓ {count:,} rows, {size_mb:.0f} MB local")
        print()

    # ── Summary ───────────────────────────────────────────────────────
    print("=== Download complete ===")
    for f in ["eth_transactions.parquet", "token_transfers.parquet"]:
        path = os.path.join(args.data_dir, f)
        if os.path.exists(path):
            size_mb = os.path.getsize(path) / 1e6
            count = con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
            ts = con.execute(
                f"SELECT MIN(block_timestamp)::DATE, MAX(block_timestamp)::DATE FROM '{path}'"
            ).fetchone()
            print(f"  {f}: {count:,} rows, {size_mb:.0f} MB, {ts[0]} → {ts[1]}")

    print()
    prices_path = os.path.join(args.data_dir, "prices.parquet")
    if not os.path.exists(prices_path):
        print("Next step: python scripts/download_prices.py")
    else:
        print("All data ready. Next step: python scripts/validate_queries.py --mode cpu-only")


if __name__ == "__main__":
    main()
