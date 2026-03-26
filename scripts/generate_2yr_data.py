#!/usr/bin/env python3
"""Generate multi-year address_flows_daily_dict by downloading real ETH data quarter by quarter.

Strategy:
  1. Copy static entity tables from the slim DB (MBAL labels don't change)
  2. For each quarter (default: Jan 2024 - Mar 2026):
     a. Download raw token_transfers parquet from AWS S3 to /tmp
     b. Aggregate into address_flows_daily (VARCHAR addresses)
     c. Dictionary-encode addresses → addr_id using existing address_dictionary
     d. Append to output address_flows_daily_dict table
     e. Delete raw parquet to free disk
  3. Verify final dataset

Output: /tmp/crypto_2yr.duckdb

Disk budget (all on /tmp local SSD):
  - Raw download per quarter: ~37 GB (92 days * 400 MB/day)
  - Output DB grows to ~17-20 GB
  - Peak: ~57 GB (well within /tmp's 235 GB free)

IMPORTANT: Writes to /tmp (local SSD), NOT JuiceFS /home.

Usage:
  python3 generate_2yr_data.py [--start 2024-01-01] [--end 2026-03-24] [--quarter-months 3]
  python3 generate_2yr_data.py --resume  # continue after failure
"""
import argparse
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Paths — override with env vars or CLI flags
# ---------------------------------------------------------------------------
SIRIUS_DIR = os.environ.get("SIRIUS_DIR", os.path.expanduser("~/sirius"))
SIRIUS_BIN = os.path.join(SIRIUS_DIR, "build/release/duckdb")
PIXI_ENV = os.path.join(SIRIUS_DIR, ".pixi/envs/default")
SOURCE_DB = os.path.expanduser("~/sirius-crypto-demo/data/crypto_demo_2025_slim.duckdb")
OUTPUT_DB = "/tmp/crypto_2yr.duckdb"
RAW_DIR = "/tmp/eth_transfers_raw"
AWS_CLI = os.environ.get("AWS_CLI", shutil.which("aws") or
                         os.path.expanduser("~/micromamba/bin/aws"))

S3_BUCKET = "aws-public-blockchain"
S3_PREFIX = "v1.0/eth/token_transfers"


def run_sql(db_path, sql, timeout=7200):
    """Execute SQL via Sirius DuckDB binary."""
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = f"{PIXI_ENV}/lib:{env.get('LD_LIBRARY_PATH', '')}"
    result = subprocess.run(
        [SIRIUS_BIN, db_path, "-unsigned", "-c", sql],
        capture_output=True, text=True, env=env, timeout=timeout
    )
    if result.returncode != 0:
        print(f"  STDERR: {result.stderr[:2000]}")
    if result.stdout.strip():
        print(result.stdout.strip())
    return result.returncode


def download_quarter(start_date, end_date):
    """Download raw parquet files from S3 for a date range. Returns download dir."""
    os.makedirs(RAW_DIR, exist_ok=True)
    current = start_date
    total_bytes = 0
    days = 0
    while current <= end_date:
        date_str = current.strftime("date=%Y-%m-%d")
        s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{date_str}/"
        local_path = os.path.join(RAW_DIR, date_str)

        if os.path.exists(local_path) and any(
            f.endswith('.parquet') for f in os.listdir(local_path)
        ):
            for f in os.listdir(local_path):
                if f.endswith('.parquet'):
                    total_bytes += os.path.getsize(os.path.join(local_path, f))
            days += 1
            current += timedelta(days=1)
            continue

        try:
            subprocess.run(
                [AWS_CLI, "s3", "sync", s3_path, local_path,
                 "--no-sign-request", "--quiet"],
                check=True, timeout=300
            )
            for f in os.listdir(local_path):
                if f.endswith('.parquet'):
                    total_bytes += os.path.getsize(os.path.join(local_path, f))
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            print(f"    WARNING: Failed {date_str}: {e}")

        days += 1
        if days % 10 == 0:
            print(f"    Downloaded {days} days ({total_bytes / 1e9:.1f} GB)...")
        current += timedelta(days=1)

    print(f"    Total: {days} days, {total_bytes / 1e9:.1f} GB")
    return RAW_DIR


def cleanup_raw():
    """Delete raw parquet files to free disk."""
    if os.path.exists(RAW_DIR):
        shutil.rmtree(RAW_DIR)
        print("    Cleaned up raw data")


def aggregate_quarter(start_date, end_date, quarter_label):
    """Aggregate raw parquet → address_flows_daily_dict, append to output DB."""
    year_pattern = start_date.strftime("%Y")
    parquet_glob = f"{RAW_DIR}/date={year_pattern}-*/*.parquet"

    # If quarter spans year boundary (e.g., Nov 2024 - Jan 2025), use broader glob
    if start_date.year != end_date.year:
        parquet_glob = f"{RAW_DIR}/date=*/*.parquet"

    # Step 1: Aggregate raw → temp table with VARCHAR addresses
    # Step 2: Dictionary-encode using address_dictionary from output DB
    # Step 3: Append to address_flows_daily_dict
    # Step 4: Drop temp table
    rc = run_sql(OUTPUT_DB, f"""
        -- Load raw token transfers into temp table
        CREATE TEMPORARY TABLE _raw_transfers AS
        SELECT *
        FROM read_parquet('{parquet_glob}', hive_partitioning=true)
        WHERE date >= '{start_date.strftime("%Y-%m-%d")}'
          AND date <= '{end_date.strftime("%Y-%m-%d")}';

        -- Aggregate into daily flows with normalized addresses (66→42 char)
        CREATE TEMPORARY TABLE _quarter_flows AS
        SELECT
            date,
            token_address AS asset,
            '0x' || SUBSTR(from_address, 27) AS from_address,
            '0x' || SUBSTR(to_address, 27)   AS to_address,
            SUM(value) AS amount,
            CAST(COUNT(*) AS BIGINT) AS tx_count
        FROM _raw_transfers
        GROUP BY 1, 2, 3, 4;

        DROP TABLE _raw_transfers;

        -- Dictionary-encode and append
        INSERT INTO address_flows_daily_dict
        SELECT
            f.date, f.asset, f.amount, f.tx_count,
            COALESCE(d_from.addr_id, 0) AS from_addr_id,
            COALESCE(d_to.addr_id, 0)   AS to_addr_id
        FROM _quarter_flows f
        LEFT JOIN address_dictionary d_from ON f.from_address = d_from.address
        LEFT JOIN address_dictionary d_to   ON f.to_address   = d_to.address;

        DROP TABLE _quarter_flows;
    """, timeout=7200)

    return rc


def main():
    parser = argparse.ArgumentParser(description="Generate 2-year crypto dataset from real ETH data")
    parser.add_argument("--start", default="2024-01-01", help="Start date (default: 2024-01-01)")
    parser.add_argument("--end", default="2026-03-24", help="End date (default: 2026-03-24, yesterday)")
    parser.add_argument("--quarter-months", type=int, default=3,
                        help="Months per download batch (default: 3)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from existing output DB (skip entity table copy)")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d")
    end = datetime.strptime(args.end, "%Y-%m-%d")

    # Preflight checks
    if not os.path.isfile(SIRIUS_BIN):
        print(f"ERROR: Sirius binary not found at {SIRIUS_BIN}")
        print(f"  Build with: cd {SIRIUS_DIR} && pixi run make release")
        sys.exit(1)

    if not os.path.isfile(SOURCE_DB):
        print(f"ERROR: Source DB not found at {SOURCE_DB}")
        sys.exit(1)

    if not os.path.isfile(AWS_CLI):
        print(f"ERROR: aws CLI not found at {AWS_CLI}")
        sys.exit(1)

    print(f"Source DB:  {SOURCE_DB}")
    print(f"Output DB:  {OUTPUT_DB} (local SSD)")
    print(f"Raw dir:    {RAW_DIR}")
    print(f"Date range: {start.date()} to {end.date()}")
    print()

    # Step 1: Initialize output DB with entity tables from slim DB
    if not args.resume:
        if os.path.exists(OUTPUT_DB):
            print(f"Removing existing {OUTPUT_DB}")
            os.remove(OUTPUT_DB)
            wal = OUTPUT_DB + ".wal"
            if os.path.exists(wal):
                os.remove(wal)

        print("Step 1: Copying entity tables from source...")
        t0 = time.time()
        rc = run_sql(OUTPUT_DB, f"""
            ATTACH '{SOURCE_DB}' AS src (READ_ONLY);

            -- Copy entity tables (small, static)
            CREATE TABLE entity_address_map_int AS SELECT * FROM src.entity_address_map_int;
            CREATE TABLE entity_lookup AS SELECT * FROM src.entity_lookup;
            CREATE TABLE entity_address_map_dict AS SELECT * FROM src.entity_address_map_dict;
            CREATE TABLE address_dictionary AS SELECT * FROM src.address_dictionary;

            -- Create empty flows table
            CREATE TABLE address_flows_daily_dict (
                date DATE,
                asset VARCHAR,
                amount DOUBLE,
                tx_count BIGINT,
                from_addr_id INTEGER,
                to_addr_id INTEGER
            );

            DETACH src;
        """)
        if rc != 0:
            print("FAILED to initialize output DB")
            sys.exit(1)
        print(f"  Done in {time.time()-t0:.1f}s")
    else:
        print("Step 1: Resuming — skipping entity table copy")

    # Step 2: Process each quarter
    quarter_start = start
    quarter_num = 0
    while quarter_start <= end:
        quarter_end = quarter_start + timedelta(days=args.quarter_months * 30)
        if quarter_end > end:
            quarter_end = end
        quarter_num += 1
        label = f"{quarter_start.strftime('%Y-%m')} to {quarter_end.strftime('%Y-%m')}"

        print(f"\n{'='*60}")
        print(f"Quarter {quarter_num}: {label}")
        print(f"{'='*60}")

        # 2a. Download
        print(f"  Downloading raw data...")
        t0 = time.time()
        download_quarter(quarter_start, quarter_end)
        dl_time = time.time() - t0
        print(f"  Download: {dl_time:.0f}s")

        # 2b. Aggregate and append
        print(f"  Aggregating and appending...")
        t0 = time.time()
        rc = aggregate_quarter(quarter_start, quarter_end, label)
        agg_time = time.time() - t0
        if rc != 0:
            print(f"  FAILED at quarter {quarter_num}")
            print("  Raw data preserved for debugging. Re-run with --resume to continue.")
            sys.exit(1)
        print(f"  Aggregate: {agg_time:.0f}s")

        # 2c. Cleanup raw
        cleanup_raw()

        # Progress
        run_sql(OUTPUT_DB, "SELECT COUNT(*) AS total_rows FROM address_flows_daily_dict;")
        db_size = os.path.getsize(OUTPUT_DB) / 1e9
        print(f"  Output DB: {db_size:.1f} GB")

        quarter_start = quarter_end + timedelta(days=1)

    # Step 3: Verify
    print(f"\n{'='*60}")
    print("Verification")
    print(f"{'='*60}")
    run_sql(OUTPUT_DB, """
        SELECT COUNT(*) AS total_rows FROM address_flows_daily_dict;
        SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM address_flows_daily_dict;
        SELECT date_trunc('month', date)::DATE AS month, COUNT(*) AS rows
        FROM address_flows_daily_dict GROUP BY 1 ORDER BY 1;
    """)

    size_gb = os.path.getsize(OUTPUT_DB) / 1e9
    print(f"\nOutput: {OUTPUT_DB} ({size_gb:.1f} GB)")
    print("Done!")


if __name__ == "__main__":
    main()
