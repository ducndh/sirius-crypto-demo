#!/usr/bin/env python3
"""Download Ethereum token transfers from AWS Public Blockchain Data.

S3 bucket: s3://aws-public-blockchain/v1.0/eth/token_transfers/
Format: Parquet, partitioned by date
No AWS credentials required (public bucket, --no-sign-request).

Requires: pip install boto3 (or awscli for s3 sync)
"""
import os
import sys
import subprocess
import argparse
from datetime import datetime, timedelta

BUCKET = "aws-public-blockchain"
PREFIX = "v1.0/eth/token_transfers"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "eth_transfers")


def download_with_aws_cli(start_date, end_date):
    """Download using aws s3 sync with date filtering."""
    os.makedirs(DATA_DIR, exist_ok=True)

    current = start_date
    while current <= end_date:
        date_str = current.strftime("date=%Y-%m-%d")
        s3_path = f"s3://{BUCKET}/{PREFIX}/{date_str}/"
        local_path = os.path.join(DATA_DIR, date_str)

        if os.path.exists(local_path) and any(f.endswith('.parquet') for f in os.listdir(local_path)):
            print(f"  {date_str} — already downloaded, skipping")
            current += timedelta(days=1)
            continue

        print(f"  Downloading {date_str}...")
        try:
            subprocess.run([
                "aws", "s3", "sync", s3_path, local_path,
                "--no-sign-request",
                "--quiet"
            ], check=True)
        except subprocess.CalledProcessError as e:
            print(f"  WARNING: Failed to download {date_str}: {e}")

        current += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description="Download Ethereum token transfers from AWS")
    parser.add_argument("--months", type=int, default=1,
                        help="Number of months to download (default: 1)")
    parser.add_argument("--start-date", type=str, default="2024-01-01",
                        help="Start date YYYY-MM-DD (default: 2024-01-01)")
    parser.add_argument("--end-date", type=str, default=None,
                        help="End date YYYY-MM-DD (overrides --months)")
    args = parser.parse_args()

    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    if args.end_date:
        end = datetime.strptime(args.end_date, "%Y-%m-%d")
    else:
        # Approximate months
        end = start + timedelta(days=30 * args.months)

    print(f"Downloading Ethereum token transfers: {start.date()} to {end.date()}")
    print(f"Target directory: {DATA_DIR}")
    print(f"Source: s3://{BUCKET}/{PREFIX}/")
    print()

    # Check aws CLI availability
    try:
        subprocess.run(["aws", "--version"], capture_output=True, check=True)
    except FileNotFoundError:
        print("ERROR: aws CLI not found. Install with: pip install awscli")
        sys.exit(1)

    download_with_aws_cli(start, end)

    # Summary
    total_files = 0
    total_bytes = 0
    for root, dirs, files in os.walk(DATA_DIR):
        for f in files:
            if f.endswith('.parquet'):
                total_files += 1
                total_bytes += os.path.getsize(os.path.join(root, f))

    print(f"\nDownload complete:")
    print(f"  Parquet files: {total_files}")
    print(f"  Total size: {total_bytes / 1024 / 1024 / 1024:.2f} GB")


if __name__ == "__main__":
    main()
