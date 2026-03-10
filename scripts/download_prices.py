#!/usr/bin/env python3
"""Download historical ETH/USD price data for the ASOF JOIN demo queries.

Primary source: CryptoDataDownload (free, direct CSV, full historical depth)
  - Binance ETHUSDT hourly OHLCV
  - No account, no API key, single curl-style download
  - Covers all years back to 2017

Fallback: CoinGecko API (free tier, last 365 days only — NOT suitable for historical data)

Output: data/prices.parquet with schema:
  ts TIMESTAMP, symbol VARCHAR, price_usd DOUBLE

Usage:
  python scripts/download_prices.py --start 2024-01-01 --end 2024-04-11
  python scripts/download_prices.py  # auto-detects range from eth_transactions.parquet
"""
import argparse
import os
import urllib.request
import duckdb


CDD_URL = "https://www.cryptodatadownload.com/cdd/Binance_ETHUSDT_1h.csv"


def detect_tx_range(data_dir: str) -> tuple[str, str]:
    """Read date range from existing eth_transactions.parquet."""
    path = os.path.join(data_dir, "eth_transactions.parquet")
    if not os.path.exists(path):
        return "2024-01-01", "2024-12-31"
    con = duckdb.connect()
    row = con.execute(
        f"SELECT MIN(block_timestamp)::DATE::VARCHAR, MAX(block_timestamp)::DATE::VARCHAR "
        f"FROM '{path}'"
    ).fetchone()
    return row[0], row[1]


def download_cdd(tmp_path: str):
    """Download the CryptoDataDownload ETH/USDT hourly CSV."""
    print(f"Downloading from CryptoDataDownload...", end=" ", flush=True)
    req = urllib.request.Request(
        CDD_URL, headers={"User-Agent": "sirius-crypto-demo/1.0"}
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read()
    with open(tmp_path, "wb") as f:
        f.write(data)
    print(f"{len(data)/1e6:.1f} MB")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", default="data")
    p.add_argument("--start", default=None,
                   help="Start date (default: auto-detect from eth_transactions.parquet)")
    p.add_argument("--end", default=None,
                   help="End date (default: auto-detect from eth_transactions.parquet)")
    args = p.parse_args()

    os.makedirs(args.data_dir, exist_ok=True)
    out_path = os.path.join(args.data_dir, "prices.parquet")
    tmp_csv = os.path.join(args.data_dir, "_prices_raw.csv")

    # Auto-detect date range from transactions if not specified
    start, end = args.start, args.end
    if not start or not end:
        detected_start, detected_end = detect_tx_range(args.data_dir)
        start = start or detected_start
        # Add 1 day buffer on end to ensure full coverage
        from datetime import date, timedelta
        end_date = date.fromisoformat(end or detected_end) + timedelta(days=1)
        end = end_date.isoformat()
        print(f"Auto-detected range from eth_transactions.parquet: {start} → {end}")

    try:
        download_cdd(tmp_csv)
        con = duckdb.connect()

        # CDD CSV has a URL as first line, then header, then data
        # Columns: Unix (epoch_ms), Date, Symbol, Open, High, Low, Close, Volume ETH, Volume USDT, tradecount
        con.execute(f"""
            COPY (
                SELECT
                    epoch_ms(Unix) AS ts,
                    'ETH'          AS symbol,
                    Close          AS price_usd
                FROM read_csv('{tmp_csv}',
                    skip=1,
                    header=true,
                    columns={{
                        'Unix': 'BIGINT', 'Date': 'VARCHAR', 'Symbol': 'VARCHAR',
                        'Open': 'DOUBLE', 'High': 'DOUBLE', 'Low': 'DOUBLE',
                        'Close': 'DOUBLE', 'Volume ETH': 'DOUBLE',
                        'Volume USDT': 'DOUBLE', 'tradecount': 'BIGINT'
                    }}
                )
                WHERE epoch_ms(Unix) BETWEEN '{start}' AND '{end}'
                ORDER BY ts
            ) TO '{out_path}' (FORMAT PARQUET)
        """)

        count, min_ts, max_ts = con.execute(
            f"SELECT COUNT(*), MIN(ts)::DATE, MAX(ts)::DATE FROM '{out_path}'"
        ).fetchone()
        print(f"  {count:,} hourly ETH/USD prices: {min_ts} → {max_ts}")

    finally:
        if os.path.exists(tmp_csv):
            os.unlink(tmp_csv)

    print(f"\nNext step: python scripts/validate_queries.py --mode cpu-only")


if __name__ == "__main__":
    main()
