#!/usr/bin/env python3
"""Download historical crypto price data from CoinGecko (free tier).

Fetches hourly ETH/USD prices (and optionally other coins) and writes
a parquet file at data/prices.parquet with schema:
  ts TIMESTAMP, symbol VARCHAR, price_usd DOUBLE

Usage:
  python scripts/download_prices.py [--data-dir data] [--days 365]
"""
import argparse
import os
import time
import json
import urllib.request
import urllib.parse
import duckdb


# CoinGecko coin IDs → symbol names
COINS = {
    'ethereum': 'ETH',
    'bitcoin': 'BTC',
    'tether': 'USDT',
    'usd-coin': 'USDC',
    'dai': 'DAI',
    'chainlink': 'LINK',
    'uniswap': 'UNI',
    'wrapped-bitcoin': 'WBTC',
}


def fetch_coin_prices(coin_id: str, days: int) -> list[dict]:
    """Fetch hourly price history from CoinGecko free API."""
    params = urllib.parse.urlencode({
        'vs_currency': 'usd',
        'days': str(days),
    })
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?{params}"
    req = urllib.request.Request(url, headers={'User-Agent': 'sirius-crypto-demo/1.0'})

    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())

    # data['prices'] = [[timestamp_ms, price], ...]
    return [
        {'ts_ms': int(ts_ms), 'symbol': COINS[coin_id], 'price_usd': float(price)}
        for ts_ms, price in data['prices']
    ]


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--data-dir', default='data')
    p.add_argument('--days', type=int, default=365,
                   help='Days of history (free tier max: 365)')
    p.add_argument('--coins', nargs='+', default=list(COINS.keys()),
                   help='CoinGecko coin IDs to fetch')
    args = p.parse_args()

    os.makedirs(args.data_dir, exist_ok=True)
    out_path = os.path.join(args.data_dir, 'prices.parquet')

    all_rows = []
    for coin_id in args.coins:
        if coin_id not in COINS:
            print(f"WARNING: Unknown coin ID '{coin_id}', skipping")
            continue
        symbol = COINS[coin_id]
        print(f"Fetching {symbol} ({coin_id})...", end=' ', flush=True)
        try:
            rows = fetch_coin_prices(coin_id, args.days)
            all_rows.extend(rows)
            print(f"{len(rows):,} rows")
        except Exception as e:
            print(f"ERROR: {e}")
            print(f"  Skipping {symbol}. Try again later or use CryptoDataDownload CSV.")

        # Free tier: ~10-15 calls/min → sleep 6s between coins
        if coin_id != args.coins[-1]:
            time.sleep(6)

    if not all_rows:
        print("No price data fetched. Exiting.")
        return

    print(f"\nWriting {len(all_rows):,} total price rows to {out_path}...")
    con = duckdb.connect()
    con.execute("CREATE TABLE _prices AS SELECT * FROM all_rows")
    con.execute(f"""
        COPY (
            SELECT
                epoch_ms(ts_ms) AS ts,
                symbol,
                price_usd
            FROM _prices
            ORDER BY symbol, ts
        ) TO '{out_path}' (FORMAT PARQUET)
    """)

    # Verify
    count = con.execute(f"SELECT COUNT(*) FROM '{out_path}'").fetchone()[0]
    min_ts, max_ts = con.execute(
        f"SELECT MIN(ts)::DATE, MAX(ts)::DATE FROM '{out_path}'"
    ).fetchone()
    symbols = [r[0] for r in con.execute(
        f"SELECT DISTINCT symbol FROM '{out_path}' ORDER BY 1"
    ).fetchall()]
    print(f"  {count:,} rows, {min_ts} → {max_ts}")
    print(f"  Symbols: {', '.join(symbols)}")
    print(f"\nNext step: python scripts/prepare_demo_data.py")


if __name__ == '__main__':
    main()
