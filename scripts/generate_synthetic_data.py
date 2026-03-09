#!/usr/bin/env python3
"""FALLBACK ONLY: Generate synthetic crypto data matching the real AWS schema.

Use this ONLY when AWS download is unavailable.
DO NOT use synthetic data for demos or slides — it's only for pipeline testing.

The synthetic data has:
- Same column names/types as real AWS data
- Same table structure as prepare_demo_data.py output
- Realistic-ish distributions (power-law addresses, monotone timestamps)

Usage:
  python scripts/generate_synthetic_data.py --scale dev
  python scripts/generate_synthetic_data.py --scale rtx6000
"""
import argparse
import os
import duckdb

SCALES = {
    'dev':     {'eth_txns': 1_000_000,   'token_transfers': 500_000,    'price_hours': 8_760},
    'rtx6000': {'eth_txns': 35_000_000,  'token_transfers': 20_000_000, 'price_hours': 8_760},
    'h100':    {'eth_txns': 200_000_000, 'token_transfers': 150_000_000, 'price_hours': 8_760},
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--scale', default='dev', choices=list(SCALES.keys()))
    p.add_argument('--data-dir', default='data')
    args = p.parse_args()

    print("=" * 60)
    print("WARNING: Generating SYNTHETIC data.")
    print("This is for pipeline testing only.")
    print("Use real data for demos: bash scripts/download_eth_data.sh")
    print("=" * 60)
    print()

    s = SCALES[args.scale]
    os.makedirs(args.data_dir, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET threads=4")

    # ETH Transactions
    eth_out = f"{args.data_dir}/eth_transactions.parquet"
    print(f"Generating {s['eth_txns']:,} synthetic ETH transactions → {eth_out}")
    con.execute(f"""
        COPY (
            SELECT
                md5(i::VARCHAR) AS tx_hash,
                '2024-01-01'::TIMESTAMP + INTERVAL (i * 2) SECOND AS block_timestamp,
                -- Power-law-ish: top 100 addresses get many txns, long tail sparse
                printf('0x%040x',
                    CASE WHEN (abs(hash(i * 7)) % 100) < 5
                         THEN (abs(hash(i * 7)) % 5)        -- top 5 addresses: ~5% of txns
                         ELSE (abs(hash(i * 7)) % 100000)   -- 100K addresses: 95%
                    END
                ) AS from_address,
                printf('0x%040x', abs(hash(i * 13)) % 100000) AS to_address,
                -- wei values: log-normal-ish (most small, some large)
                CASE
                    WHEN abs(hash(i * 3)) % 100 < 60 THEN abs(hash(i * 3)) % 1000000000000000     -- <0.001 ETH
                    WHEN abs(hash(i * 3)) % 100 < 90 THEN abs(hash(i * 3)) % 1000000000000000000  -- <1 ETH
                    ELSE (abs(hash(i * 3)) % 100 + 1)::DOUBLE * 1e18                              -- 1-100 ETH
                END::DOUBLE AS value,
                (abs(hash(i * 5)) % 500 + 10)::BIGINT AS gas_price,
                (19800000 + i / 12)::BIGINT AS block_number
            FROM generate_series(1, {s['eth_txns']}) t(i)
        ) TO '{eth_out}' (FORMAT PARQUET, ROW_GROUP_SIZE 500000)
    """)

    # Token Transfers
    tok_out = f"{args.data_dir}/token_transfers.parquet"
    print(f"Generating {s['token_transfers']:,} synthetic token transfers → {tok_out}")
    con.execute(f"""
        COPY (
            SELECT
                printf('0x%040x', abs(hash(i * 17)) % 50) AS token_address,
                printf('0x%040x', abs(hash(i * 7)) % 100000) AS from_address,
                printf('0x%040x', abs(hash(i * 13)) % 100000) AS to_address,
                (abs(hash(i * 3)) % 1000000)::DOUBLE AS value,
                '2024-01-01'::TIMESTAMP + INTERVAL (i * 4) SECOND AS block_timestamp
            FROM generate_series(1, {s['token_transfers']}) t(i)
        ) TO '{tok_out}' (FORMAT PARQUET, ROW_GROUP_SIZE 500000)
    """)

    # Prices (hourly, 1 year)
    prices_out = f"{args.data_dir}/prices.parquet"
    print(f"Generating {s['price_hours']:,} synthetic price rows → {prices_out}")
    con.execute(f"""
        COPY (
            SELECT
                '2024-01-01'::TIMESTAMP + INTERVAL (i * 3600) SECOND AS ts,
                'ETH' AS symbol,
                2000.0 + 800.0 * sin(i * 0.01) + (abs(hash(i)) % 200) * 0.5 AS price_usd
            FROM generate_series(1, {s['price_hours']}) t(i)
        ) TO '{prices_out}' (FORMAT PARQUET)
    """)

    print()
    print("=== Synthetic data ready ===")
    for label, path in [('eth_transactions', eth_out), ('token_transfers', tok_out), ('prices', prices_out)]:
        size_mb = os.path.getsize(path) / 1e6
        count = con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
        print(f"  {label}: {count:,} rows, {size_mb:.1f} MB")

    print()
    print("Next step: python scripts/validate_queries.py --mode cpu-only")


if __name__ == '__main__':
    main()
