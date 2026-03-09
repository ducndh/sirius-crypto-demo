#!/usr/bin/env python3
"""Validate all demo queries: run on CPU and/or GPU, compare row counts.

Usage:
  python scripts/validate_queries.py --mode cpu-only
  python scripts/validate_queries.py --mode gpu-only
  python scripts/validate_queries.py --mode both
"""
import argparse
import os
import re
import subprocess
import tempfile
import time
import duckdb

QUERIES = [
    'q01_count_range',
    'q02_address_activity',
    'q03_top_addresses',
    'q04_price_match',
    'q05_daily_volume',
    'q06_top_senders_usd',
    'q07_token_volumes',
    'q08_hourly_gas',
    'q09_address_pairs',
    'q10_block_stats',
]

SIRIUS_PIXI_ENV = os.environ.get(
    'SIRIUS_PIXI_ENV', '/home/cc/sirius-asof/.pixi/envs/cuda12'
)
SIRIUS_BIN = os.environ.get(
    'SIRIUS_BIN', '/home/cc/sirius-asof/build/release/duckdb'
)


def load_sql(query_name: str, queries_dir: str) -> str:
    path = os.path.join(queries_dir, f'{query_name}.sql')
    with open(path) as f:
        # Strip comments and blank lines for cleaner output
        return f.read().strip()


def run_cpu(sql: str, data_dir: str) -> tuple[int, float]:
    con = duckdb.connect()
    for table in ['eth_transactions', 'token_transfers', 'prices']:
        path = os.path.join(data_dir, f'{table}.parquet')
        if os.path.exists(path):
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM '{path}'")
    t0 = time.perf_counter()
    result = con.execute(sql).fetchall()
    elapsed_ms = (time.perf_counter() - t0) * 1000
    return len(result), elapsed_ms


def run_gpu(sql: str, data_dir: str, gpu_cache: str, gpu_proc: str) -> tuple[int | None, float | None, str]:
    # Escape single quotes in the user query for gpu_processing(...)
    escaped_sql = sql.replace("'", "''")

    abs_data_dir = os.path.abspath(data_dir)

    init_lines = [
        f"CALL gpu_buffer_init('{gpu_cache}', '{gpu_proc}');",
    ]
    for table in ['eth_transactions', 'token_transfers', 'prices']:
        path = os.path.join(abs_data_dir, f'{table}.parquet')
        if os.path.exists(path):
            init_lines.append(
                f"CREATE TABLE {table} AS SELECT * FROM '{path}';"
            )
    init_lines += [
        # Warmup: pull all tables into GPU cache
        "CALL gpu_processing('SELECT COUNT(*) FROM eth_transactions');",
        ".timer on",
        f"CALL gpu_processing('{escaped_sql}');",
    ]

    init_sql = '\n'.join(init_lines)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write(init_sql)
        tmp_path = f.name

    env = os.environ.copy()
    env['LD_LIBRARY_PATH'] = (
        f"{SIRIUS_PIXI_ENV}/lib:{env.get('LD_LIBRARY_PATH', '')}"
    )

    try:
        result = subprocess.run(
            [SIRIUS_BIN, '-unsigned', '-init', tmp_path, '-c', '.quit'],
            capture_output=True, text=True, env=env, timeout=300
        )
    finally:
        os.unlink(tmp_path)

    output = result.stdout + result.stderr

    # Parse timer output: "Run Time (s): real X.XXX ..."
    timer_match = re.search(r'Run Time.*?real\s+([\d.]+)', output)
    elapsed_ms = float(timer_match.group(1)) * 1000 if timer_match else None

    # Parse row count from DuckDB output lines like "X rows" or "(X rows)"
    rows_matches = re.findall(r'(\d+)\s+rows?', output)
    # Take the last match (most likely to be from our query, not warmup)
    row_count = int(rows_matches[-1]) if rows_matches else None

    return row_count, elapsed_ms, output


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--mode', default='both',
                   choices=['cpu-only', 'gpu-only', 'both'])
    p.add_argument('--sirius-binary', default=SIRIUS_BIN)
    p.add_argument('--data-dir', default='data')
    p.add_argument('--queries-dir', default='queries')
    p.add_argument('--gpu-cache-size', default='20 GB')
    p.add_argument('--gpu-processing-size', default='15 GB')
    p.add_argument('--verbose', action='store_true',
                   help='Print full GPU output on failure')
    args = p.parse_args()

    global SIRIUS_BIN
    SIRIUS_BIN = args.sirius_binary

    if not os.path.exists(SIRIUS_BIN) and args.mode != 'cpu-only':
        print(f"ERROR: Sirius binary not found: {SIRIUS_BIN}")
        print(f"Build with: cd /home/cc/sirius-asof && ~/.pixi/bin/pixi run -e cuda12 make release")
        print(f"Or set SIRIUS_BIN env var.")
        return

    header = f"{'Query':<28} {'CPU rows':>10} {'CPU ms':>8} {'GPU rows':>10} {'GPU ms':>8} {'Speedup':>8}  Status"
    print(header)
    print('─' * len(header))

    all_pass = True
    for name in QUERIES:
        try:
            sql = load_sql(name, args.queries_dir)
        except FileNotFoundError:
            print(f"{name:<28} MISSING — {args.queries_dir}/{name}.sql not found")
            continue

        cpu_rows = cpu_ms = gpu_rows = gpu_ms = None

        if args.mode in ('cpu-only', 'both'):
            try:
                cpu_rows, cpu_ms = run_cpu(sql, args.data_dir)
            except Exception as e:
                print(f"{name:<28} CPU ERROR: {e}")
                all_pass = False
                continue

        gpu_output = None
        if args.mode in ('gpu-only', 'both'):
            try:
                gpu_rows, gpu_ms, gpu_output = run_gpu(
                    sql, args.data_dir,
                    args.gpu_cache_size, args.gpu_processing_size
                )
            except Exception as e:
                print(f"{name:<28} GPU ERROR: {e}")
                all_pass = False
                continue

        # Determine status
        if cpu_rows is not None and gpu_rows is not None:
            if cpu_rows == gpu_rows:
                status = 'PASS'
            else:
                status = f'MISMATCH (CPU={cpu_rows}, GPU={gpu_rows})'
                all_pass = False
        elif cpu_rows is not None:
            status = f'CPU={cpu_rows}'
        elif gpu_rows is not None:
            status = f'GPU={gpu_rows}'
        else:
            status = 'N/A'

        speedup_str = ''
        if cpu_ms and gpu_ms and gpu_ms > 0:
            speedup_str = f"{cpu_ms / gpu_ms:.1f}x"

        print(
            f"{name:<28} "
            f"{(str(cpu_rows) if cpu_rows is not None else 'N/A'):>10} "
            f"{(f'{cpu_ms:.0f}' if cpu_ms else 'N/A'):>8} "
            f"{(str(gpu_rows) if gpu_rows is not None else 'N/A'):>10} "
            f"{(f'{gpu_ms:.0f}' if gpu_ms else 'N/A'):>8} "
            f"{speedup_str:>8}  {status}"
        )

        if args.verbose and gpu_output and 'MISMATCH' in status:
            print("  --- GPU output ---")
            print(gpu_output[:2000])
            print("  ---")

    print()
    if all_pass:
        print("All queries PASSED.")
    else:
        print("Some queries FAILED. Check above for details.")
        print("Tip: check build/release/log/sirius_*.log in the sirius-asof directory.")


if __name__ == '__main__':
    main()
