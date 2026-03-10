#!/usr/bin/env python3
"""Full benchmark: CPU vs GPU, multiple runs, JSON output + printed table.

Usage:
  python scripts/run_demo_benchmark.py
  python scripts/run_demo_benchmark.py --warmup-runs 1 --benchmark-runs 3
  GPU_CACHE_SIZE='70 GB' GPU_PROC_SIZE='60 GB' python scripts/run_demo_benchmark.py  # H100
"""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import duckdb

QUERIES = [
    ('q01_count_range',     'Count transactions in date range'),
    ('q02_address_activity','Address activity summary'),
    ('q03_top_addresses',   'Top 100 addresses by tx count'),
    ('q04_price_match',     'ASOF JOIN: match all txns to ETH price'),
    ('q05_daily_volume',    'ASOF JOIN: daily USD volume'),
    ('q06_top_senders_usd', 'ASOF JOIN: top senders by USD volume'),
    ('q07_token_volumes',   'Top token contracts by activity'),
    ('q08_hourly_gas',      'Hourly gas price trends'),
    ('q09_address_pairs',   'Most active address pairs'),
    ('q10_block_stats',     'Block-level statistics'),
]

SIRIUS_PIXI_ENV = os.environ.get(
    'SIRIUS_PIXI_ENV', '/home/cc/sirius-asof/.pixi/envs/cuda12'
)
SIRIUS_BIN = os.environ.get(
    'SIRIUS_BIN', '/home/cc/sirius-asof/build/release/duckdb'
)


def load_sql(name: str, queries_dir: str) -> str:
    with open(os.path.join(queries_dir, f'{name}.sql')) as f:
        return f.read().strip()


# ── CPU baseline ──────────────────────────────────────────────────────

_cpu_con = None

def get_cpu_con(data_dir: str) -> duckdb.DuckDBPyConnection:
    global _cpu_con
    if _cpu_con is None:
        con = duckdb.connect()
        con.execute(f"SET threads = {os.cpu_count()}")
        for table in ['eth_transactions', 'token_transfers', 'prices']:
            path = os.path.join(data_dir, f'{table}.parquet')
            if os.path.exists(path):
                con.execute(f"CREATE TABLE {table} AS SELECT * FROM '{path}'")
        _cpu_con = con  # only cache after successful setup
    return _cpu_con


def run_cpu_once(sql: str, data_dir: str) -> float:
    con = get_cpu_con(data_dir)
    t0 = time.perf_counter()
    con.execute(sql).fetchall()
    return (time.perf_counter() - t0) * 1000


# ── GPU (Sirius) ──────────────────────────────────────────────────────

_gpu_init_done = False
_gpu_tmp = None


def gpu_init_script(data_dir: str, gpu_cache: str, gpu_proc: str) -> str:
    abs_dir = os.path.abspath(data_dir)
    lines = [f"CALL gpu_buffer_init('{gpu_cache}', '{gpu_proc}');"]
    for table in ['eth_transactions', 'token_transfers', 'prices']:
        path = os.path.join(abs_dir, f'{table}.parquet')
        if os.path.exists(path):
            lines.append(f"CREATE TABLE {table} AS SELECT * FROM '{path}';")
    # Warmup: load all tables into GPU VRAM
    lines.append("CALL gpu_processing('SELECT COUNT(*) FROM eth_transactions');")
    return '\n'.join(lines)


def run_gpu_query(sql: str, data_dir: str, gpu_cache: str, gpu_proc: str,
                  n_runs: int, sirius_bin: str = SIRIUS_BIN) -> tuple[list[float], int | None]:
    """Run a single query N times on GPU, return list of elapsed_ms and row count."""
    escaped_sql = sql.replace("'", "''")
    abs_dir = os.path.abspath(data_dir)

    # Build a single script: init + warmup + N timed runs
    lines = [f"CALL gpu_buffer_init('{gpu_cache}', '{gpu_proc}');"]
    for table in ['eth_transactions', 'token_transfers', 'prices']:
        path = os.path.join(abs_dir, f'{table}.parquet')
        if os.path.exists(path):
            lines.append(f"CREATE TABLE {table} AS SELECT * FROM '{path}';")
    # One warmup run (not timed)
    lines.append(f"CALL gpu_processing('{escaped_sql}');")
    # N timed runs
    lines.append(".timer on")
    for _ in range(n_runs):
        lines.append(f"CALL gpu_processing('{escaped_sql}');")

    script = '\n'.join(lines)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write(script)
        tmp_path = f.name

    env = os.environ.copy()
    env['LD_LIBRARY_PATH'] = (
        f"{SIRIUS_PIXI_ENV}/lib:{env.get('LD_LIBRARY_PATH', '')}"
    )

    try:
        result = subprocess.run(
            [sirius_bin, '-unsigned', '-init', tmp_path, '-c', '.quit'],
            capture_output=True, text=True, env=env, timeout=600
        )
    finally:
        os.unlink(tmp_path)

    output = result.stdout + result.stderr

    # Parse all timer lines: "Run Time (s): real X.XXX"
    timings = [
        float(m) * 1000
        for m in re.findall(r'Run Time.*?real\s+([\d.]+)', output)
    ]

    # Row count from last "X rows" match
    rows_matches = re.findall(r'(\d+)\s+rows?', output)
    row_count = int(rows_matches[-1]) if rows_matches else None

    return timings[-n_runs:] if len(timings) >= n_runs else timings, row_count, output


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--data-dir', default='data')
    p.add_argument('--queries-dir', default='queries')
    p.add_argument('--sirius-binary', default=SIRIUS_BIN)
    p.add_argument('--gpu-cache-size',
                   default=os.environ.get('GPU_CACHE_SIZE', '20 GB'))
    p.add_argument('--gpu-processing-size',
                   default=os.environ.get('GPU_PROC_SIZE', '15 GB'))
    p.add_argument('--warmup-runs', type=int, default=1)
    p.add_argument('--benchmark-runs', type=int, default=3)
    p.add_argument('--output', default='results/benchmark_results.json')
    p.add_argument('--cpu-only', action='store_true')
    args = p.parse_args()

    if not os.path.exists(args.sirius_binary) and not args.cpu_only:
        print(f"ERROR: Sirius binary not found: {args.sirius_binary}")
        print(f"Set SIRIUS_BIN env var or build Sirius first.")
        sys.exit(1)

    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)

    print(f"Sirius Crypto Demo Benchmark")
    print(f"  Data:      {args.data_dir}")
    print(f"  Queries:   {args.queries_dir}")
    print(f"  GPU:       {args.sirius_binary}")
    print(f"  GPU cache: {args.gpu_cache_size} / {args.gpu_processing_size}")
    print(f"  Runs:      {args.warmup_runs} warmup + {args.benchmark_runs} timed")
    print()

    results = []

    # Header
    col_w = 30
    print(f"{'Query':<{col_w}} {'CPU best':>10} {'GPU best':>10} {'Speedup':>9}  Status")
    print('─' * (col_w + 35))

    for name, description in QUERIES:
        try:
            sql = load_sql(name, args.queries_dir)
        except FileNotFoundError:
            print(f"{name:<{col_w}} MISSING")
            continue

        # CPU runs
        cpu_times = []
        try:
            for _ in range(args.warmup_runs):
                run_cpu_once(sql, args.data_dir)
            for _ in range(args.benchmark_runs):
                cpu_times.append(run_cpu_once(sql, args.data_dir))
        except Exception as e:
            print(f"{name:<{col_w}} CPU ERROR: {e}")
            continue
        cpu_best = min(cpu_times)

        if args.cpu_only:
            print(f"{name:<{col_w}} {cpu_best:>10.0f}ms {'N/A':>10} {'N/A':>9}  CPU-only")
            results.append({'query': name, 'cpu_ms': cpu_best})
            continue

        # GPU runs
        try:
            gpu_times, gpu_rows, gpu_output = run_gpu_query(
                sql, args.data_dir, args.gpu_cache_size,
                args.gpu_processing_size,
                args.warmup_runs + args.benchmark_runs,
                sirius_bin=args.sirius_binary,
            )
            # Last N are the timed runs
            gpu_timed = gpu_times[-args.benchmark_runs:] if len(gpu_times) >= args.benchmark_runs else gpu_times
            gpu_best = min(gpu_timed) if gpu_timed else None
        except Exception as e:
            print(f"{name:<{col_w}} GPU ERROR: {e}")
            continue

        speedup = cpu_best / gpu_best if gpu_best and gpu_best > 0 else None
        speedup_str = f"{speedup:.1f}x" if speedup else "N/A"

        # Quick sanity: GPU should be faster than CPU (or close)
        if speedup and speedup < 0.5:
            status = "WARN: GPU slower (fallback?)"
        elif gpu_rows == 0 and speedup and speedup > 100:
            status = "WARN: 0 rows returned"
        else:
            status = "OK"

        print(
            f"{name:<{col_w}} "
            f"{cpu_best:>10.0f}ms "
            f"{(f'{gpu_best:.0f}ms' if gpu_best else 'N/A'):>10} "
            f"{speedup_str:>9}  {status}"
        )

        results.append({
            'query': name,
            'description': description,
            'cpu_times_ms': cpu_times,
            'cpu_best_ms': cpu_best,
            'gpu_times_ms': gpu_timed,
            'gpu_best_ms': gpu_best,
            'speedup': speedup,
            'gpu_rows': gpu_rows,
            'status': status,
        })

    # Write JSON results
    with open(args.output, 'w') as f:
        json.dump({
            'config': {
                'sirius_binary': args.sirius_binary,
                'gpu_cache': args.gpu_cache_size,
                'gpu_proc': args.gpu_processing_size,
                'benchmark_runs': args.benchmark_runs,
            },
            'results': results,
        }, f, indent=2)

    print()
    print(f"Results saved to {args.output}")

    if not args.cpu_only and results:
        speedups = [r['speedup'] for r in results if r.get('speedup')]
        if speedups:
            print(f"Speedup range: {min(speedups):.1f}x – {max(speedups):.1f}x")
            print(f"Geometric mean speedup: {(1.0 if not speedups else (1.0)) ** (1/len(speedups)):.1f}x")


if __name__ == '__main__':
    main()
