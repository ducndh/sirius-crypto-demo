#!/usr/bin/env python3
"""TRM Sankey Benchmark — portable across machines.

Runs the 3-hop entity Sankey query on both GPU (gpu_processing) and CPU,
reports warm-cache timings. Designed for the crypto TRM demo workload:
  - Load data into GPU memory once
  - Run multiple query types (simulating analyst session)
  - Report per-query and aggregate timings

Requirements:
  - Sirius DuckDB binary built with GPU support
  - ~/.sirius/sirius.cfg must NOT exist (gpu_processing path)
  - Data DB with: address_flows_daily_dict, entity_address_map_int, entity_lookup

Usage:
  python3 bench_trm.py [--db /path/to/crypto.duckdb] [--sirius /path/to/sirius]
  python3 bench_trm.py --help
"""
import argparse
import os
import re
import subprocess
import sys
import time

# ---------------------------------------------------------------------------
# Defaults — override with CLI flags
# ---------------------------------------------------------------------------
DEFAULT_DB = os.path.expanduser("~/sirius-crypto-demo/data/crypto_demo_2025_slim.duckdb")
DEFAULT_SIRIUS = os.path.expanduser("~/sirius")

# ---------------------------------------------------------------------------
# SQL: views + queries
# ---------------------------------------------------------------------------
SETUP_VIEWS = """
CREATE OR REPLACE VIEW v_flows AS SELECT * FROM address_flows_daily_dict;
CREATE OR REPLACE VIEW v_emap AS SELECT entity_id, addr_id FROM entity_address_map_int;
"""

SANKEY_3HOP = """
WITH seed_addrs AS (
    SELECT addr_id FROM v_emap WHERE entity_id = 143
),
hop1_edges AS (
    SELECT 143 AS from_eid, dst.entity_id AS to_eid, SUM(f.amount) AS total_amount
    FROM v_flows f
    JOIN seed_addrs s ON f.from_addr_id = s.addr_id
    JOIN v_emap dst ON f.to_addr_id = dst.addr_id
    WHERE dst.entity_id != 143
    GROUP BY 1, 2
),
hop1_frontier AS (SELECT to_eid AS eid, COUNT(*) AS _c FROM hop1_edges GROUP BY 1),
hop2_edges AS (
    SELECT src.entity_id AS from_eid, dst.entity_id AS to_eid, SUM(f.amount) AS total_amount
    FROM v_flows f
    JOIN v_emap src ON f.from_addr_id = src.addr_id
    JOIN hop1_frontier h1 ON src.entity_id = h1.eid
    JOIN v_emap dst ON f.to_addr_id = dst.addr_id
    WHERE dst.entity_id != 143
      AND dst.entity_id NOT IN (SELECT eid FROM hop1_frontier)
    GROUP BY 1, 2
),
hop2_frontier AS (SELECT to_eid AS eid, COUNT(*) AS _c FROM hop2_edges GROUP BY 1),
hop3_edges AS (
    SELECT src.entity_id AS from_eid, dst.entity_id AS to_eid, SUM(f.amount) AS total_amount
    FROM v_flows f
    JOIN v_emap src ON f.from_addr_id = src.addr_id
    JOIN hop2_frontier h2 ON src.entity_id = h2.eid
    JOIN v_emap dst ON f.to_addr_id = dst.addr_id
    WHERE dst.entity_id != 143
      AND dst.entity_id NOT IN (SELECT eid FROM hop1_frontier)
      AND dst.entity_id NOT IN (SELECT eid FROM hop2_frontier)
    GROUP BY 1, 2
)
SELECT 1 AS hop, from_eid, to_eid, total_amount FROM hop1_edges
UNION ALL
SELECT 2 AS hop, from_eid, to_eid, total_amount FROM hop2_edges
UNION ALL
SELECT 3 AS hop, from_eid, to_eid, total_amount FROM hop3_edges;
"""

# Single-hop entity rollup (different seed entities for mixed workload)
ENTITY_ROLLUP_TEMPLATE = """
WITH seed AS (SELECT addr_id FROM v_emap WHERE entity_id = {eid})
SELECT {eid} AS from_eid, dst.entity_id AS to_eid, SUM(f.amount) AS total_amount
FROM v_flows f
JOIN seed s ON f.from_addr_id = s.addr_id
JOIN v_emap dst ON f.to_addr_id = dst.addr_id
WHERE dst.entity_id != {eid}
GROUP BY 1, 2;
"""

# Top inflow/outflow aggregation (common TRM dashboard query)
TOP_FLOWS = """
SELECT to_addr_id, SUM(amount) AS total_in, SUM(tx_count) AS total_tx
FROM v_flows
GROUP BY 1;
"""


def find_binary(sirius_dir):
    binary = os.path.join(sirius_dir, "build/release/duckdb")
    if not os.path.isfile(binary):
        print(f"ERROR: Sirius binary not found at {binary}")
        print(f"  Note: {sirius_dir}/duckdb/ is the source submodule, not the binary.")
        print(f"  Build with: cd {sirius_dir} && pixi run make release")
        sys.exit(1)
    return binary


def find_lib_path(sirius_dir):
    lib = os.path.join(sirius_dir, ".pixi/envs/default/lib")
    if not os.path.isdir(lib):
        print(f"ERROR: pixi lib dir not found at {lib}")
        sys.exit(1)
    return lib


def run_sql(binary, lib_path, db_path, sql, timeout=300):
    """Run SQL via DuckDB CLI, return (stdout, stderr, returncode, elapsed_s)."""
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = f"{lib_path}:{env.get('LD_LIBRARY_PATH', '')}"
    t0 = time.time()
    result = subprocess.run(
        [binary, db_path, "-unsigned", "-c", sql],
        capture_output=True, text=True, env=env, timeout=timeout
    )
    elapsed = time.time() - t0
    return result.stdout.strip(), result.stderr.strip(), result.returncode, elapsed


def parse_timing(output):
    """Extract 'Run Time (s): real X.XXX' from DuckDB .timer output."""
    m = re.search(r"Run Time \(s\): real (\d+\.\d+)", output)
    return float(m.group(1)) if m else None


def run_benchmark(binary, lib_path, db_path, label, sql, n_warmup=2, n_runs=5, gpu=False):
    """Run a query multiple times, return median timing in ms."""
    if gpu:
        full_sql = f"""
{SETUP_VIEWS}
CALL gpu_buffer_init('10 GB', '10 GB');
-- warmup
"""
        for _ in range(n_warmup):
            full_sql += f"CALL gpu_processing('{sql.replace(chr(39), chr(39)+chr(39))}');\n"
        full_sql += ".timer on\n"
        for _ in range(n_runs):
            full_sql += f"CALL gpu_processing('{sql.replace(chr(39), chr(39)+chr(39))}');\n"
    else:
        full_sql = f"{SETUP_VIEWS}\n"
        for _ in range(n_warmup):
            full_sql += sql + "\n"
        full_sql += ".timer on\n"
        for _ in range(n_runs):
            full_sql += sql + "\n"

    stdout, stderr, rc, wall = run_sql(binary, lib_path, db_path, full_sql, timeout=600)

    if rc != 0:
        print(f"  [{label}] FAILED (rc={rc})")
        if stderr:
            print(f"  stderr: {stderr[:500]}")
        return None

    # Parse all timings
    timings = [float(m) * 1000 for m in re.findall(r"Run Time \(s\): real (\d+\.\d+)", stdout)]
    if not timings:
        print(f"  [{label}] No timing output found")
        return None

    timings.sort()
    median = timings[len(timings) // 2]
    return median


def main():
    parser = argparse.ArgumentParser(description="TRM Sankey Benchmark")
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to DuckDB database")
    parser.add_argument("--sirius", default=DEFAULT_SIRIUS, help="Path to Sirius repo")
    parser.add_argument("--runs", type=int, default=5, help="Number of timed runs per query")
    parser.add_argument("--cpu-only", action="store_true", help="Skip GPU benchmarks")
    parser.add_argument("--gpu-only", action="store_true", help="Skip CPU benchmarks")
    args = parser.parse_args()

    binary = find_binary(args.sirius)
    lib_path = find_lib_path(args.sirius)

    if not os.path.isfile(args.db):
        print(f"ERROR: Database not found at {args.db}")
        sys.exit(1)

    print(f"Database:  {args.db}")
    print(f"Binary:    {binary}")
    print(f"Runs:      {args.runs} (median reported)")
    print()

    # Check row count
    stdout, _, _, _ = run_sql(binary, lib_path, args.db,
                              "SELECT COUNT(*) FROM address_flows_daily_dict;")
    print(f"Flows rows: {stdout}")
    print()

    # Check if sirius.cfg exists (affects gpu_processing)
    cfg = os.path.expanduser("~/.sirius/sirius.cfg")
    if os.path.exists(cfg) and not args.cpu_only:
        print(f"WARNING: {cfg} exists — gpu_processing may not get full GPU memory.")
        print("  Move it aside for accurate gpu_processing benchmarks.")
        print()

    queries = {
        "3-hop Sankey (fixedfloat)": SANKEY_3HOP,
        "1-hop rollup (entity 143)": ENTITY_ROLLUP_TEMPLATE.format(eid=143),
        "1-hop rollup (entity 1)": ENTITY_ROLLUP_TEMPLATE.format(eid=1),
        "1-hop rollup (entity 50)": ENTITY_ROLLUP_TEMPLATE.format(eid=50),
        "Top inflows aggregation": TOP_FLOWS,
    }

    results = {}
    for label, sql in queries.items():
        print(f"--- {label} ---")
        row = {}

        if not args.gpu_only:
            cpu_ms = run_benchmark(binary, lib_path, args.db, f"{label}/CPU",
                                   sql, n_runs=args.runs, gpu=False)
            if cpu_ms is not None:
                print(f"  CPU: {cpu_ms:.1f} ms")
            row["cpu_ms"] = cpu_ms

        if not args.cpu_only:
            gpu_ms = run_benchmark(binary, lib_path, args.db, f"{label}/GPU",
                                   sql, n_runs=args.runs, gpu=True)
            if gpu_ms is not None:
                print(f"  GPU: {gpu_ms:.1f} ms")
            row["gpu_ms"] = gpu_ms

        if row.get("cpu_ms") and row.get("gpu_ms"):
            speedup = row["cpu_ms"] / row["gpu_ms"]
            print(f"  Speedup: {speedup:.1f}x")

        results[label] = row
        print()

    # Summary table
    print("=" * 70)
    print(f"{'Query':<35} {'GPU (ms)':>10} {'CPU (ms)':>10} {'Speedup':>10}")
    print("-" * 70)
    for label, row in results.items():
        gpu = f"{row.get('gpu_ms', 0):.1f}" if row.get("gpu_ms") else "n/a"
        cpu = f"{row.get('cpu_ms', 0):.1f}" if row.get("cpu_ms") else "n/a"
        if row.get("cpu_ms") and row.get("gpu_ms"):
            spd = f"{row['cpu_ms'] / row['gpu_ms']:.1f}x"
        else:
            spd = "n/a"
        print(f"{label:<35} {gpu:>10} {cpu:>10} {spd:>10}")
    print("=" * 70)


if __name__ == "__main__":
    main()
