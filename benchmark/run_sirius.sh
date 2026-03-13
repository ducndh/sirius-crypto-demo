#!/bin/bash
# Run TRM pipeline queries on Sirius GPU
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
DB_PATH="${REPO_DIR}/data/crypto_demo.duckdb"
QUERY_DIR="${REPO_DIR}/queries/trm_pipeline"
RESULTS_DIR="${SCRIPT_DIR}/results"

SIRIUS_BIN="${SIRIUS_BIN:-$HOME/sirius-dev/build/release/duckdb}"
SIRIUS_PIXI_ENV="${SIRIUS_PIXI_ENV:-$HOME/sirius-dev/.pixi/envs/cuda12}"
GPU_CACHE_SIZE="${GPU_CACHE_SIZE:-20 GB}"
GPU_PROC_SIZE="${GPU_PROC_SIZE:-15 GB}"

mkdir -p "$RESULTS_DIR"

if [ ! -f "$DB_PATH" ]; then
    echo "ERROR: Database not found at $DB_PATH"
    echo "Run: python scripts/prepare_tables.py"
    exit 1
fi

if [ ! -f "$SIRIUS_BIN" ]; then
    echo "ERROR: Sirius binary not found at $SIRIUS_BIN"
    exit 1
fi

export LD_LIBRARY_PATH="${SIRIUS_PIXI_ENV}/lib:${LD_LIBRARY_PATH:-}"

echo "=== Sirius GPU Benchmark ==="
echo "Binary: $SIRIUS_BIN"
echo "Database: $DB_PATH"
echo "GPU Cache: $GPU_CACHE_SIZE | GPU Proc: $GPU_PROC_SIZE"
echo ""

for qfile in "$QUERY_DIR"/q*.sql; do
    qname=$(basename "$qfile" .sql)
    query=$(cat "$qfile")
    echo -n "  $qname ... "

    # Build the full command: init GPU buffer, then run query with gpu_processing
    gpu_query="CALL gpu_buffer_init('${GPU_CACHE_SIZE}', '${GPU_PROC_SIZE}');
CALL gpu_processing('$(echo "$query" | tr "'" "''" | tr '\n' ' ')');"

    # Run 3 times, take median
    times=()
    for i in 1 2 3; do
        t=$( { echo "$gpu_query"; } | "$SIRIUS_BIN" "$DB_PATH" -noheader -csv \
            ".timer on" 2>&1 | grep "Run Time" | tail -1 | awk '{print $4}')
        times+=("$t")
    done

    sorted=($(printf '%s\n' "${times[@]}" | sort -n))
    median=${sorted[1]}
    echo "${median}s (runs: ${times[*]})"
done

echo ""
echo "Done."
