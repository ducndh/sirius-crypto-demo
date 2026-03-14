#!/usr/bin/env bash
# =============================================================================
# Sirius gpu_execution OOM Test
# Tests gpu_execution on increasingly large datasets to find OOM boundary.
# Uses raw eth_transfers parquets (no DuckDB needed).
#
# Usage:
#   ./benchmark/run_oom_test.sh [--months 3|6|12] [--sirius-dir DIR]
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
SIRIUS_DIR="${SIRIUS_DIR:-$HOME/sirius-dev}"
DATA_DIR="${DATA_DIR:-$DEMO_DIR/data}"
PIXI_ENV="${PIXI_ENV:-cuda12}"
MONTHS="${1:-all}"  # 3, 6, 12, or "all" to run all sizes
RESULTS_DIR="$SCRIPT_DIR/results"

DUCKDB_BIN="$SIRIUS_DIR/build/release/duckdb"
PIXI_LIB="$SIRIUS_DIR/.pixi/envs/$PIXI_ENV/lib"
ETH_DIR="$DATA_DIR/eth_transfers"

mkdir -p "$RESULTS_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_CSV="$RESULTS_DIR/oom_test_${TIMESTAMP}.csv"
echo "months,query,status,time_ms,rows,error" > "$RESULTS_CSV"

# GPU info
GPU_NAME=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1 | xargs)
GPU_MEM_MB=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null | head -1 | xargs)
echo "=== Sirius gpu_execution OOM Test ==="
echo "GPU: $GPU_NAME (${GPU_MEM_MB}MB)"
echo "Binary: $DUCKDB_BIN"
echo "Results: $RESULTS_CSV"
echo ""

# Generate Sirius config
CONFIG_PATH="$SCRIPT_DIR/sirius_rtx6000.cfg"
if [[ ! -f "$CONFIG_PATH" ]]; then
    echo "ERROR: Config file not found at $CONFIG_PATH"
    exit 1
fi

export SIRIUS_CONFIG_FILE="$CONFIG_PATH"
export SIRIUS_LOG_LEVEL=info

# Build parquet glob patterns for different time ranges
build_parquet_glob() {
    local months=$1
    case $months in
        3)  echo "'$ETH_DIR/date=2025-10-*/*.parquet','$ETH_DIR/date=2025-11-*/*.parquet','$ETH_DIR/date=2025-12-*/*.parquet'" ;;
        6)  echo "'$ETH_DIR/date=2025-07-*/*.parquet','$ETH_DIR/date=2025-08-*/*.parquet','$ETH_DIR/date=2025-09-*/*.parquet','$ETH_DIR/date=2025-10-*/*.parquet','$ETH_DIR/date=2025-11-*/*.parquet','$ETH_DIR/date=2025-12-*/*.parquet'" ;;
    esac
}

# Test queries (simplified — operate on raw token_transfers via parquet)
# Q1: Simple aggregation (GROUP BY + SUM + COUNT)
build_q1() {
    echo "SELECT date, token_address AS asset, SUM(value) AS total_value, COUNT(*) AS tx_count FROM transfers GROUP BY 1, 2 ORDER BY total_value DESC LIMIT 100"
}

# Q2: Address-level aggregation (heavier GROUP BY)
build_q2() {
    echo "SELECT ''0x'' || SUBSTR(from_address, 27) AS from_addr, ''0x'' || SUBSTR(to_address, 27) AS to_addr, SUM(value) AS amount, COUNT(*) AS tx_count FROM transfers GROUP BY 1, 2 ORDER BY amount DESC LIMIT 100"
}

# Q3: COUNT DISTINCT (memory intensive)
build_q3() {
    echo "SELECT date, COUNT(DISTINCT from_address) AS unique_senders, COUNT(DISTINCT to_address) AS unique_receivers FROM transfers GROUP BY 1 ORDER BY 1"
}

run_oom_query() {
    local months=$1
    local query_name=$2
    local query_sql=$3
    local parquet_glob=$4

    local sql_cmds=""
    sql_cmds+="CREATE VIEW transfers AS SELECT * FROM read_parquet([$parquet_glob], hive_partitioning=true);"$'\n'
    sql_cmds+=".timer on"$'\n'
    sql_cmds+="CALL gpu_execution('$query_sql');"$'\n'

    local start end elapsed_ms
    start=$(date +%s%N)

    local output exit_code
    output=$(echo "$sql_cmds" | LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" \
        "$DUCKDB_BIN" 2>&1) || true
    exit_code=$?
    end=$(date +%s%N)
    elapsed_ms=$(( (end - start) / 1000000 ))

    # Check for OOM or other errors
    if echo "$output" | grep -qi "out of memory\|OOM\|bad_alloc\|allocation failed\|CUDA error"; then
        local err_msg
        err_msg=$(echo "$output" | grep -i "out of memory\|OOM\|bad_alloc\|allocation failed\|CUDA error" | head -1 | tr ',' ';')
        echo "  ${months}m | $query_name | OOM/ERROR (${elapsed_ms}ms)"
        echo "$months,$query_name,OOM,$elapsed_ms,0,$err_msg" >> "$RESULTS_CSV"
    elif echo "$output" | grep -qi "error\|exception\|fatal"; then
        local err_msg
        err_msg=$(echo "$output" | grep -i "error\|exception\|fatal" | head -1 | tr ',' ';')
        echo "  ${months}m | $query_name | ERROR (${elapsed_ms}ms): $err_msg"
        echo "$months,$query_name,ERROR,$elapsed_ms,0,$err_msg" >> "$RESULTS_CSV"
    else
        # Extract timing from .timer output
        local real_time
        real_time=$(echo "$output" | grep "Run Time" | grep -oP 'real \K[0-9.]+' | tail -1)
        local timer_ms
        timer_ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}' 2>/dev/null || echo "$elapsed_ms")
        echo "  ${months}m | $query_name | OK ${timer_ms}ms (wall ${elapsed_ms}ms)"
        echo "$months,$query_name,OK,$timer_ms,0," >> "$RESULTS_CSV"
    fi
}

# Run tests
run_test_set() {
    local months=$1
    local parquet_glob
    parquet_glob=$(build_parquet_glob "$months")

    # Count rows first (unset config to avoid Sirius init for simple row count)
    local row_count
    row_count=$(echo "SELECT COUNT(*) FROM read_parquet([$parquet_glob], hive_partitioning=true);" | \
        SIRIUS_CONFIG_FILE="" LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" "$DUCKDB_BIN" -noheader 2>/dev/null | tr -d '[:space:]')

    echo ""
    echo "--- ${months} months (${row_count} rows) ---"

    run_oom_query "$months" "Q1_agg" "$(build_q1)" "$parquet_glob"
    run_oom_query "$months" "Q2_addr_agg" "$(build_q2)" "$parquet_glob"
    run_oom_query "$months" "Q3_count_distinct" "$(build_q3)" "$parquet_glob"
}

# Run requested test sizes
case "$MONTHS" in
    3)  run_test_set 3 ;;
    6)  run_test_set 6 ;;
    all)
        run_test_set 3
        run_test_set 6
        ;;
    *)  echo "Usage: $0 [3|6|all]"; exit 1 ;;
esac

echo ""
echo "=== Results ==="
cat "$RESULTS_CSV"
echo ""
echo "OOM test complete: $(date)"
