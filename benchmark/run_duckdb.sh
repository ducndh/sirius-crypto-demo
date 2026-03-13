#!/bin/bash
# Run TRM pipeline queries on CPU DuckDB as baseline
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
DB_PATH="${REPO_DIR}/data/crypto_demo.duckdb"
QUERY_DIR="${REPO_DIR}/queries/trm_pipeline"
RESULTS_DIR="${SCRIPT_DIR}/results"

mkdir -p "$RESULTS_DIR"

if [ ! -f "$DB_PATH" ]; then
    echo "ERROR: Database not found at $DB_PATH"
    echo "Run: python scripts/prepare_tables.py"
    exit 1
fi

echo "=== DuckDB CPU Baseline ==="
echo "Database: $DB_PATH"
echo ""

for qfile in "$QUERY_DIR"/q*.sql; do
    qname=$(basename "$qfile" .sql)
    echo -n "  $qname ... "

    # Run query 3 times, take median
    times=()
    for i in 1 2 3; do
        t=$(duckdb "$DB_PATH" -noheader -csv \
            ".timer on" < "$qfile" 2>&1 | grep "Run Time" | awk '{print $4}')
        times+=("$t")
    done

    # Sort and take median
    sorted=($(printf '%s\n' "${times[@]}" | sort -n))
    median=${sorted[1]}
    echo "${median}s (runs: ${times[*]})"
done

echo ""
echo "Done."
