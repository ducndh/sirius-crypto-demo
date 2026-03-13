#!/bin/bash
# Run TRM pipeline queries on StarRocks
# Requires: docker compose up -d in starrocks/
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
QUERY_DIR="${REPO_DIR}/queries/trm_pipeline"

SR_HOST="${SR_HOST:-127.0.0.1}"
SR_PORT="${SR_PORT:-9030}"

echo "=== StarRocks Benchmark ==="
echo "Host: $SR_HOST:$SR_PORT"
echo ""

# Check connection
if ! mysql -h "$SR_HOST" -P "$SR_PORT" -u root -e "SELECT 1" &>/dev/null; then
    echo "ERROR: Cannot connect to StarRocks at $SR_HOST:$SR_PORT"
    echo "Start with: cd starrocks && docker compose up -d"
    echo "Then load data with: bash benchmark/setup_starrocks.sh"
    exit 1
fi

for qfile in "$QUERY_DIR"/q*.sql; do
    qname=$(basename "$qfile" .sql)
    echo -n "  $qname ... "

    times=()
    for i in 1 2 3; do
        t=$(mysql -h "$SR_HOST" -P "$SR_PORT" -u root -D crypto_demo \
            --skip-column-names -e "SET enable_profile=true; SOURCE $qfile;" 2>&1 \
            | grep -i "query time" | awk '{print $NF}')
        times+=("$t")
    done

    sorted=($(printf '%s\n' "${times[@]}" | sort -n))
    median=${sorted[1]}
    echo "${median}s (runs: ${times[*]})"
done

echo ""
echo "Done."
