#!/usr/bin/env bash
# =============================================================================
# Multi-hop BFS Benchmark
# Tests 1-hop, 2-hop, 3-hop counterparty trace via gpu_processing + CPU.
#
# Usage:
#   ./benchmark/bench_multihop.sh [--sirius-dir DIR] [--source-id ADDR_ID]
#
# Strategy per hop:
#   - Frontier expansion (JOIN): run via gpu_processing
#   - Anti-join (visited dedup):  run on CPU (EXCEPT not yet in GPU)
#   - Final entity resolution:   run on CPU (UNION ALL not yet in GPU)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
SIRIUS_DIR="${SIRIUS_DIR:-$HOME/sirius}"
DATA_DIR="${DATA_DIR:-$DEMO_DIR/data}"
PIXI_ENV="${PIXI_ENV:-default}"
SOURCE_ID="${SOURCE_ID:-520262}"   # 0x28c6c06... (Binance-level, 1981 direct neighbors)
RUNS=3

while [[ $# -gt 0 ]]; do
    case $1 in
        --sirius-dir) SIRIUS_DIR="$2"; shift 2 ;;
        --source-id)  SOURCE_ID="$2";  shift 2 ;;
        --runs)       RUNS="$2";       shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

DUCKDB_BIN="$SIRIUS_DIR/build/release/duckdb"
PIXI_LIB="$SIRIUS_DIR/.pixi/envs/$PIXI_ENV/lib"
RUN_DB() { LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" "$DUCKDB_BIN" -unsigned "$@"; }

DB_PATH="$DATA_DIR/crypto_demo_2025.duckdb"
[[ -f "$DATA_DIR/crypto_demo_2025q4.duckdb" ]] && DB_PATH="$DATA_DIR/crypto_demo_2025q4.duckdb"

GPU_MEM_GB=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null | head -1 | awk '{printf "%d", $1/1024}')
GPU_CACHE_GB=$(( GPU_MEM_GB * 40 / 100 ))
GPU_PROC_GB=$(( GPU_MEM_GB * 40 / 100 ))

echo "=== Multi-hop BFS Benchmark ==="
echo "Source addr_id: $SOURCE_ID"
echo "Database: $DB_PATH"
echo "GPU buffer: ${GPU_CACHE_GB}GB cache + ${GPU_PROC_GB}GB proc"
echo ""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
ms_now() { date +%s%N | awk '{printf "%d", $1/1000000}'; }

run_timed_label() {
    local label="$1"; shift
    local t0 t1
    t0=$(ms_now)
    "$@" > /dev/null 2>&1
    t1=$(ms_now)
    echo "$label: $(( t1 - t0 ))ms"
}

# ---------------------------------------------------------------------------
# Hop expansion queries (these are the GPU-able JOIN kernels)
# ---------------------------------------------------------------------------
HOP1_SQL="SELECT DISTINCT e.dst AS vertex
FROM address_flows_daily_dict e
WHERE e.from_addr_id = ${SOURCE_ID}"

HOP2_SQL="SELECT DISTINCT e.dst AS vertex
FROM address_flows_daily_dict e
JOIN (
    SELECT DISTINCT from_addr_id AS dst FROM address_flows_daily_dict WHERE from_addr_id = ${SOURCE_ID}
) h1 ON e.from_addr_id = h1.dst
WHERE e.dst != ${SOURCE_ID}"

HOP3_SQL="SELECT DISTINCT e.dst AS vertex
FROM address_flows_daily_dict e
JOIN (
    SELECT DISTINCT e2.dst
    FROM address_flows_daily_dict e2
    JOIN (SELECT DISTINCT from_addr_id AS dst FROM address_flows_daily_dict WHERE from_addr_id = ${SOURCE_ID}) h1
      ON e2.from_addr_id = h1.dst
    WHERE e2.dst != ${SOURCE_ID}
) h2 ON e.from_addr_id = h2.dst
WHERE e.dst != ${SOURCE_ID}"

# Entity resolution for a frontier (passed as CTE)
resolve_sql() {
    local hop_num="$1"
    local frontier_sql="$2"
    echo "SELECT
    ${hop_num} AS hop,
    CASE WHEN em.entity IS NOT NULL THEN em.entity ELSE 'unknown' END AS entity,
    em.category,
    COUNT(*) AS address_count
FROM (${frontier_sql}) h
LEFT JOIN entity_address_map_dict em ON h.vertex = em.addr_id
GROUP BY 1, 2, 3
ORDER BY address_count DESC
LIMIT 20"
}

# ---------------------------------------------------------------------------
# Section 1: CPU baseline — full 3-hop resolve in one DuckDB session
# ---------------------------------------------------------------------------
echo "--- CPU baseline (DuckDB, single session, warm) ---"

CPU_FULL_SQL="
WITH hop1 AS (
    SELECT DISTINCT dst AS vertex
    FROM (SELECT from_addr_id, to_addr_id AS dst FROM address_flows_daily_dict)
    WHERE from_addr_id = ${SOURCE_ID}
),
hop2 AS (
    SELECT DISTINCT e.to_addr_id AS vertex
    FROM address_flows_daily_dict e
    JOIN hop1 ON e.from_addr_id = hop1.vertex
    WHERE e.to_addr_id NOT IN (SELECT vertex FROM hop1)
      AND e.to_addr_id != ${SOURCE_ID}
),
hop3 AS (
    SELECT DISTINCT e.to_addr_id AS vertex
    FROM address_flows_daily_dict e
    JOIN hop2 ON e.from_addr_id = hop2.vertex
    WHERE e.to_addr_id NOT IN (SELECT vertex FROM hop1)
      AND e.to_addr_id NOT IN (SELECT vertex FROM hop2)
      AND e.to_addr_id != ${SOURCE_ID}
),
all_hops AS (
    SELECT 1 AS hop, vertex FROM hop1
    UNION ALL SELECT 2, vertex FROM hop2
    UNION ALL SELECT 3, vertex FROM hop3
)
SELECT hop,
    CASE WHEN em.entity IS NOT NULL THEN em.entity ELSE 'unknown' END AS entity,
    em.category,
    COUNT(*) AS address_count
FROM all_hops h
LEFT JOIN entity_address_map_dict em ON h.vertex = em.addr_id
GROUP BY 1, 2, 3
ORDER BY hop, address_count DESC
LIMIT 30"

# Warmup
echo "$CPU_FULL_SQL" | RUN_DB "$DB_PATH" > /dev/null 2>&1

for r in $(seq 1 $RUNS); do
    t0=$(ms_now)
    echo "$CPU_FULL_SQL" | RUN_DB "$DB_PATH" > /dev/null 2>&1
    t1=$(ms_now)
    echo "  cpu | 3-hop full | run $r | $(( t1 - t0 ))ms"
done
echo ""

# ---------------------------------------------------------------------------
# Section 2: GPU hop expansion — each hop individually via gpu_processing
# Move sirius.cfg aside so gpu_buffer_init can allocate freely
# ---------------------------------------------------------------------------
echo "--- gpu_processing: per-hop expansion ---"

if [[ -f "$HOME/.sirius/sirius.cfg" ]]; then
    mv "$HOME/.sirius/sirius.cfg" "$HOME/.sirius/sirius.cfg.bak"
    trap 'mv "$HOME/.sirius/sirius.cfg.bak" "$HOME/.sirius/sirius.cfg" 2>/dev/null || true' EXIT
fi

GPU_INIT="CALL gpu_buffer_init('${GPU_CACHE_GB} GB', '${GPU_PROC_GB} GB');"

run_gpu_hop() {
    local label="$1"
    local sql="$2"
    local escaped
    escaped=$(echo "$sql" | sed "s/'/''/g")
    local session_sql="$GPU_INIT
.timer on
CALL gpu_processing('$escaped');"

    # Warmup
    echo "$session_sql" | RUN_DB "$DB_PATH" > /dev/null 2>&1

    for r in $(seq 1 $RUNS); do
        local output
        output=$(echo "$session_sql" | RUN_DB "$DB_PATH" 2>&1)
        local real_time ms
        real_time=$(echo "$output" | grep -oP 'real \K[0-9.]+' | head -1)
        ms=$(echo "$real_time" | awk '{printf "%.0f", $1*1000}')
        echo "  gpu_processing | $label | run $r | ${ms}ms"
    done
}

run_gpu_hop "hop1_expand" "$HOP1_SQL"
run_gpu_hop "hop2_expand" "$HOP2_SQL"
run_gpu_hop "hop3_expand" "$HOP3_SQL"
echo ""

# ---------------------------------------------------------------------------
# Section 3: GPU hop sizes (how many vertices per frontier?)
# ---------------------------------------------------------------------------
echo "--- Frontier sizes ---"
COUNT_SQL() { echo "SELECT COUNT(*) FROM (${1}) t"; }

for hop_label in "hop1:$HOP1_SQL" "hop2:$HOP2_SQL" "hop3:$HOP3_SQL"; do
    label="${hop_label%%:*}"
    sql="${hop_label#*:}"
    count=$(echo "$sql" | awk -v src="$SOURCE_ID" '{gsub(/\$\{SOURCE_ID\}/,src); print}' | \
        RUN_DB "$DB_PATH" -c "$(COUNT_SQL "$sql")" 2>/dev/null | grep -oP '^\s*\K[0-9]+' | tail -1 || echo "?")
    echo "  $label vertices: $count"
done
echo ""

echo "=== Done ==="
