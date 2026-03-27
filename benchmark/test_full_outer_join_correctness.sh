#!/usr/bin/env bash
# =============================================================================
# Correctness test: FULL OUTER JOIN in gpu_processing (patch e80c019)
#
# Tests different FULL OUTER JOIN shapes to verify the cudf::full_join route
# produces the same results as DuckDB CPU across:
#   1. Small synthetic — checks left-only, right-only, matched rows all present
#   2. Q05 real data — entity inflow/outflow balance (the motivating query)
#   3. Q05 left-only rows — send-only entities (absent if silently INNER JOIN)
#   4. Q05 right-only rows — receive-only entities (same)
#   5. Int-key FULL OUTER JOIN — different column type from Q05's varchar keys
# =============================================================================
set -euo pipefail

SIRIUS="${SIRIUS:-/home/dnguyen56/sirius}"
DB="${DB:-/home/dnguyen56/sirius-crypto-demo/data/crypto_demo_2025.duckdb}"
PIXI_LIB="$SIRIUS/.pixi/envs/default/lib"
GPU_MEM_GB=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null | head -1 | awk '{printf "%d", $1/1024}')
CACHE_GB=$(( GPU_MEM_GB * 40 / 100 ))
PROC_GB=$(( GPU_MEM_GB * 40 / 100 ))

echo "=== FULL OUTER JOIN Correctness Test ==="
echo "Binary: $(stat --format='%y' $SIRIUS/build/release/duckdb | cut -d. -f1)"
echo ""

if [[ -f "$HOME/.sirius/sirius.cfg" ]]; then
    mv "$HOME/.sirius/sirius.cfg" "$HOME/.sirius/sirius.cfg.bak"
fi
trap 'mv "$HOME/.sirius/sirius.cfg.bak" "$HOME/.sirius/sirius.cfg" 2>/dev/null || true' EXIT

RUN() { LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" "$SIRIUS/build/release/duckdb" -unsigned "$DB" "$@"; }

normalize() { sed 's/[[:space:]]*$//' | tr -d '\r'; }
run_cpu() { RUN -csv -c "$1" 2>/dev/null | tail -n +2 | normalize; }
run_gpu() {
    local escaped
    escaped=$(echo "$1" | sed "s/'/''/g")
    printf ".mode csv\nCALL gpu_buffer_init('${CACHE_GB} GB', '${PROC_GB} GB');\nCALL gpu_processing('$escaped');" | \
        RUN 2>/dev/null \
        | grep -v "^Success\|^Error in GPU\|^===\|^boolean\|^0 rows\|^true\|^false\|^$" \
        | tail -n +2 | normalize
}

PASS=0; FAIL=0
check() {
    local name="$1" cpu_out gpu_out
    cpu_out=$(run_cpu "$2" | sort)
    gpu_out=$(run_gpu "$2" | sort)
    if [[ "$cpu_out" == "$gpu_out" ]]; then
        echo "  PASS  $name"
        PASS=$(( PASS + 1 ))
    else
        echo "  FAIL  $name"
        echo "    CPU rows : $(echo "$cpu_out" | wc -l)"
        echo "    GPU rows : $(echo "$gpu_out" | wc -l)"
        diff <(echo "$cpu_out") <(echo "$gpu_out") | head -15
        FAIL=$(( FAIL + 1 ))
    fi
}

# ---------------------------------------------------------------------------
# 1. Tiny synthetic: verifies all three row types come through
#    key 'a' = left-only, key 'd' = right-only, keys 'b'/'c' = matched
#    Uses VALUES() instead of UNION ALL (UNION ALL not supported in gpu_processing)
# ---------------------------------------------------------------------------
check "synthetic_left_right_matched" "
WITH l(k, v) AS (VALUES ('a', 10), ('b', 20), ('c', 30)),
     r(k, v) AS (VALUES ('b', 200), ('c', 300), ('d', 400))
SELECT CASE WHEN l.k IS NOT NULL THEN l.k ELSE r.k END AS key,
       CASE WHEN l.v IS NOT NULL THEN l.v ELSE 0 END    AS lv,
       CASE WHEN r.v IS NOT NULL THEN r.v ELSE 0 END    AS rv
FROM l FULL OUTER JOIN r ON l.k = r.k ORDER BY key"

# ---------------------------------------------------------------------------
# 2. Q05 real data: full result set (varchar keys, two aggregation CTEs)
#    Note: CASE WHEN instead of COALESCE — GPU expression translator workaround
# ---------------------------------------------------------------------------
Q05="
WITH outflows AS (
    SELECT src.entity, SUM(f.amount) AS total_outflow
    FROM address_flows_daily_dict f
    JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id
    GROUP BY 1
),
inflows AS (
    SELECT dst.entity, SUM(f.amount) AS total_inflow
    FROM address_flows_daily_dict f
    JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id
    GROUP BY 1
)
SELECT
    CASE WHEN o.entity IS NOT NULL THEN o.entity ELSE i.entity END AS entity,
    CASE WHEN i.total_inflow  IS NOT NULL THEN i.total_inflow  ELSE 0 END AS total_inflow,
    CASE WHEN o.total_outflow IS NOT NULL THEN o.total_outflow ELSE 0 END AS total_outflow
FROM outflows o FULL OUTER JOIN inflows i ON o.entity = i.entity
ORDER BY entity"

# Check entity names only (not SUM values — float accumulation order differs between CPU/GPU)
check "q05_entity_names_match"         "SELECT entity FROM ($Q05) t ORDER BY entity"
check "q05_row_count"                  "SELECT COUNT(*) FROM ($Q05) t"
check "q05_left_only_send_entities" "
WITH outflows AS (
    SELECT src.entity, SUM(f.amount) AS total_outflow
    FROM address_flows_daily_dict f
    JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id GROUP BY 1
), inflows AS (
    SELECT dst.entity, SUM(f.amount) AS total_inflow
    FROM address_flows_daily_dict f
    JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id GROUP BY 1
)
SELECT CASE WHEN o.entity IS NOT NULL THEN o.entity ELSE i.entity END AS entity
FROM outflows o FULL OUTER JOIN inflows i ON o.entity = i.entity
WHERE i.entity IS NULL ORDER BY entity"

check "q05_right_only_recv_entities" "
WITH outflows AS (
    SELECT src.entity, SUM(f.amount) AS total_outflow
    FROM address_flows_daily_dict f
    JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id GROUP BY 1
), inflows AS (
    SELECT dst.entity, SUM(f.amount) AS total_inflow
    FROM address_flows_daily_dict f
    JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id GROUP BY 1
)
SELECT CASE WHEN o.entity IS NOT NULL THEN o.entity ELSE i.entity END AS entity
FROM outflows o FULL OUTER JOIN inflows i ON o.entity = i.entity
WHERE o.entity IS NULL ORDER BY entity"

# ---------------------------------------------------------------------------
# 5. Int-key FULL OUTER JOIN: different key type (INTEGER vs VARCHAR in Q05)
#    Senders vs receivers — most addr_ids appear on both sides, some only one
# ---------------------------------------------------------------------------
check "int_key_senders_vs_receivers" "
WITH senders   AS (SELECT from_addr_id AS id, COUNT(*) AS sc FROM address_flows_daily_dict GROUP BY 1),
     receivers AS (SELECT to_addr_id   AS id, COUNT(*) AS rc FROM address_flows_daily_dict GROUP BY 1)
SELECT
    CASE WHEN s.id IS NOT NULL THEN s.id ELSE r.id END AS addr_id,
    CASE WHEN s.sc IS NOT NULL THEN s.sc ELSE 0 END    AS send_count,
    CASE WHEN r.rc IS NOT NULL THEN r.rc ELSE 0 END    AS recv_count
FROM senders s FULL OUTER JOIN receivers r ON s.id = r.id
ORDER BY addr_id LIMIT 100"

# ---------------------------------------------------------------------------
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
