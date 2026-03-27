#!/usr/bin/env bash
# Validated scaling benchmark: physical tables at each size, result comparison
# Creates separate DuckDB files per date range, benchmarks GPU vs CPU,
# compares outputs to catch silent fallback.
#
# Requirements:
#   - Sirius built on feature/union-all-gpu-processing branch (or dev with UNION ALL cherry-pick)
#   - ENABLE_LEGACY_SIRIUS=ON in CMake
#   - ~/.sirius/sirius.cfg moved aside (script does this automatically)
#   - Source DB with address_flows_daily_dict + entity_address_map_int tables
#
# Known workarounds baked into queries:
#   - CAST(SUM(...) AS DOUBLE) — avoids DECIMAL bug in gpu_physical_result_collector
#   - CASE WHEN instead of COALESCE — COALESCE not implemented in gpu_processing
#   - UNION ALL requires feature/union-all-gpu-processing branch
#
# CPU timing note: 5-sample median has ~15ms variance; GPU timings are more stable.
# Data distribution note: Q2 2024 has 8x fewer entity-143 matching rows than Q1
# — causes irregular per-slice scaling patterns, not a bug.
#
# =============================================================================
# Machine config — all values are env-var overridable, nothing to edit here.
# Copy scripts/machines.env, fill it in, then source it before running:
#
#   cp scripts/machines.env my_machine.env
#   # edit my_machine.env
#   source my_machine.env && bash scripts/scaling_validated.sh
#
# Running a subset of slices:
#   bash scripts/scaling_validated.sh 3mo 6mo        # only 3mo and 6mo
#   bash scripts/scaling_validated.sh 12mo            # single slice
#   bash scripts/scaling_validated.sh                 # all slices (default)
# =============================================================================
set -uo pipefail

SIRIUS_DIR="${SIRIUS_DIR:-$HOME/sirius}"
PIXI_ENV="${PIXI_ENV:-default}"
SIRIUS_BIN="$SIRIUS_DIR/build/release/duckdb"
LIB_PATH="$SIRIUS_DIR/.pixi/envs/$PIXI_ENV/lib"
SRC_DB="${SRC_DB:-/tmp/crypto_demo.duckdb}"
WORK_DIR="${WORK_DIR:-/tmp/scaling_dbs}"
RESULTS="${RESULTS:-/tmp/scaling_validated_results.csv}"

# GPU memory split: data cache + working pool. Total should leave ~10% VRAM free.
# OOM note: the working pool is the bottleneck, not the data cache.
# 24GB GPU: CACHE_DATA=12 CACHE_WORK=8 → OOM at ~320M rows (15mo)
# 40GB GPU: CACHE_DATA=18 CACHE_WORK=18 → should handle all slices
CACHE_DATA="${CACHE_DATA:-12 GB}"
CACHE_WORK="${CACHE_WORK:-8 GB}"

mkdir -p "$WORK_DIR"

# Date cutoffs for cumulative slices (bottom to top)
SLICES=(
    "3mo  2024-01-01 2024-03-31"
    "6mo  2024-01-01 2024-06-30"
    "9mo  2024-01-01 2024-09-30"
    "12mo 2024-01-01 2024-12-31"
    "15mo 2024-01-01 2025-03-31"
    "18mo 2024-01-01 2025-06-30"
    "21mo 2024-01-01 2025-09-30"
    "22mo 2024-01-01 2025-10-31"
    "24mo 2024-01-01 2025-12-31"
    "25mo 2024-01-01 2026-01-31"
    "27mo 2024-01-01 2026-03-25"
)

# Filter slices if labels passed on command line
if [[ $# -gt 0 ]]; then
    FILTER=("$@")
    FILTERED=()
    for spec in "${SLICES[@]}"; do
        label="${spec%% *}"
        for f in "${FILTER[@]}"; do
            [[ "$label" == "$f" ]] && FILTERED+=("$spec") && break
        done
    done
    if [[ ${#FILTERED[@]} -eq 0 ]]; then
        echo "No matching slices for: $*"
        echo "Available: ${SLICES[*]%% *}"
        exit 1
    fi
    SLICES=("${FILTERED[@]}")
fi

# ---- Queries ----
# Sankey 3-hop
Q_SANKEY='WITH seed_addrs AS (SELECT addr_id FROM entity_address_map_int WHERE entity_id = 143), hop1_edges AS (SELECT 143 AS from_eid, dst.entity_id AS to_eid, CAST(SUM(f.amount) AS DOUBLE) AS total_amount FROM address_flows_daily_dict f JOIN seed_addrs s ON f.from_addr_id = s.addr_id JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE dst.entity_id != 143 GROUP BY 1, 2), hop1_frontier AS (SELECT to_eid AS eid, COUNT(*) AS _c FROM hop1_edges GROUP BY 1), hop2_edges AS (SELECT src.entity_id AS from_eid, dst.entity_id AS to_eid, CAST(SUM(f.amount) AS DOUBLE) AS total_amount FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id JOIN hop1_frontier h1 ON src.entity_id = h1.eid JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE dst.entity_id != 143 AND dst.entity_id NOT IN (SELECT eid FROM hop1_frontier) GROUP BY 1, 2), hop2_frontier AS (SELECT to_eid AS eid, COUNT(*) AS _c FROM hop2_edges GROUP BY 1), hop3_edges AS (SELECT src.entity_id AS from_eid, dst.entity_id AS to_eid, CAST(SUM(f.amount) AS DOUBLE) AS total_amount FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id JOIN hop2_frontier h2 ON src.entity_id = h2.eid JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE dst.entity_id != 143 AND dst.entity_id NOT IN (SELECT eid FROM hop1_frontier) AND dst.entity_id NOT IN (SELECT eid FROM hop2_frontier) GROUP BY 1, 2) SELECT 1 AS hop, from_eid, to_eid, total_amount FROM hop1_edges UNION ALL SELECT 2 AS hop, from_eid, to_eid, total_amount FROM hop2_edges UNION ALL SELECT 3 AS hop, from_eid, to_eid, total_amount FROM hop3_edges'

# Q02: entity rollup
Q02='SELECT dst.entity_id, CAST(SUM(f.amount) AS DOUBLE) AS total_vol, COUNT(*) AS tx_cnt FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE src.entity_id = 143 AND dst.entity_id != 143 GROUP BY 1 ORDER BY total_vol DESC'

# Q03: inflow rollup
Q03='SELECT src.entity_id, CAST(SUM(f.amount) AS DOUBLE) AS total_vol, COUNT(*) AS tx_cnt FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE dst.entity_id = 143 AND src.entity_id != 143 GROUP BY 1 ORDER BY total_vol DESC'

# Q04: daily volume
Q04='SELECT f.date, CAST(SUM(f.amount) AS DOUBLE) AS daily_vol, CAST(SUM(f.tx_count) AS DOUBLE) AS daily_tx FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id WHERE src.entity_id = 143 GROUP BY 1 ORDER BY 1'

# Q05: bidirectional FULL OUTER JOIN — aliases both sides to avoid DuckDB duplicate-column-name
# binder error. No CASE WHEN/COALESCE (gpu_processing can't handle IS NOT NULL on FULL OUTER JOIN
# output columns). NULLs left as-is; normalize_csv handles empty/NULL identically.
Q05='SELECT o.entity_id AS out_entity_id, i.entity_id AS in_entity_id, o.out_vol, i.in_vol FROM (SELECT dst.entity_id, CAST(SUM(f.amount) AS DOUBLE) AS out_vol FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE src.entity_id = 143 AND dst.entity_id != 143 GROUP BY 1) o FULL OUTER JOIN (SELECT src.entity_id, CAST(SUM(f.amount) AS DOUBLE) AS in_vol FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE dst.entity_id = 143 AND src.entity_id != 143 GROUP BY 1) i ON o.entity_id = i.entity_id'

# Q06: top flows
Q06='SELECT src.entity_id AS from_eid, dst.entity_id AS to_eid, f.date, f.amount FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE src.entity_id = 143 AND dst.entity_id != 143 ORDER BY f.amount DESC LIMIT 100'

escape_sql() {
    echo "${1//\'/\'\'}"
}

run_duckdb() {
    local db="$1"
    shift
    LD_LIBRARY_PATH="$LIB_PATH:${LD_LIBRARY_PATH:-}" "$SIRIUS_BIN" "$db" -unsigned "$@" 2>&1
}

# Normalize CSV: truncate floats to 9 sig digits, sort rows.
# GPU/CPU floats differ in last 1-2 digits; this makes them compare equal.
normalize_csv() {
    sed '/^$/d' "$1" | tr -d '\r' | awk -F',' '{
        for(i=1;i<=NF;i++){
            if($i ~ /^-?[0-9]*\.[0-9]+(e[+-]?[0-9]+)?$/){
                printf "%.9g",$i
            } else {
                printf "%s",$i
            }
            if(i<NF) printf ","
        }
        print ""
    }' | sort
}

# Benchmark query: GPU and CPU in one session each.
# Each session: capture result on warmup 1, do 2 warmups total, then 5 timed runs.
# Returns: "status gpu_ms cpu_ms"
benchmark_query() {
    local db="$1"
    local query="$2"
    local query_esc
    query_esc=$(escape_sql "$query")
    local tmp_gpu_out="/tmp/_bench_gpu.txt"
    local tmp_gpu_csv="/tmp/_bench_gpu.csv"
    local tmp_cpu_out="/tmp/_bench_cpu.txt"
    local tmp_cpu_csv="/tmp/_bench_cpu.csv"

    # ---- GPU session: init + capture warmup + 5 timed ----
    run_duckdb "$db" > "$tmp_gpu_out" <<GPUEOF || true
CALL gpu_buffer_init('$CACHE_DATA', '$CACHE_WORK');
.mode csv
.headers off
.output $tmp_gpu_csv
CALL gpu_processing('$query_esc');
.output
.mode duckbox
CALL gpu_processing('$query_esc');
.timer on
CALL gpu_processing('$query_esc');
CALL gpu_processing('$query_esc');
CALL gpu_processing('$query_esc');
CALL gpu_processing('$query_esc');
CALL gpu_processing('$query_esc');
GPUEOF

    local gpu_err
    gpu_err=$(grep -c "Error in GPUExecuteQuery" "$tmp_gpu_out" || true)
    if [[ "$gpu_err" -gt 0 || ! -s "$tmp_gpu_csv" ]]; then
        echo "FALLBACK 0 0"
        rm -f "$tmp_gpu_out" "$tmp_gpu_csv" "$tmp_cpu_out" "$tmp_cpu_csv"
        return
    fi

    # ---- CPU session: capture warmup + 5 timed ----
    run_duckdb "$db" > "$tmp_cpu_out" <<CPUEOF || true
.mode csv
.headers off
.output $tmp_cpu_csv
$query;
.output
.mode duckbox
$query;
.timer on
$query;
$query;
$query;
$query;
$query;
CPUEOF

    # ---- Compare results ----
    local match
    if diff -q <(normalize_csv "$tmp_gpu_csv") <(normalize_csv "$tmp_cpu_csv") > /dev/null 2>&1; then
        match="MATCH"
    else
        local gpu_rows cpu_rows
        gpu_rows=$(sed '/^$/d' "$tmp_gpu_csv" | wc -l)
        cpu_rows=$(sed '/^$/d' "$tmp_cpu_csv" | wc -l)
        if [[ "$gpu_rows" == "$cpu_rows" ]]; then
            match="MISMATCH(${cpu_rows}rows-same,values-differ)"
        else
            match="MISMATCH(cpu=${cpu_rows}rows gpu=${gpu_rows}rows)"
        fi
    fi

    # ---- Extract timings (median of 5, .timer was on only for timed runs) ----
    local gpu_med cpu_med
    gpu_med=$(grep -oP 'real \K[0-9.]+' "$tmp_gpu_out" | sort -n | sed -n '3p')
    cpu_med=$(grep -oP 'real \K[0-9.]+' "$tmp_cpu_out" | sort -n | sed -n '3p')

    local gpu_ms cpu_ms
    gpu_ms=$(awk "BEGIN {printf \"%.0f\", ${gpu_med:-0} * 1000}")
    cpu_ms=$(awk "BEGIN {printf \"%.0f\", ${cpu_med:-0} * 1000}")

    echo "$match $gpu_ms $cpu_ms"
    rm -f "$tmp_gpu_out" "$tmp_gpu_csv" "$tmp_cpu_out" "$tmp_cpu_csv"
}

# ---- Main ----
echo "Validated Scaling Benchmark"
echo "==========================="
echo "Source DB: $SRC_DB"
echo "Cache: $CACHE_DATA data + $CACHE_WORK working"
echo ""

# Move sirius.cfg aside
if [[ -f ~/.sirius/sirius.cfg ]]; then
    mv ~/.sirius/sirius.cfg ~/.sirius/sirius.cfg.bak
    echo "Moved sirius.cfg aside"
fi

echo "slice,rows,query,status,gpu_ms,cpu_ms,speedup" > "$RESULTS"

for spec in "${SLICES[@]}"; do
    read -r label start end <<< "$spec"
    db="$WORK_DIR/flows_${label}.duckdb"

    echo ""
    echo "============================================"
    echo "Creating $label ($start to $end)"
    echo "============================================"

    # Check if we have enough disk
    avail_gb=$(df /tmp --output=avail -BG | tail -1 | grep -oP '\d+')
    if (( avail_gb < 15 )); then
        echo "  LOW DISK ($avail_gb GB). Stopping."
        break
    fi

    # Create physical DB with filtered data
    rm -f "$db"
    run_duckdb "$db" <<CREATEEOF > /dev/null
ATTACH '$SRC_DB' AS src (READ_ONLY);
CREATE TABLE address_flows_daily_dict AS
    SELECT * FROM src.address_flows_daily_dict
    WHERE date >= '$start' AND date <= '$end';
CREATE TABLE entity_address_map_int AS
    SELECT * FROM src.entity_address_map_int;
CREATEEOF

    rows=$(run_duckdb "$db" -c "SELECT COUNT(*) FROM address_flows_daily_dict;" | grep -oP '\d{6,}' | head -1)
    db_size=$(du -sh "$db" | cut -f1)
    echo "  Rows: $rows  DB: $db_size"

    # Benchmark each query
    fallback_count=0
    query_count=0
    for qname in SANKEY Q02 Q03 Q04 Q05 Q06; do
        qvar="Q_$qname"
        [[ "$qname" != "SANKEY" ]] && qvar="$qname"
        query="${!qvar:-}"
        [[ -z "$query" ]] && query="${!qname:-}"

        echo -n "  $qname: "
        result=$(benchmark_query "$db" "$query")
        status=$(echo "$result" | awk '{print $1}')
        gpu_ms=$(echo "$result" | awk '{print $2}')
        cpu_ms=$(echo "$result" | awk '{print $3}')
        query_count=$((query_count + 1))

        if [[ "$status" == "MATCH" && "$gpu_ms" != "0" ]]; then
            speedup=$(awk "BEGIN {printf \"%.1f\", $cpu_ms / $gpu_ms}")
            echo "GPU=${gpu_ms}ms CPU=${cpu_ms}ms ${speedup}x [OK]"
            echo "$label,$rows,$qname,$status,$gpu_ms,$cpu_ms,$speedup" >> "$RESULTS"
        elif [[ "$status" == "FALLBACK" ]]; then
            echo "GPU FALLBACK — working memory exceeded"
            echo "$label,$rows,$qname,FALLBACK,,,," >> "$RESULTS"
            fallback_count=$((fallback_count + 1))
        elif [[ "$status" == "MATCH" && "$gpu_ms" == "0" ]]; then
            echo "MATCH but GPU timing missing — check gpu_buffer_init output"
            echo "$label,$rows,$qname,MATCH_NO_TIMING,,$cpu_ms," >> "$RESULTS"
        else
            echo "RESULT MISMATCH — $status"
            echo "$label,$rows,$qname,$status,$gpu_ms,$cpu_ms,MISMATCH" >> "$RESULTS"
        fi
    done

    # Stop early if ALL queries in this slice fell back
    if [[ "$fallback_count" -eq "$query_count" ]]; then
        echo "  All queries fell back — stopping (larger slices will also fail)"
        rm -f "$db"
        break
    fi

    # Delete the per-slice DB to save disk (keep results in CSV)
    rm -f "$db"
done

# Restore sirius.cfg
if [[ -f ~/.sirius/sirius.cfg.bak ]]; then
    mv ~/.sirius/sirius.cfg.bak ~/.sirius/sirius.cfg
    echo ""
    echo "Restored sirius.cfg"
fi

echo ""
echo "============================================"
echo "FINAL RESULTS"
echo "============================================"
column -t -s',' "$RESULTS"
echo ""
echo "Raw: $RESULTS"
