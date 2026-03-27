#!/usr/bin/env bash
# Validated scaling benchmark: physical tables at each size, result comparison
# Creates separate DuckDB files per date range, benchmarks GPU vs CPU,
# compares outputs to catch silent fallback.
#
# Requirements:
#   - Sirius built on feature/union-all-gpu-processing branch (or dev with UNION ALL cherry-pick)
#   - ENABLE_LEGACY_SIRIUS=ON in CMake
#   - ~/.sirius/sirius.cfg moved aside (script does this automatically)
#   - Source DB at /tmp/crypto_slim.duckdb with address_flows_daily_dict + entity_address_map_int
#
# Known workarounds baked into queries:
#   - CAST(SUM(...) AS DOUBLE) — avoids DECIMAL bug in gpu_physical_result_collector
#   - CASE WHEN instead of COALESCE — COALESCE not implemented in gpu_processing
set -uo pipefail

SIRIUS_DIR="${SIRIUS_DIR:-$HOME/sirius-dev}"
SIRIUS_BIN="$SIRIUS_DIR/build/release/duckdb"
LIB_PATH="$SIRIUS_DIR/.pixi/envs/cuda12/lib"
SRC_DB="/tmp/crypto_slim.duckdb"
WORK_DIR="/tmp/scaling_dbs"
RESULTS="/tmp/scaling_validated_results.csv"

# Cache config: 12 GB data + 8 GB working = 20 GB of 24 GB VRAM
CACHE_DATA="12 GB"
CACHE_WORK="8 GB"

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
    "27mo 2024-01-01 2026-03-25"
)

# ---- Queries ----
# Sankey 3-hop
Q_SANKEY='WITH seed_addrs AS (SELECT addr_id FROM entity_address_map_int WHERE entity_id = 143), hop1_edges AS (SELECT 143 AS from_eid, dst.entity_id AS to_eid, CAST(SUM(f.amount) AS DOUBLE) AS total_amount FROM address_flows_daily_dict f JOIN seed_addrs s ON f.from_addr_id = s.addr_id JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE dst.entity_id != 143 GROUP BY 1, 2), hop1_frontier AS (SELECT to_eid AS eid, COUNT(*) AS _c FROM hop1_edges GROUP BY 1), hop2_edges AS (SELECT src.entity_id AS from_eid, dst.entity_id AS to_eid, CAST(SUM(f.amount) AS DOUBLE) AS total_amount FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id JOIN hop1_frontier h1 ON src.entity_id = h1.eid JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE dst.entity_id != 143 AND dst.entity_id NOT IN (SELECT eid FROM hop1_frontier) GROUP BY 1, 2), hop2_frontier AS (SELECT to_eid AS eid, COUNT(*) AS _c FROM hop2_edges GROUP BY 1), hop3_edges AS (SELECT src.entity_id AS from_eid, dst.entity_id AS to_eid, CAST(SUM(f.amount) AS DOUBLE) AS total_amount FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id JOIN hop2_frontier h2 ON src.entity_id = h2.eid JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE dst.entity_id != 143 AND dst.entity_id NOT IN (SELECT eid FROM hop1_frontier) AND dst.entity_id NOT IN (SELECT eid FROM hop2_frontier) GROUP BY 1, 2) SELECT 1 AS hop, from_eid, to_eid, total_amount FROM hop1_edges UNION ALL SELECT 2 AS hop, from_eid, to_eid, total_amount FROM hop2_edges UNION ALL SELECT 3 AS hop, from_eid, to_eid, total_amount FROM hop3_edges'

# Q02: entity rollup
Q02='SELECT dst.entity_id, CAST(SUM(f.amount) AS DOUBLE) AS total_vol, COUNT(*) AS tx_cnt FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE src.entity_id = 143 AND dst.entity_id != 143 GROUP BY 1 ORDER BY total_vol DESC'

# Q03: inflow rollup
Q03='SELECT src.entity_id, CAST(SUM(f.amount) AS DOUBLE) AS total_vol, COUNT(*) AS tx_cnt FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE dst.entity_id = 143 AND src.entity_id != 143 GROUP BY 1 ORDER BY total_vol DESC'

# Q04: daily volume
Q04='SELECT f.date, CAST(SUM(f.amount) AS DOUBLE) AS daily_vol, CAST(SUM(f.tx_count) AS DOUBLE) AS daily_tx FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id WHERE src.entity_id = 143 GROUP BY 1 ORDER BY 1'

# Q05: bidirectional (uses FULL OUTER JOIN replacement via UNION)
Q05='SELECT CASE WHEN o.entity_id IS NOT NULL THEN o.entity_id ELSE i.entity_id END AS entity_id, CASE WHEN o.out_vol IS NOT NULL THEN o.out_vol ELSE 0 END AS out_vol, CASE WHEN i.in_vol IS NOT NULL THEN i.in_vol ELSE 0 END AS in_vol FROM (SELECT dst.entity_id, CAST(SUM(f.amount) AS DOUBLE) AS out_vol FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE src.entity_id = 143 AND dst.entity_id != 143 GROUP BY 1) o FULL OUTER JOIN (SELECT src.entity_id, CAST(SUM(f.amount) AS DOUBLE) AS in_vol FROM address_flows_daily_dict f JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id WHERE dst.entity_id = 143 AND src.entity_id != 143 GROUP BY 1) i ON o.entity_id = i.entity_id ORDER BY out_vol DESC NULLS LAST'

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

# Validation: run query on CPU and GPU, compare text output, detect fallback.
# Returns: "status gpu_ms cpu_ms"
benchmark_query() {
    local db="$1"
    local query="$2"
    local query_esc
    query_esc=$(escape_sql "$query")
    local tmp_cpu="/tmp/_bench_cpu.txt"
    local tmp_gpu="/tmp/_bench_gpu.txt"
    local tmp_time="/tmp/_bench_time.txt"

    # ---- GPU: run once, check for fallback, capture output ----
    run_duckdb "$db" > "$tmp_gpu" <<GPUEOF 2>&1 || true
CALL gpu_buffer_init('$CACHE_DATA', '$CACHE_WORK');
CALL gpu_processing('$query_esc');
GPUEOF
    local gpu_err
    gpu_err=$(grep -c "Error in GPUExecuteQuery" "$tmp_gpu" || true)
    if [[ "$gpu_err" -gt 0 ]]; then
        echo "FALLBACK 0 0"
        rm -f "$tmp_cpu" "$tmp_gpu" "$tmp_time"
        return
    fi
    # Strip gpu_buffer_init output (first result block) — keep only query result
    local gpu_out
    gpu_out=$(sed -n '/^┌/,/^└/p' "$tmp_gpu" | tail -n +1 | sed '1,/^└/{ /^└/!d; /^└/d }' | head -40)

    # ---- CPU: run once, capture output ----
    run_duckdb "$db" > "$tmp_cpu" <<CPUEOF || true
$query;
CPUEOF
    local cpu_out
    cpu_out=$(sed -n '/^┌/,/^└/p' "$tmp_cpu" | head -40)

    # ---- Compare results: CSV output with float truncation to 10 sig digits ----
    # GPU/CPU floats differ in last 1-2 digits; truncate to 10 sig digits before diff.
    local tmp_gpu_csv="/tmp/_bench_gpu_csv.txt"
    local tmp_cpu_csv="/tmp/_bench_cpu_csv.txt"

    # Normalize: truncate floats to 10 significant digits, sort rows
    normalize_csv() {
        sed '/^$/d' "$1" | awk -F',' '{
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

    run_duckdb "$db" -csv -noheader <<CPUCSVEOF > "$tmp_cpu_csv" || true
$query;
CPUCSVEOF

    run_duckdb "$db" -csv -noheader <<GPUCSVEOF > "$tmp_gpu_csv" 2>&1 || true
CALL gpu_buffer_init('$CACHE_DATA', '$CACHE_WORK');
CALL gpu_processing('$query_esc');
GPUCSVEOF

    local match
    if diff -q <(normalize_csv "$tmp_cpu_csv") <(normalize_csv "$tmp_gpu_csv") > /dev/null 2>&1; then
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
    rm -f "$tmp_gpu_csv" "$tmp_cpu_csv"

    # ---- CPU timing: 2 warmup + 3 measured, take median ----
    run_duckdb "$db" > "$tmp_time" <<CPUEOF2 || true
$query;
$query;
.timer on
$query;
$query;
$query;
CPUEOF2
    local cpu_med
    cpu_med=$(grep -oP 'real \K[0-9.]+' "$tmp_time" | sort -n | sed -n '2p')

    # ---- GPU timing: 2 warmup + 5 measured, take median ----
    run_duckdb "$db" > "$tmp_time" <<GPUEOF2 || true
CALL gpu_buffer_init('$CACHE_DATA', '$CACHE_WORK');
CALL gpu_processing('$query_esc');
CALL gpu_processing('$query_esc');
.timer on
CALL gpu_processing('$query_esc');
CALL gpu_processing('$query_esc');
CALL gpu_processing('$query_esc');
CALL gpu_processing('$query_esc');
CALL gpu_processing('$query_esc');
GPUEOF2
    # Check timing runs didn't fallback either
    local timing_err
    timing_err=$(grep -c "Error in GPUExecuteQuery" "$tmp_time" || true)
    if [[ "$timing_err" -gt 0 ]]; then
        echo "FALLBACK 0 0"
        rm -f "$tmp_cpu" "$tmp_gpu" "$tmp_time"
        return
    fi
    local gpu_med
    gpu_med=$(grep -oP 'real \K[0-9.]+' "$tmp_time" | sort -n | sed -n '3p')

    local gpu_ms cpu_ms
    gpu_ms=$(echo "${gpu_med:-0} * 1000" | bc 2>/dev/null || echo "0")
    cpu_ms=$(echo "${cpu_med:-0} * 1000" | bc 2>/dev/null || echo "0")

    echo "$match $gpu_ms $cpu_ms"
    rm -f "$tmp_cpu" "$tmp_gpu" "$tmp_time"
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
            speedup=$(echo "scale=1; $cpu_ms / $gpu_ms" | bc 2>/dev/null || echo "N/A")
            echo "GPU=${gpu_ms}ms CPU=${cpu_ms}ms ${speedup}x [OK]"
            echo "$label,$rows,$qname,$status,$gpu_ms,$cpu_ms,$speedup" >> "$RESULTS"
        elif [[ "$status" == "FALLBACK" ]]; then
            echo "GPU FALLBACK — working memory exceeded"
            echo "$label,$rows,$qname,FALLBACK,,,," >> "$RESULTS"
            fallback_count=$((fallback_count + 1))
        else
            echo "RESULT MISMATCH! $status"
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
