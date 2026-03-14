#!/usr/bin/env bash
# Fix V3 queries with correct column names (from_addr_id/to_addr_id)
# Run after main benchmark to fill in missing V3 data
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
SIRIUS_DIR="${SIRIUS_DIR:-$HOME/sirius-dev}"
DATA_DIR="${DATA_DIR:-$DEMO_DIR/data}"
PIXI_ENV="${PIXI_ENV:-cuda12}"

DUCKDB_BIN="$SIRIUS_DIR/build/release/duckdb"
PIXI_LIB="$SIRIUS_DIR/.pixi/envs/$PIXI_ENV/lib"
CONFIG_PATH="$SCRIPT_DIR/sirius_rtx6000.cfg"
RESULTS_DIR="$SCRIPT_DIR/results"

mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CSV="$RESULTS_DIR/v3_benchmark_${TIMESTAMP}.csv"
echo "dataset,query,engine,run_type,time_ms" > "$CSV"

DB_OCT="$DATA_DIR/crypto_demo_2025_oct.duckdb"
DB_Q4="$DATA_DIR/crypto_demo_2025q4.duckdb"

run_one() {
    local dataset="$1" qname="$2" engine="$3" run_type="$4"
    local db_path="$5" sql="$6"

    local full_sql=".timer on"$'\n'
    case "$engine" in
        cpu)
            full_sql+="$sql;"$'\n'
            ;;
        gpu_processing)
            local esc="${sql//\'/\'\'}"
            full_sql+="CALL gpu_buffer_init('16 GB', '6 GB');"$'\n'
            full_sql+="CALL gpu_processing('$esc');"$'\n'
            ;;
        gpu_execution)
            local esc="${sql//\'/\'\'}"
            full_sql+="CALL gpu_execution('$esc');"$'\n'
            ;;
    esac

    local env_vars="LD_LIBRARY_PATH=$PIXI_LIB:${LD_LIBRARY_PATH:-}"
    [[ "$engine" == "gpu_execution" ]] && env_vars+=" SIRIUS_CONFIG_FILE=$CONFIG_PATH SIRIUS_LOG_LEVEL=warn"

    local start end elapsed_ms output
    start=$(date +%s%N)
    output=$(echo "$full_sql" | env $env_vars "$DUCKDB_BIN" "$db_path" 2>&1) || true
    end=$(date +%s%N)
    elapsed_ms=$(( (end - start) / 1000000 ))

    local real_time timer_ms
    real_time=$(echo "$output" | grep "Run Time" | grep -oP 'real \K[0-9.]+' | tail -1)
    timer_ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}' 2>/dev/null || echo "0")

    local has_error=false
    if echo "$output" | grep -qiP "^(Error|Binder Error|Parser Error|Catalog Error|Invalid Error|INTERNAL Error|IO Error|Fatal|Out of Memory|bad_alloc|OOM)"; then
        has_error=true
    elif echo "$output" | grep -qi "FAILURE maximum pool size exceeded\|Not enough room to grow\|CUDA error\|allocation failed\|Set operation not supported"; then
        has_error=true
    fi

    if $has_error; then
        local err_line
        err_line=$(echo "$output" | grep -iP "(Error|FAILURE|OOM|bad_alloc)" | head -1 | cut -c1-120 | tr ',' ';')
        echo "  $qname | $engine | $run_type | ERROR ${elapsed_ms}ms | $err_line"
        echo "$dataset,$qname,$engine,${run_type}_error,$elapsed_ms" >> "$CSV"
    else
        [[ "$timer_ms" == "0" ]] && timer_ms="$elapsed_ms"
        echo "  $qname | $engine | $run_type | ${timer_ms}ms (wall ${elapsed_ms}ms)"
        echo "$dataset,$qname,$engine,$run_type,$timer_ms" >> "$CSV"
    fi
}

# Correct V3 queries using from_addr_id / to_addr_id
Q02_V3="SELECT e1.entity AS from_entity, e2.entity AS to_entity, f.date, f.asset, SUM(f.amount) AS total_amount, SUM(f.tx_count) AS total_tx FROM address_flows_daily_dict f LEFT JOIN entity_address_map_dict e1 ON f.from_addr_id = e1.addr_id LEFT JOIN entity_address_map_dict e2 ON f.to_addr_id = e2.addr_id GROUP BY 1, 2, 3, 4 ORDER BY total_amount DESC LIMIT 100"
Q03_V3="SELECT e1.entity AS from_entity, e2.entity AS to_entity, SUM(f.amount) AS total_amount, COUNT(*) AS flow_count FROM address_flows_daily_dict f JOIN entity_address_map_dict e1 ON f.from_addr_id = e1.addr_id JOIN entity_address_map_dict e2 ON f.to_addr_id = e2.addr_id GROUP BY 1, 2 ORDER BY total_amount DESC LIMIT 100"
Q04_V3="SELECT f.date, SUM(f.amount) AS daily_amount, SUM(f.tx_count) AS daily_tx FROM address_flows_daily_dict f JOIN entity_address_map_dict e1 ON f.from_addr_id = e1.addr_id JOIN entity_address_map_dict e2 ON f.to_addr_id = e2.addr_id WHERE e1.entity = 'binance' AND e2.entity = 'coinbase' GROUP BY 1 ORDER BY 1"
Q05_V3="SELECT entity, SUM(inflow) AS inflow, SUM(outflow) AS outflow FROM (SELECT e.entity, f.amount AS inflow, 0 AS outflow FROM address_flows_daily_dict f JOIN entity_address_map_dict e ON f.to_addr_id = e.addr_id UNION ALL SELECT e.entity, 0 AS inflow, f.amount AS outflow FROM address_flows_daily_dict f JOIN entity_address_map_dict e ON f.from_addr_id = e.addr_id) sub GROUP BY 1 ORDER BY inflow DESC LIMIT 50"
Q06_V3="SELECT e1.category AS from_cat, e2.category AS to_cat, SUM(f.amount) AS total_amount, COUNT(*) AS flow_count FROM address_flows_daily_dict f JOIN entity_address_map_dict e1 ON f.from_addr_id = e1.addr_id JOIN entity_address_map_dict e2 ON f.to_addr_id = e2.addr_id GROUP BY 1, 2 ORDER BY total_amount DESC"

QNAMES=(Q02_V3 Q03_V3 Q04_V3 Q05_V3 Q06_V3)
QUERIES=("$Q02_V3" "$Q03_V3" "$Q04_V3" "$Q05_V3" "$Q06_V3")

echo "=== V3 Benchmark Fix ==="
echo "Output: $CSV"

DB_LABELS=("1mo" "q4")
DB_PATHS=("$DB_OCT" "$DB_Q4")

for di in 0 1; do
    db_label="${DB_LABELS[$di]}"
    db_path="${DB_PATHS[$di]}"
    echo ""
    echo "== $db_label =="

    for engine in cpu gpu_processing gpu_execution; do
        echo "--- $db_label / V3 / $engine ---"
        for i in "${!QNAMES[@]}"; do
            if [[ "$engine" == "cpu" ]]; then
                for r in 1 2 3; do
                    run_one "$db_label" "${QNAMES[$i]}" "$engine" "run_$r" "$db_path" "${QUERIES[$i]}"
                done
            else
                run_one "$db_label" "${QNAMES[$i]}" "$engine" "cold" "$db_path" "${QUERIES[$i]}"
                for r in 1 2 3; do
                    run_one "$db_label" "${QNAMES[$i]}" "$engine" "hot_$r" "$db_path" "${QUERIES[$i]}"
                done
            fi
        done
    done
done

echo ""
echo "=== V3 Results ==="
cat "$CSV"
echo ""
echo "Complete: $(date)"
