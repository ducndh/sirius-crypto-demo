#!/usr/bin/env bash
# =============================================================================
# Comprehensive benchmark — simplified, robust version
# =============================================================================
set -uo pipefail  # no -e, handle errors manually

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
SIRIUS_DIR="${SIRIUS_DIR:-$HOME/sirius-dev}"
DATA_DIR="${DATA_DIR:-$DEMO_DIR/data}"
PIXI_ENV="${PIXI_ENV:-cuda12}"
RESULTS_DIR="$SCRIPT_DIR/results"

DUCKDB_BIN="$SIRIUS_DIR/build/release/duckdb"
PIXI_LIB="$SIRIUS_DIR/.pixi/envs/$PIXI_ENV/lib"
ETH_DIR="$DATA_DIR/eth_transfers"
CONFIG_PATH="$SCRIPT_DIR/sirius_rtx6000.cfg"

mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CSV="$RESULTS_DIR/full_benchmark_${TIMESTAMP}.csv"
echo "dataset,query,engine,run_type,time_ms" > "$CSV"

GPU_NAME=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1 | xargs)
echo "=== Full Benchmark | GPU: $GPU_NAME | $(date) ==="
echo "Output: $CSV"
echo ""

DB_OCT="$DATA_DIR/crypto_demo_2025_oct.duckdb"
DB_Q4="$DATA_DIR/crypto_demo_2025q4.duckdb"

# ---- Run a single query and record result ----
run_one() {
    local dataset="$1" qname="$2" engine="$3" run_type="$4"
    local db_path="$5" sql="$6"
    local extra_prefix="${7:-}"

    local full_sql=""
    [[ -n "$extra_prefix" ]] && full_sql+="$extra_prefix"$'\n'
    full_sql+=".timer on"$'\n'

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

    # Extract .timer output
    local real_time timer_ms
    real_time=$(echo "$output" | grep "Run Time" | grep -oP 'real \K[0-9.]+' | tail -1)
    timer_ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}' 2>/dev/null || echo "0")

    # Detect real errors (not just log lines with "error" in them)
    local has_error=false
    if echo "$output" | grep -qiP "^(Error|Binder Error|Parser Error|Catalog Error|Invalid Error|INTERNAL Error|IO Error|Fatal|Out of Memory|bad_alloc|OOM)"; then
        has_error=true
    elif echo "$output" | grep -qi "FAILURE maximum pool size exceeded\|Not enough room to grow\|CUDA error\|allocation failed"; then
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

# ---- Query definitions ----
Q01="SELECT date, token_address AS asset, from_address, to_address, SUM(CAST(value AS DOUBLE)) AS amount, COUNT(*) AS tx_count FROM token_transfers GROUP BY 1, 2, 3, 4"

Q02_V1="SELECT e1.entity AS from_entity, e2.entity AS to_entity, f.date, f.asset, SUM(f.amount) AS total_amount, SUM(f.tx_count) AS total_tx FROM address_flows_daily f LEFT JOIN entity_address_map e1 ON f.from_address = e1.address LEFT JOIN entity_address_map e2 ON f.to_address = e2.address GROUP BY 1, 2, 3, 4 ORDER BY total_amount DESC LIMIT 100"
Q03_V1="SELECT e1.entity AS from_entity, e2.entity AS to_entity, SUM(f.amount) AS total_amount, COUNT(*) AS flow_count FROM address_flows_daily f JOIN entity_address_map e1 ON f.from_address = e1.address JOIN entity_address_map e2 ON f.to_address = e2.address GROUP BY 1, 2 ORDER BY total_amount DESC LIMIT 100"
Q04_V1="SELECT f.date, SUM(f.amount) AS daily_amount, SUM(f.tx_count) AS daily_tx FROM address_flows_daily f JOIN entity_address_map e1 ON f.from_address = e1.address JOIN entity_address_map e2 ON f.to_address = e2.address WHERE e1.entity = 'binance' AND e2.entity = 'coinbase' GROUP BY 1 ORDER BY 1"
Q05_V1="SELECT entity, SUM(inflow) AS inflow, SUM(outflow) AS outflow FROM (SELECT e.entity, f.amount AS inflow, 0 AS outflow FROM address_flows_daily f JOIN entity_address_map e ON f.to_address = e.address UNION ALL SELECT e.entity, 0 AS inflow, f.amount AS outflow FROM address_flows_daily f JOIN entity_address_map e ON f.from_address = e.address) sub GROUP BY 1 ORDER BY inflow DESC LIMIT 50"
Q06_V1="SELECT e1.category AS from_cat, e2.category AS to_cat, SUM(f.amount) AS total_amount, COUNT(*) AS flow_count FROM address_flows_daily f JOIN entity_address_map e1 ON f.from_address = e1.address JOIN entity_address_map e2 ON f.to_address = e2.address GROUP BY 1, 2 ORDER BY total_amount DESC"

Q02_V3="SELECT e1.entity AS from_entity, e2.entity AS to_entity, f.date, f.asset, SUM(f.amount) AS total_amount, SUM(f.tx_count) AS total_tx FROM address_flows_daily_dict f LEFT JOIN entity_address_map_dict e1 ON f.from_id = e1.addr_id LEFT JOIN entity_address_map_dict e2 ON f.to_id = e2.addr_id GROUP BY 1, 2, 3, 4 ORDER BY total_amount DESC LIMIT 100"
Q03_V3="SELECT e1.entity AS from_entity, e2.entity AS to_entity, SUM(f.amount) AS total_amount, COUNT(*) AS flow_count FROM address_flows_daily_dict f JOIN entity_address_map_dict e1 ON f.from_id = e1.addr_id JOIN entity_address_map_dict e2 ON f.to_id = e2.addr_id GROUP BY 1, 2 ORDER BY total_amount DESC LIMIT 100"
Q04_V3="SELECT f.date, SUM(f.amount) AS daily_amount, SUM(f.tx_count) AS daily_tx FROM address_flows_daily_dict f JOIN entity_address_map_dict e1 ON f.from_id = e1.addr_id JOIN entity_address_map_dict e2 ON f.to_id = e2.addr_id WHERE e1.entity = 'binance' AND e2.entity = 'coinbase' GROUP BY 1 ORDER BY 1"
Q05_V3="SELECT entity, SUM(inflow) AS inflow, SUM(outflow) AS outflow FROM (SELECT e.entity, f.amount AS inflow, 0 AS outflow FROM address_flows_daily_dict f JOIN entity_address_map_dict e ON f.to_id = e.addr_id UNION ALL SELECT e.entity, 0 AS inflow, f.amount AS outflow FROM address_flows_daily_dict f JOIN entity_address_map_dict e ON f.from_id = e.addr_id) sub GROUP BY 1 ORDER BY inflow DESC LIMIT 50"
Q06_V3="SELECT e1.category AS from_cat, e2.category AS to_cat, SUM(f.amount) AS total_amount, COUNT(*) AS flow_count FROM address_flows_daily_dict f JOIN entity_address_map_dict e1 ON f.from_id = e1.addr_id JOIN entity_address_map_dict e2 ON f.to_id = e2.addr_id GROUP BY 1, 2 ORDER BY total_amount DESC"

QNAMES_V1=(Q02_V1 Q03_V1 Q04_V1 Q05_V1 Q06_V1)
QUERIES_V1=("$Q02_V1" "$Q03_V1" "$Q04_V1" "$Q05_V1" "$Q06_V1")
QNAMES_V3=(Q02_V3 Q03_V3 Q04_V3 Q05_V3 Q06_V3)
QUERIES_V3=("$Q02_V3" "$Q03_V3" "$Q04_V3" "$Q05_V3" "$Q06_V3")

run_set() {
    local dataset="$1" engine="$2" variant="$3" db_path="$4" cold_hot="$5"
    echo "--- $dataset / $variant / $engine ---"

    local -n names="QNAMES_${variant}"
    local -n queries="QUERIES_${variant}"

    for i in "${!names[@]}"; do
        if [[ "$cold_hot" == "cold_hot" ]]; then
            run_one "$dataset" "${names[$i]}" "$engine" "cold" "$db_path" "${queries[$i]}"
            for r in 1 2 3; do
                run_one "$dataset" "${names[$i]}" "$engine" "hot_$r" "$db_path" "${queries[$i]}"
            done
        else
            for r in 1 2 3; do
                run_one "$dataset" "${names[$i]}" "$engine" "run_$r" "$db_path" "${queries[$i]}"
            done
        fi
    done
}

# =============================================================================
# PHASE 1: 1 MONTH
# =============================================================================
echo "========================================"
echo " PHASE 1: 1-MONTH (Oct 2025)"
echo "========================================"

echo ""
echo ">> CPU"
for r in 1 2 3; do
    run_one "1mo" "Q01" "cpu" "run_$r" "$DB_OCT" "$Q01"
done
run_set "1mo" "cpu" "V1" "$DB_OCT" "runs"
run_set "1mo" "cpu" "V3" "$DB_OCT" "runs"

echo ""
echo ">> gpu_processing (cold+hot)"
run_one "1mo" "Q01" "gpu_processing" "cold" "$DB_OCT" "$Q01"
for r in 1 2 3; do
    run_one "1mo" "Q01" "gpu_processing" "hot_$r" "$DB_OCT" "$Q01"
done
run_set "1mo" "gpu_processing" "V1" "$DB_OCT" "cold_hot"
run_set "1mo" "gpu_processing" "V3" "$DB_OCT" "cold_hot"

echo ""
echo ">> gpu_execution (cold+hot)"
run_one "1mo" "Q01" "gpu_execution" "cold" "$DB_OCT" "$Q01"
for r in 1 2 3; do
    run_one "1mo" "Q01" "gpu_execution" "hot_$r" "$DB_OCT" "$Q01"
done
run_set "1mo" "gpu_execution" "V1" "$DB_OCT" "cold_hot"
run_set "1mo" "gpu_execution" "V3" "$DB_OCT" "cold_hot"

# =============================================================================
# PHASE 2: Q4 (3 months, 292M token_transfers)
# =============================================================================
echo ""
echo "========================================"
echo " PHASE 2: Q4 (Oct-Dec 2025)"
echo "========================================"

echo ""
echo ">> CPU"
for r in 1 2 3; do
    run_one "q4" "Q01" "cpu" "run_$r" "$DB_Q4" "$Q01"
done
run_set "q4" "cpu" "V1" "$DB_Q4" "runs"
run_set "q4" "cpu" "V3" "$DB_Q4" "runs"

echo ""
echo ">> gpu_processing (cold+hot, Q02-Q06 only, Q01 OOM)"
run_set "q4" "gpu_processing" "V1" "$DB_Q4" "cold_hot"
run_set "q4" "gpu_processing" "V3" "$DB_Q4" "cold_hot"

echo ""
echo ">> gpu_execution (cold+hot)"
run_set "q4" "gpu_execution" "V1" "$DB_Q4" "cold_hot"
run_set "q4" "gpu_execution" "V3" "$DB_Q4" "cold_hot"

# gpu_execution Q01 from raw parquets
Q01_PARQUET="SELECT date, token_address AS asset, from_address, to_address, SUM(CAST(value AS DOUBLE)) AS amount, COUNT(*) AS tx_count FROM transfers GROUP BY 1, 2, 3, 4"
PARQUET_VIEW_Q4="CREATE VIEW transfers AS SELECT * FROM read_parquet(['$ETH_DIR/date=2025-10-*/*.parquet','$ETH_DIR/date=2025-11-*/*.parquet','$ETH_DIR/date=2025-12-*/*.parquet'], hive_partitioning=true);"
echo "--- q4 / Q01 / gpu_execution (parquet) ---"
run_one "q4" "Q01" "gpu_execution" "cold" "" "$Q01_PARQUET" "$PARQUET_VIEW_Q4"
for r in 1 2 3; do
    run_one "q4" "Q01" "gpu_execution" "hot_$r" "" "$Q01_PARQUET" "$PARQUET_VIEW_Q4"
done

# =============================================================================
# PHASE 3: SCALING — gpu_execution on 3mo and 6mo raw parquets
# =============================================================================
echo ""
echo "========================================"
echo " PHASE 3: Scaling (gpu_execution)"
echo "========================================"

SCALE_Q1="SELECT date, token_address AS asset, SUM(CAST(value AS DOUBLE)) AS total_value, COUNT(*) AS tx_count FROM transfers GROUP BY 1, 2 ORDER BY total_value DESC LIMIT 100"
SCALE_Q2="SELECT from_address, to_address, SUM(CAST(value AS DOUBLE)) AS amount, COUNT(*) AS tx_count FROM transfers GROUP BY 1, 2 ORDER BY amount DESC LIMIT 100"
SCALE_Q3="SELECT date, COUNT(DISTINCT from_address) AS unique_senders, COUNT(DISTINCT to_address) AS unique_receivers FROM transfers GROUP BY 1 ORDER BY 1"

GLOB_3MO="'$ETH_DIR/date=2025-10-*/*.parquet','$ETH_DIR/date=2025-11-*/*.parquet','$ETH_DIR/date=2025-12-*/*.parquet'"
GLOB_6MO="'$ETH_DIR/date=2025-07-*/*.parquet','$ETH_DIR/date=2025-08-*/*.parquet','$ETH_DIR/date=2025-09-*/*.parquet','$ETH_DIR/date=2025-10-*/*.parquet','$ETH_DIR/date=2025-11-*/*.parquet','$ETH_DIR/date=2025-12-*/*.parquet'"

SCALE_QNAMES=(Q1_agg Q2_addr_agg Q3_count_distinct)
SCALE_QUERIES=("$SCALE_Q1" "$SCALE_Q2" "$SCALE_Q3")

for size_label in 3mo 6mo; do
    if [[ "$size_label" == "3mo" ]]; then
        VIEW="CREATE VIEW transfers AS SELECT * FROM read_parquet([$GLOB_3MO], hive_partitioning=true);"
    else
        VIEW="CREATE VIEW transfers AS SELECT * FROM read_parquet([$GLOB_6MO], hive_partitioning=true);"
    fi

    echo ""
    echo ">> $size_label gpu_execution"
    for i in "${!SCALE_QNAMES[@]}"; do
        run_one "$size_label" "${SCALE_QNAMES[$i]}" "gpu_execution" "cold" "" "${SCALE_QUERIES[$i]}" "$VIEW"
        for r in 1 2 3; do
            run_one "$size_label" "${SCALE_QNAMES[$i]}" "gpu_execution" "hot_$r" "" "${SCALE_QUERIES[$i]}" "$VIEW"
        done
    done

    # CPU baseline for scaling
    echo ">> $size_label CPU"
    for i in "${!SCALE_QNAMES[@]}"; do
        for r in 1 2 3; do
            run_one "$size_label" "${SCALE_QNAMES[$i]}" "cpu" "run_$r" "" "${SCALE_QUERIES[$i]}" "$VIEW"
        done
    done
done

echo ""
echo "========================================"
echo " DICT ENCODING BENCHMARK"
echo "========================================"
echo ">> Dict encoding time on 1-month (37M flows)"
DICT_SQL="CREATE TEMP TABLE addr_dict AS WITH all_addrs AS (SELECT DISTINCT from_address AS address FROM address_flows_daily UNION SELECT DISTINCT to_address AS address FROM address_flows_daily) SELECT address, ROW_NUMBER() OVER (ORDER BY address)::INTEGER AS addr_id FROM all_addrs"
DICT_ENCODE_SQL="CREATE TEMP TABLE flows_dict AS SELECT d1.addr_id AS from_id, d2.addr_id AS to_id, f.date, f.asset, f.amount, f.tx_count FROM address_flows_daily f JOIN addr_dict d1 ON f.from_address = d1.address JOIN addr_dict d2 ON f.to_address = d2.address"

# Run dict encoding 3 times
for r in 1 2 3; do
    full_dict_sql=".timer on"$'\n'"$DICT_SQL;"$'\n'"$DICT_ENCODE_SQL;"
    start_ns=$(date +%s%N)
    dict_output=$(echo "$full_dict_sql" | LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" "$DUCKDB_BIN" "$DB_OCT" 2>&1) || true
    end_ns=$(date +%s%N)
    dict_elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    dict_times=$(echo "$dict_output" | grep "Run Time" | grep -oP 'real \K[0-9.]+')
    dict_total_s=$(echo "$dict_times" | awk '{s+=$1} END {printf "%.3f", s}')
    dict_total_ms=$(echo "$dict_total_s" | awk '{printf "%.0f", $1 * 1000}')
    echo "  dict_encode | cpu | run_$r | ${dict_total_ms}ms (wall ${dict_elapsed_ms}ms)"
    echo "1mo,dict_encode,cpu,run_$r,$dict_total_ms" >> "$CSV"
done

echo ""
echo "========================================"
echo " RESULTS SUMMARY"
echo "========================================"
cat "$CSV"
echo ""
echo "Complete: $(date)"
echo "Saved to: $CSV"
