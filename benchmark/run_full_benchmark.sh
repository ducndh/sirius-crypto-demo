#!/usr/bin/env bash
# =============================================================================
# Full Sirius Crypto Demo Benchmark
# Runs CPU, gpu_processing, gpu_execution across 1-month and Q4 datasets
# Records cold (warmup) and hot runs separately.
# =============================================================================
set -euo pipefail

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
RESULTS_CSV="$RESULTS_DIR/full_benchmark_${TIMESTAMP}.csv"
echo "dataset,query,engine,run_type,time_ms" > "$RESULTS_CSV"

# GPU info
GPU_NAME=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1 | xargs)
GPU_MEM_MB=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null | head -1 | xargs)
echo "=== Full Sirius Crypto Benchmark ==="
echo "GPU: $GPU_NAME (${GPU_MEM_MB}MB)"
echo "Results: $RESULTS_CSV"
echo ""

# ---- Query definitions ----
# Q01: Daily address flow aggregation (from token_transfers)
Q01="SELECT date, token_address AS asset, from_address, to_address, SUM(CAST(value AS DOUBLE)) AS amount, COUNT(*) AS tx_count FROM token_transfers GROUP BY 1, 2, 3, 4"

# Q02-Q06 operate on address_flows_daily + entity_address_map
Q02_V1="SELECT e1.entity AS from_entity, e2.entity AS to_entity, f.date, f.asset, SUM(f.amount) AS total_amount, SUM(f.tx_count) AS total_tx FROM address_flows_daily f LEFT JOIN entity_address_map e1 ON f.from_address = e1.address LEFT JOIN entity_address_map e2 ON f.to_address = e2.address GROUP BY 1, 2, 3, 4 ORDER BY total_amount DESC LIMIT 100"
Q03_V1="SELECT e1.entity AS from_entity, e2.entity AS to_entity, SUM(f.amount) AS total_amount, COUNT(*) AS flow_count FROM address_flows_daily f JOIN entity_address_map e1 ON f.from_address = e1.address JOIN entity_address_map e2 ON f.to_address = e2.address GROUP BY 1, 2 ORDER BY total_amount DESC LIMIT 100"
Q04_V1="SELECT f.date, SUM(f.amount) AS daily_amount, SUM(f.tx_count) AS daily_tx FROM address_flows_daily f JOIN entity_address_map e1 ON f.from_address = e1.address JOIN entity_address_map e2 ON f.to_address = e2.address WHERE e1.entity = 'binance' AND e2.entity = 'coinbase' GROUP BY 1 ORDER BY 1"
Q05_V1="SELECT entity, SUM(inflow) AS inflow, SUM(outflow) AS outflow FROM (SELECT e.entity, f.amount AS inflow, 0 AS outflow FROM address_flows_daily f JOIN entity_address_map e ON f.to_address = e.address UNION ALL SELECT e.entity, 0 AS inflow, f.amount AS outflow FROM address_flows_daily f JOIN entity_address_map e ON f.from_address = e.address) sub GROUP BY 1 ORDER BY inflow DESC LIMIT 50"
Q06_V1="SELECT e1.category AS from_cat, e2.category AS to_cat, SUM(f.amount) AS total_amount, COUNT(*) AS flow_count FROM address_flows_daily f JOIN entity_address_map e1 ON f.from_address = e1.address JOIN entity_address_map e2 ON f.to_address = e2.address GROUP BY 1, 2 ORDER BY total_amount DESC"

# V3 (dict-encoded integer joins)
Q02_V3="SELECT e1.entity AS from_entity, e2.entity AS to_entity, f.date, f.asset, SUM(f.amount) AS total_amount, SUM(f.tx_count) AS total_tx FROM address_flows_daily_dict f LEFT JOIN entity_address_map_dict e1 ON f.from_id = e1.addr_id LEFT JOIN entity_address_map_dict e2 ON f.to_id = e2.addr_id GROUP BY 1, 2, 3, 4 ORDER BY total_amount DESC LIMIT 100"
Q03_V3="SELECT e1.entity AS from_entity, e2.entity AS to_entity, SUM(f.amount) AS total_amount, COUNT(*) AS flow_count FROM address_flows_daily_dict f JOIN entity_address_map_dict e1 ON f.from_id = e1.addr_id JOIN entity_address_map_dict e2 ON f.to_id = e2.addr_id GROUP BY 1, 2 ORDER BY total_amount DESC LIMIT 100"
Q04_V3="SELECT f.date, SUM(f.amount) AS daily_amount, SUM(f.tx_count) AS daily_tx FROM address_flows_daily_dict f JOIN entity_address_map_dict e1 ON f.from_id = e1.addr_id JOIN entity_address_map_dict e2 ON f.to_id = e2.addr_id WHERE e1.entity = 'binance' AND e2.entity = 'coinbase' GROUP BY 1 ORDER BY 1"
Q05_V3="SELECT entity, SUM(inflow) AS inflow, SUM(outflow) AS outflow FROM (SELECT e.entity, f.amount AS inflow, 0 AS outflow FROM address_flows_daily_dict f JOIN entity_address_map_dict e ON f.to_id = e.addr_id UNION ALL SELECT e.entity, 0 AS inflow, f.amount AS outflow FROM address_flows_daily_dict f JOIN entity_address_map_dict e ON f.from_id = e.addr_id) sub GROUP BY 1 ORDER BY inflow DESC LIMIT 50"
Q06_V3="SELECT e1.category AS from_cat, e2.category AS to_cat, SUM(f.amount) AS total_amount, COUNT(*) AS flow_count FROM address_flows_daily_dict f JOIN entity_address_map_dict e1 ON f.from_id = e1.addr_id JOIN entity_address_map_dict e2 ON f.to_id = e2.addr_id GROUP BY 1, 2 ORDER BY total_amount DESC"

# ---- Helper functions ----

run_duckdb_cpu() {
    local db_path=$1 query=$2 query_name=$3 dataset=$4

    local sql_cmds=""
    sql_cmds+=".timer on"$'\n'
    sql_cmds+="$query;"$'\n'

    local start end elapsed_ms output
    start=$(date +%s%N)
    output=$(echo "$sql_cmds" | LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" \
        "$DUCKDB_BIN" "$db_path" 2>&1) || true
    end=$(date +%s%N)
    elapsed_ms=$(( (end - start) / 1000000 ))

    if echo "$output" | grep -qi "error\|exception"; then
        echo "  $query_name | cpu | ERROR (${elapsed_ms}ms)"
        echo "$dataset,$query_name,cpu,error,$elapsed_ms" >> "$RESULTS_CSV"
    else
        local real_time timer_ms
        real_time=$(echo "$output" | grep "Run Time" | grep -oP 'real \K[0-9.]+' | tail -1)
        timer_ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}' 2>/dev/null || echo "$elapsed_ms")
        echo "  $query_name | cpu | ${timer_ms}ms"
        echo "$dataset,$query_name,cpu,run,$timer_ms" >> "$RESULTS_CSV"
    fi
}

run_gpu_processing() {
    local db_path=$1 query=$2 query_name=$3 dataset=$4 run_type=$5

    local escaped_sql="${query//\'/\'\'}"
    local sql_cmds=""
    sql_cmds+="CALL gpu_buffer_init('16 GB', '6 GB');"$'\n'
    sql_cmds+=".timer on"$'\n'
    sql_cmds+="CALL gpu_processing('$escaped_sql');"$'\n'

    local start end elapsed_ms output
    start=$(date +%s%N)
    output=$(echo "$sql_cmds" | LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" \
        "$DUCKDB_BIN" "$db_path" 2>&1) || true
    end=$(date +%s%N)
    elapsed_ms=$(( (end - start) / 1000000 ))

    if echo "$output" | grep -qi "error\|exception\|OOM\|bad_alloc"; then
        local err=$(echo "$output" | grep -i "error\|exception\|OOM" | head -1 | tr ',' ';')
        echo "  $query_name | gpu_processing | $run_type ERROR (${elapsed_ms}ms): $err"
        echo "$dataset,$query_name,gpu_processing,${run_type}_error,$elapsed_ms" >> "$RESULTS_CSV"
    else
        local real_time timer_ms
        real_time=$(echo "$output" | grep "Run Time" | grep -oP 'real \K[0-9.]+' | tail -1)
        timer_ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}' 2>/dev/null || echo "$elapsed_ms")
        echo "  $query_name | gpu_processing | $run_type ${timer_ms}ms (wall ${elapsed_ms}ms)"
        echo "$dataset,$query_name,gpu_processing,$run_type,$timer_ms" >> "$RESULTS_CSV"
    fi
}

run_gpu_execution() {
    local db_path=$1 query=$2 query_name=$3 dataset=$4 run_type=$5
    local extra_sql="${6:-}"  # optional SQL to prepend (e.g., CREATE VIEW)

    local escaped_sql="${query//\'/\'\'}"
    local sql_cmds=""
    if [[ -n "$extra_sql" ]]; then
        sql_cmds+="$extra_sql"$'\n'
    fi
    sql_cmds+=".timer on"$'\n'
    sql_cmds+="CALL gpu_execution('$escaped_sql');"$'\n'

    local start end elapsed_ms output
    start=$(date +%s%N)
    output=$(echo "$sql_cmds" | SIRIUS_CONFIG_FILE="$CONFIG_PATH" SIRIUS_LOG_LEVEL=warn \
        LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" \
        "$DUCKDB_BIN" "$db_path" 2>&1) || true
    end=$(date +%s%N)
    elapsed_ms=$(( (end - start) / 1000000 ))

    if echo "$output" | grep -qi "error\|exception\|OOM\|bad_alloc"; then
        local err=$(echo "$output" | grep -i "error\|exception\|OOM" | head -1 | tr ',' ';')
        echo "  $query_name | gpu_execution | $run_type ERROR (${elapsed_ms}ms): $err"
        echo "$dataset,$query_name,gpu_execution,${run_type}_error,$elapsed_ms" >> "$RESULTS_CSV"
    else
        local real_time timer_ms
        real_time=$(echo "$output" | grep "Run Time" | grep -oP 'real \K[0-9.]+' | tail -1)
        timer_ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}' 2>/dev/null || echo "$elapsed_ms")
        echo "  $query_name | gpu_execution | $run_type ${timer_ms}ms (wall ${elapsed_ms}ms)"
        echo "$dataset,$query_name,gpu_execution,$run_type,$timer_ms" >> "$RESULTS_CSV"
    fi
}

run_query_set() {
    local dataset=$1 db_path=$2 engine=$3 variant=$4 num_hot=$5

    local -a names queries
    if [[ "$variant" == "V1" ]]; then
        names=(Q02_V1 Q03_V1 Q04_V1 Q05_V1 Q06_V1)
        queries=("$Q02_V1" "$Q03_V1" "$Q04_V1" "$Q05_V1" "$Q06_V1")
    else
        names=(Q02_V3 Q03_V3 Q04_V3 Q05_V3 Q06_V3)
        queries=("$Q02_V3" "$Q03_V3" "$Q04_V3" "$Q05_V3" "$Q06_V3")
    fi

    echo "--- $dataset / $variant / $engine ---"

    for i in "${!names[@]}"; do
        # Cold run
        if [[ "$engine" == "cpu" ]]; then
            run_duckdb_cpu "$db_path" "${queries[$i]}" "${names[$i]}" "$dataset"
        elif [[ "$engine" == "gpu_processing" ]]; then
            run_gpu_processing "$db_path" "${queries[$i]}" "${names[$i]}" "$dataset" "cold"
            # Hot runs
            for r in $(seq 1 "$num_hot"); do
                run_gpu_processing "$db_path" "${queries[$i]}" "${names[$i]}" "$dataset" "hot_$r"
            done
        elif [[ "$engine" == "gpu_execution" ]]; then
            run_gpu_execution "$db_path" "${queries[$i]}" "${names[$i]}" "$dataset" "cold"
            for r in $(seq 1 "$num_hot"); do
                run_gpu_execution "$db_path" "${queries[$i]}" "${names[$i]}" "$dataset" "hot_$r"
            done
        fi
    done
}

# ---- Q01 special handling ----
run_q01() {
    local dataset=$1 db_path=$2 engine=$3 num_hot=$4
    local extra_sql="${5:-}"

    echo "--- $dataset / Q01 / $engine ---"
    if [[ "$engine" == "cpu" ]]; then
        run_duckdb_cpu "$db_path" "$Q01" "Q01" "$dataset"
    elif [[ "$engine" == "gpu_processing" ]]; then
        run_gpu_processing "$db_path" "$Q01" "Q01" "$dataset" "cold"
        for r in $(seq 1 "$num_hot"); do
            run_gpu_processing "$db_path" "$Q01" "Q01" "$dataset" "hot_$r"
        done
    elif [[ "$engine" == "gpu_execution" ]]; then
        run_gpu_execution "$db_path" "$Q01" "Q01" "$dataset" "cold" "$extra_sql"
        for r in $(seq 1 "$num_hot"); do
            run_gpu_execution "$db_path" "$Q01" "Q01" "$dataset" "hot_$r" "$extra_sql"
        done
    fi
}

# ---- gpu_execution Q01 from raw parquets ----
run_q01_gpu_exec_parquet() {
    local dataset=$1 parquet_glob=$2 num_hot=$3

    local q01_parquet="SELECT date, token_address AS asset, from_address, to_address, SUM(CAST(value AS DOUBLE)) AS amount, COUNT(*) AS tx_count FROM transfers GROUP BY 1, 2, 3, 4"
    local view_sql="CREATE VIEW transfers AS SELECT * FROM read_parquet([$parquet_glob], hive_partitioning=true);"

    echo "--- $dataset / Q01 / gpu_execution (parquet) ---"
    run_gpu_execution "" "$q01_parquet" "Q01" "$dataset" "cold" "$view_sql"
    for r in $(seq 1 "$num_hot"); do
        run_gpu_execution "" "$q01_parquet" "Q01" "$dataset" "hot_$r" "$view_sql"
    done
}

# ---- OOM scaling test (gpu_execution on raw parquets) ----
run_oom_scaling() {
    local dataset=$1 parquet_glob=$2 num_hot=$3

    echo "--- $dataset / gpu_execution scaling ---"

    # Q1: GROUP BY + SUM
    local q1="SELECT date, token_address AS asset, SUM(CAST(value AS DOUBLE)) AS total_value, COUNT(*) AS tx_count FROM transfers GROUP BY 1, 2 ORDER BY total_value DESC LIMIT 100"
    local view_sql="CREATE VIEW transfers AS SELECT * FROM read_parquet([$parquet_glob], hive_partitioning=true);"

    run_gpu_execution "" "$q1" "Q1_agg" "$dataset" "cold" "$view_sql"
    for r in $(seq 1 "$num_hot"); do
        run_gpu_execution "" "$q1" "Q1_agg" "$dataset" "hot_$r" "$view_sql"
    done

    # Q2: Address aggregation (no || concat)
    local q2="SELECT from_address, to_address, SUM(CAST(value AS DOUBLE)) AS amount, COUNT(*) AS tx_count FROM transfers GROUP BY 1, 2 ORDER BY amount DESC LIMIT 100"
    run_gpu_execution "" "$q2" "Q2_addr_agg" "$dataset" "cold" "$view_sql"
    for r in $(seq 1 "$num_hot"); do
        run_gpu_execution "" "$q2" "Q2_addr_agg" "$dataset" "hot_$r" "$view_sql"
    done

    # Q3: COUNT DISTINCT
    local q3="SELECT date, COUNT(DISTINCT from_address) AS unique_senders, COUNT(DISTINCT to_address) AS unique_receivers FROM transfers GROUP BY 1 ORDER BY 1"
    run_gpu_execution "" "$q3" "Q3_count_distinct" "$dataset" "cold" "$view_sql"
    for r in $(seq 1 "$num_hot"); do
        run_gpu_execution "" "$q3" "Q3_count_distinct" "$dataset" "hot_$r" "$view_sql"
    done
}


# =============================================================================
# BENCHMARK EXECUTION
# =============================================================================

DB_OCT="$DATA_DIR/crypto_demo_2025_oct.duckdb"
DB_Q4="$DATA_DIR/crypto_demo_2025q4.duckdb"
NUM_HOT=3

echo "============================================"
echo " PHASE 1: 1-MONTH (Oct 2025, 72M transfers)"
echo "============================================"

# CPU
echo ""
echo ">> CPU baseline"
run_q01 "1mo" "$DB_OCT" "cpu" 0
run_query_set "1mo" "$DB_OCT" "cpu" "V1" 0
run_query_set "1mo" "$DB_OCT" "cpu" "V3" 0
# Run CPU 3 times for variance
for r in 2 3; do
    run_q01 "1mo" "$DB_OCT" "cpu" 0
    run_query_set "1mo" "$DB_OCT" "cpu" "V1" 0
    run_query_set "1mo" "$DB_OCT" "cpu" "V3" 0
done

# gpu_processing (cold + hot)
echo ""
echo ">> gpu_processing"
run_q01 "1mo" "$DB_OCT" "gpu_processing" "$NUM_HOT"
run_query_set "1mo" "$DB_OCT" "gpu_processing" "V1" "$NUM_HOT"
run_query_set "1mo" "$DB_OCT" "gpu_processing" "V3" "$NUM_HOT"

# gpu_execution (cold + hot) — uses DuckDB tables
echo ""
echo ">> gpu_execution"
run_q01 "1mo" "$DB_OCT" "gpu_execution" "$NUM_HOT"
run_query_set "1mo" "$DB_OCT" "gpu_execution" "V1" "$NUM_HOT"
run_query_set "1mo" "$DB_OCT" "gpu_execution" "V3" "$NUM_HOT"


echo ""
echo "============================================"
echo " PHASE 2: Q4 (Oct-Dec 2025, 292M transfers)"
echo "============================================"

# CPU (already have results but re-run for consistency)
echo ""
echo ">> CPU baseline (Q02-Q06 only)"
run_query_set "q4" "$DB_Q4" "cpu" "V1" 0
run_query_set "q4" "$DB_Q4" "cpu" "V3" 0
for r in 2 3; do
    run_query_set "q4" "$DB_Q4" "cpu" "V1" 0
    run_query_set "q4" "$DB_Q4" "cpu" "V3" 0
done

# gpu_processing Q02-Q06 (Q01 too big for 24GB)
echo ""
echo ">> gpu_processing (Q02-Q06)"
run_query_set "q4" "$DB_Q4" "gpu_processing" "V1" "$NUM_HOT"
run_query_set "q4" "$DB_Q4" "gpu_processing" "V3" "$NUM_HOT"

# gpu_execution Q02-Q06
echo ""
echo ">> gpu_execution (Q02-Q06)"
run_query_set "q4" "$DB_Q4" "gpu_execution" "V1" "$NUM_HOT"
run_query_set "q4" "$DB_Q4" "gpu_execution" "V3" "$NUM_HOT"

# gpu_execution Q01 from raw parquets (3 months for Q4)
OCT_GLOB="'$ETH_DIR/date=2025-10-*/*.parquet','$ETH_DIR/date=2025-11-*/*.parquet','$ETH_DIR/date=2025-12-*/*.parquet'"
run_q01_gpu_exec_parquet "q4" "$OCT_GLOB" "$NUM_HOT"


echo ""
echo "============================================"
echo " PHASE 3: SCALING (gpu_execution, 3mo + 6mo)"
echo "============================================"

# 3 months
GLOB_3MO="'$ETH_DIR/date=2025-10-*/*.parquet','$ETH_DIR/date=2025-11-*/*.parquet','$ETH_DIR/date=2025-12-*/*.parquet'"
echo ""
echo ">> 3 months (~146M rows)"
run_oom_scaling "3mo" "$GLOB_3MO" "$NUM_HOT"

# 6 months
GLOB_6MO="'$ETH_DIR/date=2025-07-*/*.parquet','$ETH_DIR/date=2025-08-*/*.parquet','$ETH_DIR/date=2025-09-*/*.parquet','$ETH_DIR/date=2025-10-*/*.parquet','$ETH_DIR/date=2025-11-*/*.parquet','$ETH_DIR/date=2025-12-*/*.parquet'"
echo ""
echo ">> 6 months (~292M rows) — CPU baseline for scaling"
# CPU on 6mo raw parquets (just Q1_agg for scaling comparison)
echo "--- 6mo / CPU scaling ---"
for r in 1 2 3; do
    local_q="SELECT date, token_address AS asset, SUM(CAST(value AS DOUBLE)) AS total_value, COUNT(*) AS tx_count FROM read_parquet([$GLOB_6MO], hive_partitioning=true) GROUP BY 1, 2 ORDER BY total_value DESC LIMIT 100"
    start=$(date +%s%N)
    output=$(echo ".timer on
$local_q;" | LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" "$DUCKDB_BIN" 2>&1) || true
    end=$(date +%s%N)
    elapsed_ms=$(( (end - start) / 1000000 ))
    real_time=$(echo "$output" | grep "Run Time" | grep -oP 'real \K[0-9.]+' | tail -1)
    timer_ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}' 2>/dev/null || echo "$elapsed_ms")
    echo "  Q1_agg | cpu | run_$r ${timer_ms}ms"
    echo "6mo,Q1_agg,cpu,run_$r,$timer_ms" >> "$RESULTS_CSV"
done

echo ""
echo ">> 6 months gpu_execution"
run_oom_scaling "6mo" "$GLOB_6MO" "$NUM_HOT"


echo ""
echo "============================================"
echo " RESULTS"
echo "============================================"
cat "$RESULTS_CSV"
echo ""
echo "Benchmark complete: $(date)"
echo "Results saved to: $RESULTS_CSV"
