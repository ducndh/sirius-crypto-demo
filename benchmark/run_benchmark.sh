#!/usr/bin/env bash
# =============================================================================
# Sirius Crypto Demo — Full Benchmark Suite
# Runs TRM pipeline queries across: DuckDB CPU, gpu_processing, gpu_execution
#
# Usage:
#   ./benchmark/run_benchmark.sh [--data-dir DIR] [--sirius-dir DIR] [--gpu-mem GB]
#
# Targets: RTX 6000 (24GB), L40S (48GB), GH200 (96GB HBM3)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
SIRIUS_DIR="${SIRIUS_DIR:-$HOME/sirius-dev}"
DATA_DIR="${DATA_DIR:-$DEMO_DIR/data}"
GPU_MEM_GB="${GPU_MEM_GB:-0}"  # 0 = auto-detect
PIXI_ENV="${PIXI_ENV:-cuda12}"
RESULTS_DIR="$SCRIPT_DIR/results"
WARMUP_RUNS=1
BENCH_RUNS=3

# --- Parse arguments ---
while [[ $# -gt 0 ]]; do
    case $1 in
        --data-dir)   DATA_DIR="$2"; shift 2 ;;
        --sirius-dir) SIRIUS_DIR="$2"; shift 2 ;;
        --gpu-mem)    GPU_MEM_GB="$2"; shift 2 ;;
        --pixi-env)   PIXI_ENV="$2"; shift 2 ;;
        --runs)       BENCH_RUNS="$2"; shift 2 ;;
        *)            echo "Unknown arg: $1"; exit 1 ;;
    esac
done

DUCKDB_BIN="$SIRIUS_DIR/build/release/duckdb"
PIXI_LIB="$SIRIUS_DIR/.pixi/envs/$PIXI_ENV/lib"

if [[ ! -x "$DUCKDB_BIN" ]]; then
    echo "ERROR: Sirius binary not found at $DUCKDB_BIN"
    echo "Build with: cd $SIRIUS_DIR && ~/.pixi/bin/pixi run -e $PIXI_ENV make release"
    exit 1
fi

# --- Auto-detect GPU ---
detect_gpu() {
    local gpu_name mem_mb
    gpu_name=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1 | xargs)
    mem_mb=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null | head -1 | xargs)

    if [[ -z "$gpu_name" ]]; then
        echo "WARNING: No GPU detected, skipping GPU benchmarks"
        GPU_NAME="none"
        GPU_MEM_MB=0
        return
    fi

    GPU_NAME="$gpu_name"
    GPU_MEM_MB="$mem_mb"

    if [[ "$GPU_MEM_GB" == "0" ]]; then
        GPU_MEM_GB=$((mem_mb / 1024))
    fi

    echo "GPU: $GPU_NAME ($GPU_MEM_MB MB / $GPU_MEM_GB GB usable)"
    echo "CUDA: $(nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -1)"
}

# --- Generate Sirius config file ---
generate_config() {
    local mem_gb=$1
    local config_path="$RESULTS_DIR/sirius_auto.cfg"

    # Use 80% of GPU memory, reserve rest for cudf overhead
    local gpu_bytes=$(( mem_gb * 1024 * 1024 * 1024 * 80 / 100 ))
    local host_bytes=$(( 32 * 1024 * 1024 * 1024 ))  # 32GB host

    cat > "$config_path" <<EOCFG
sirius = {
    topology = { num_gpus = 1; };
    memory = {
        gpu = {
            usage_limit_fraction = 0.8;
            reservation_limit_fraction = 1.0;
        };
        host = {
            capacity_bytes = ${host_bytes};
            usage_limit_fraction = 0.8;
            reservation_limit_fraction = 1.0;
        };
    };
    operator_params = {
        max_build_hash_table_bytes = 500000000;
        hash_partition_bytes = 100000000;
        concat_batch_bytes = 100000000;
        ungrouped_agg_partial_batch_bytes = 100000000;
        grouped_agg_partial_batch_bytes = 100000000;
        order_by_partition_bytes = 100000000;
        top_n_partition_bytes = 100000000;
    };
};
EOCFG
    echo "$config_path"
}

# --- Run a query and capture timing ---
run_timed() {
    local engine="$1"  # cpu, gpu_processing, gpu_execution
    local query_name="$2"
    local sql="$3"
    local run_num="$4"

    local start end elapsed_ms
    start=$(date +%s%N)

    case "$engine" in
        cpu)
            echo "$sql" | LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" \
                "$DUCKDB_BIN" "$DB_PATH" 2>/dev/null >/dev/null
            ;;
        gpu_processing)
            local escaped_sql
            escaped_sql=$(echo "$sql" | sed "s/'/''/g")
            echo "CALL gpu_processing('$escaped_sql');" | \
                LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" \
                "$DUCKDB_BIN" "$DB_PATH" 2>/dev/null >/dev/null
            ;;
        gpu_execution)
            local escaped_sql
            escaped_sql=$(echo "$sql" | sed "s/'/''/g")
            echo "CALL gpu_execution('$escaped_sql');" | \
                LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" \
                "$DUCKDB_BIN" "$DB_PATH" 2>/dev/null >/dev/null
            ;;
    esac

    local exit_code=$?
    end=$(date +%s%N)
    elapsed_ms=$(( (end - start) / 1000000 ))

    if [[ $exit_code -ne 0 ]]; then
        echo "  $query_name | $engine | run $run_num | FAILED"
        echo "$query_name,$engine,$run_num,FAILED" >> "$RESULTS_CSV"
    else
        echo "  $query_name | $engine | run $run_num | ${elapsed_ms}ms"
        echo "$query_name,$engine,$run_num,$elapsed_ms" >> "$RESULTS_CSV"
    fi
}

# --- Main ---
detect_gpu
mkdir -p "$RESULTS_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_CSV="$RESULTS_DIR/benchmark_${GPU_NAME// /_}_${TIMESTAMP}.csv"
echo "query,engine,run,time_ms" > "$RESULTS_CSV"

echo ""
echo "=== Benchmark Configuration ==="
echo "Sirius binary: $DUCKDB_BIN"
echo "Data directory: $DATA_DIR"
echo "Results: $RESULTS_CSV"
echo "Warmup runs: $WARMUP_RUNS, Bench runs: $BENCH_RUNS"
echo ""

# --- Check data availability ---
DB_PATH="$DATA_DIR/crypto_demo_2025q4.duckdb"
if [[ ! -f "$DB_PATH" ]]; then
    DB_PATH="$DATA_DIR/crypto_demo_2025.duckdb"
fi
if [[ ! -f "$DB_PATH" ]]; then
    DB_PATH="$DATA_DIR/crypto_demo_2026.duckdb"
fi
if [[ ! -f "$DB_PATH" ]]; then
    echo "ERROR: No database found. Run: python scripts/prepare_tables.py"
    exit 1
fi
echo "Database: $DB_PATH ($(du -h "$DB_PATH" | cut -f1))"

# --- Check parquet exports ---
PARQUET_DICT_FLOWS="$DATA_DIR/bench_flows_dict.parquet"
PARQUET_DICT_ENTITY="$DATA_DIR/bench_entity_dict.parquet"
PARQUET_VARCHAR_FLOWS="$DATA_DIR/bench_flows_varchar.parquet"
PARQUET_VARCHAR_ENTITY="$DATA_DIR/bench_entity_varchar.parquet"

# Export parquets if missing (for gpu_execution)
export_parquets() {
    echo "Exporting parquet files for gpu_execution..."

    LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" "$DUCKDB_BIN" "$DB_PATH" -c "
        -- V1 VARCHAR: flows + entity map
        COPY (
            SELECT date, asset, from_address, to_address, amount, tx_count
            FROM address_flows_daily
        ) TO '$PARQUET_VARCHAR_FLOWS' (FORMAT PARQUET, COMPRESSION ZSTD);

        COPY (
            SELECT address, entity, category, attribution_source
            FROM entity_address_map
        ) TO '$PARQUET_VARCHAR_ENTITY' (FORMAT PARQUET, COMPRESSION ZSTD);

        -- V3 DICT: dictionary-encoded flows + entity map
        COPY (
            SELECT
                f.date, f.asset, f.amount, f.tx_count,
                d_from.addr_id AS from_addr_id,
                d_to.addr_id AS to_addr_id
            FROM address_flows_daily f
            LEFT JOIN (
                SELECT address, ROW_NUMBER() OVER (ORDER BY address) AS addr_id
                FROM (SELECT DISTINCT address FROM entity_address_map)
            ) d_from ON f.from_address = d_from.address
            LEFT JOIN (
                SELECT address, ROW_NUMBER() OVER (ORDER BY address) AS addr_id
                FROM (SELECT DISTINCT address FROM entity_address_map)
            ) d_to ON f.to_address = d_to.address
        ) TO '$PARQUET_DICT_FLOWS' (FORMAT PARQUET, COMPRESSION ZSTD);

        COPY (
            SELECT
                entity, category, attribution_source,
                ROW_NUMBER() OVER (ORDER BY address) AS addr_id
            FROM (
                SELECT DISTINCT address, entity, category, attribution_source
                FROM entity_address_map
            )
        ) TO '$PARQUET_DICT_ENTITY' (FORMAT PARQUET, COMPRESSION ZSTD);
    " 2>&1

    echo "  VARCHAR flows: $(du -h "$PARQUET_VARCHAR_FLOWS" | cut -f1)"
    echo "  VARCHAR entity: $(du -h "$PARQUET_VARCHAR_ENTITY" | cut -f1)"
    echo "  DICT flows: $(du -h "$PARQUET_DICT_FLOWS" | cut -f1)"
    echo "  DICT entity: $(du -h "$PARQUET_DICT_ENTITY" | cut -f1)"
}

if [[ ! -f "$PARQUET_DICT_FLOWS" ]] || [[ ! -f "$PARQUET_VARCHAR_FLOWS" ]]; then
    export_parquets
fi

# =============================================================================
# TRM Pipeline Queries
# =============================================================================
# All queries operate on address_flows_daily (pre-aggregated, TRM Stage 2)
# V1: VARCHAR join keys (from_address/to_address)
# V3: INT32 dictionary join keys (from_addr_id/to_addr_id)
# =============================================================================

# --- Q01: Stage 1 aggregation (same for V1 and V3 — runs on raw token_transfers) ---
Q01="SELECT date, token_address AS asset, '0x' || SUBSTR(from_address, 27) AS from_addr, '0x' || SUBSTR(to_address, 27) AS to_addr, SUM(value) AS amount, COUNT(*) AS tx_count FROM token_transfers GROUP BY 1, 2, 3, 4 ORDER BY amount DESC LIMIT 1000"

# --- Queries for DuckDB tables (V1 VARCHAR) ---
Q02_V1="SELECT f.date, f.asset, COALESCE(src.entity, 'unknown') AS from_entity, COALESCE(dst.entity, 'unknown') AS to_entity, SUM(f.amount) AS amount FROM address_flows_daily f LEFT JOIN entity_address_map src ON f.from_address = src.address LEFT JOIN entity_address_map dst ON f.to_address = dst.address GROUP BY 1,2,3,4 HAVING from_entity != 'unknown' AND to_entity != 'unknown' ORDER BY amount DESC LIMIT 20"

Q03_V1="SELECT COALESCE(src.entity, 'unknown') AS from_entity, COALESCE(dst.entity, 'unknown') AS to_entity, SUM(f.amount) AS total_flow, COUNT(*) AS num_pairs FROM address_flows_daily f JOIN entity_address_map src ON f.from_address = src.address JOIN entity_address_map dst ON f.to_address = dst.address GROUP BY 1,2 ORDER BY total_flow DESC LIMIT 100"

Q04_V1="SELECT f.date, SUM(f.amount) AS daily_flow FROM address_flows_daily f JOIN entity_address_map src ON f.from_address = src.address JOIN entity_address_map dst ON f.to_address = dst.address WHERE src.entity = 'metamask' AND dst.entity = 'uniswap' GROUP BY 1 ORDER BY f.date"

Q05_V1="WITH outflows AS (SELECT src.entity, SUM(f.amount) AS total_outflow FROM address_flows_daily f JOIN entity_address_map src ON f.from_address = src.address GROUP BY 1), inflows AS (SELECT dst.entity, SUM(f.amount) AS total_inflow FROM address_flows_daily f JOIN entity_address_map dst ON f.to_address = dst.address GROUP BY 1) SELECT COALESCE(o.entity, i.entity) AS entity, COALESCE(i.total_inflow,0) AS total_inflow, COALESCE(o.total_outflow,0) AS total_outflow FROM outflows o FULL OUTER JOIN inflows i ON o.entity = i.entity ORDER BY total_outflow DESC LIMIT 100"

Q06_V1="SELECT COALESCE(src.category, 'unknown') AS from_category, COALESCE(dst.category, 'unknown') AS to_category, SUM(f.amount) AS total_flow FROM address_flows_daily f JOIN entity_address_map src ON f.from_address = src.address JOIN entity_address_map dst ON f.to_address = dst.address GROUP BY 1,2 ORDER BY total_flow DESC"

# --- Queries for DuckDB tables (V3 DICT) ---
# These require dict tables to be created first
Q02_V3="SELECT f.date, f.asset, COALESCE(src.entity, 'unknown') AS from_entity, COALESCE(dst.entity, 'unknown') AS to_entity, SUM(f.amount) AS amount FROM address_flows_daily_dict f LEFT JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id LEFT JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id GROUP BY 1,2,3,4 HAVING from_entity != 'unknown' AND to_entity != 'unknown' ORDER BY amount DESC LIMIT 20"

Q03_V3="SELECT COALESCE(src.entity, 'unknown') AS from_entity, COALESCE(dst.entity, 'unknown') AS to_entity, SUM(f.amount) AS total_flow, COUNT(*) AS num_pairs FROM address_flows_daily_dict f JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id GROUP BY 1,2 ORDER BY total_flow DESC LIMIT 100"

Q04_V3="SELECT f.date, SUM(f.amount) AS daily_flow FROM address_flows_daily_dict f JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id WHERE src.entity = 'metamask' AND dst.entity = 'uniswap' GROUP BY 1 ORDER BY f.date"

Q05_V3="WITH outflows AS (SELECT src.entity, SUM(f.amount) AS total_outflow FROM address_flows_daily_dict f JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id GROUP BY 1), inflows AS (SELECT dst.entity, SUM(f.amount) AS total_inflow FROM address_flows_daily_dict f JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id GROUP BY 1) SELECT COALESCE(o.entity, i.entity) AS entity, COALESCE(i.total_inflow,0) AS total_inflow, COALESCE(o.total_outflow,0) AS total_outflow FROM outflows o FULL OUTER JOIN inflows i ON o.entity = i.entity ORDER BY total_outflow DESC LIMIT 100"

Q06_V3="SELECT COALESCE(src.category, 'unknown') AS from_category, COALESCE(dst.category, 'unknown') AS to_category, SUM(f.amount) AS total_flow FROM address_flows_daily_dict f JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id GROUP BY 1,2 ORDER BY total_flow DESC"

# =============================================================================
# Run benchmarks
# =============================================================================

run_single_query() {
    local engine="$1"
    local query_name="$2"
    local sql="$3"

    # Warmup
    for w in $(seq 1 $WARMUP_RUNS); do
        run_timed "$engine" "$query_name" "$sql" "warmup_$w" 2>/dev/null || true
    done
    # Bench
    for r in $(seq 1 $BENCH_RUNS); do
        run_timed "$engine" "$query_name" "$sql" "$r"
    done
}

run_query_set() {
    local variant="$1"   # V1 or V3
    local engine="$2"    # cpu, gpu_processing, gpu_execution

    local q02 q03 q04 q05 q06
    if [[ "$variant" == "V1" ]]; then
        q02="$Q02_V1"; q03="$Q03_V1"; q04="$Q04_V1"; q05="$Q05_V1"; q06="$Q06_V1"
    else
        q02="$Q02_V3"; q03="$Q03_V3"; q04="$Q04_V3"; q05="$Q05_V3"; q06="$Q06_V3"
    fi

    echo ""
    echo "--- $variant / $engine ---"

    local queries=("$q02" "$q03" "$q04" "$q05" "$q06")
    local names=("Q02_${variant}" "Q03_${variant}" "Q04_${variant}" "Q05_${variant}" "Q06_${variant}")

    for i in "${!queries[@]}"; do
        run_single_query "$engine" "${names[$i]}" "${queries[$i]}"
    done
}

# --- 1. CPU Baseline ---
echo ""
echo "============================="
echo " CPU Baseline (DuckDB)"
echo "============================="
# Q01: Stage 1 aggregation (raw token_transfers, no V1/V3 variant)
run_single_query "cpu" "Q01" "$Q01"
# Q02-Q06: Stage 2 entity analytics
run_query_set "V1" "cpu"
run_query_set "V3" "cpu"

# --- 2. gpu_processing ---
if [[ "$GPU_NAME" != "none" ]]; then
    echo ""
    echo "============================="
    echo " gpu_processing (old path)"
    echo "============================="

    # gpu_processing needs gpu_buffer_init and DuckDB tables
    GPU_CACHE_GB=$(( GPU_MEM_GB * 40 / 100 ))
    GPU_PROC_GB=$(( GPU_MEM_GB * 40 / 100 ))

    # Create init SQL
    GPU_INIT_SQL="CALL gpu_buffer_init('${GPU_CACHE_GB} GB', '${GPU_PROC_GB} GB');"

    # For gpu_processing, we wrap queries differently — need a persistent session
    run_gpu_processing_set() {
        local variant="$1"
        local q02 q03 q04 q05 q06
        if [[ "$variant" == "V1" ]]; then
            q02="$Q02_V1"; q03="$Q03_V1"; q04="$Q04_V1"; q05="$Q05_V1"; q06="$Q06_V1"
        else
            q02="$Q02_V3"; q03="$Q03_V3"; q04="$Q04_V3"; q05="$Q05_V3"; q06="$Q06_V3"
        fi

        local queries=("$q02" "$q03" "$q04" "$q05" "$q06")
        local names=("Q02_${variant}" "Q03_${variant}" "Q04_${variant}" "Q05_${variant}" "Q06_${variant}")

        echo ""
        echo "--- $variant / gpu_processing ---"

        for i in "${!queries[@]}"; do
            local escaped
            escaped=$(echo "${queries[$i]}" | sed "s/'/''/g")

            # Run warmup + bench in one session for caching
            local sql_cmds="$GPU_INIT_SQL"$'\n'
            sql_cmds+=".timer on"$'\n'

            # Warmup
            for w in $(seq 1 $WARMUP_RUNS); do
                sql_cmds+="CALL gpu_processing('$escaped');"$'\n'
            done
            # Bench runs
            for r in $(seq 1 $BENCH_RUNS); do
                sql_cmds+="CALL gpu_processing('$escaped');"$'\n'
            done

            local output
            output=$(echo "$sql_cmds" | LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" \
                "$DUCKDB_BIN" "$DB_PATH" 2>&1) || true

            # Parse timings from .timer on output
            local run_idx=0
            while IFS= read -r line; do
                if [[ "$line" =~ "Run Time" ]]; then
                    local real_time
                    real_time=$(echo "$line" | grep -oP 'real \K[0-9.]+')
                    local ms
                    ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}')
                    run_idx=$((run_idx + 1))
                    if [[ $run_idx -le $WARMUP_RUNS ]]; then
                        echo "  ${names[$i]} | gpu_processing | warmup_$run_idx | ${ms}ms"
                        echo "${names[$i]},gpu_processing,warmup_$run_idx,$ms" >> "$RESULTS_CSV"
                    else
                        local bench_num=$((run_idx - WARMUP_RUNS))
                        echo "  ${names[$i]} | gpu_processing | run $bench_num | ${ms}ms"
                        echo "${names[$i]},gpu_processing,$bench_num,$ms" >> "$RESULTS_CSV"
                    fi
                fi
            done <<< "$output"
        done
    }

    # Q01: Stage 1 (runs on raw token_transfers)
    run_gpu_processing_single() {
        local query_name="$1"
        local query="$2"
        local escaped
        escaped=$(echo "$query" | sed "s/'/''/g")

        local sql_cmds="$GPU_INIT_SQL"$'\n'
        sql_cmds+=".timer on"$'\n'
        for w in $(seq 1 $WARMUP_RUNS); do
            sql_cmds+="CALL gpu_processing('$escaped');"$'\n'
        done
        for r in $(seq 1 $BENCH_RUNS); do
            sql_cmds+="CALL gpu_processing('$escaped');"$'\n'
        done

        local output
        output=$(echo "$sql_cmds" | LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" \
            "$DUCKDB_BIN" "$DB_PATH" 2>&1) || true

        local run_idx=0
        while IFS= read -r line; do
            if [[ "$line" =~ "Run Time" ]]; then
                local real_time ms
                real_time=$(echo "$line" | grep -oP 'real \K[0-9.]+')
                ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}')
                run_idx=$((run_idx + 1))
                if [[ $run_idx -le $WARMUP_RUNS ]]; then
                    echo "  $query_name | gpu_processing | warmup_$run_idx | ${ms}ms"
                    echo "$query_name,gpu_processing,warmup_$run_idx,$ms" >> "$RESULTS_CSV"
                else
                    local bench_num=$((run_idx - WARMUP_RUNS))
                    echo "  $query_name | gpu_processing | run $bench_num | ${ms}ms"
                    echo "$query_name,gpu_processing,$bench_num,$ms" >> "$RESULTS_CSV"
                fi
            fi
        done <<< "$output"
    }

    echo ""
    echo "--- Q01 / gpu_processing ---"
    run_gpu_processing_single "Q01" "$Q01"

    run_gpu_processing_set "V1"
    run_gpu_processing_set "V3"
fi

# --- 3. gpu_execution ---
if [[ "$GPU_NAME" != "none" ]]; then
    echo ""
    echo "============================="
    echo " gpu_execution (new path)"
    echo "============================="

    CONFIG_PATH="$SCRIPT_DIR/sirius_rtx6000.cfg"
    if [[ ! -f "$CONFIG_PATH" ]]; then
        CONFIG_PATH=$(generate_config "$GPU_MEM_GB")
    fi
    export SIRIUS_CONFIG_FILE="$CONFIG_PATH"
    export SIRIUS_LOG_LEVEL=info

    run_gpu_execution_set() {
        local variant="$1"
        local flows_pq entity_pq
        if [[ "$variant" == "V1" ]]; then
            flows_pq="$PARQUET_VARCHAR_FLOWS"
            entity_pq="$PARQUET_VARCHAR_ENTITY"
        else
            flows_pq="$PARQUET_DICT_FLOWS"
            entity_pq="$PARQUET_DICT_ENTITY"
        fi

        if [[ ! -f "$flows_pq" ]] || [[ ! -f "$entity_pq" ]]; then
            echo "  SKIP $variant — parquet files not found"
            return
        fi

        # Build queries that reference parquet views
        local q02 q03 q04 q05 q06
        if [[ "$variant" == "V1" ]]; then
            q02="SELECT f.date, f.asset, COALESCE(src.entity, ''unknown'') AS from_entity, COALESCE(dst.entity, ''unknown'') AS to_entity, SUM(f.amount) AS amount FROM flows f LEFT JOIN entity_map src ON f.from_address = src.address LEFT JOIN entity_map dst ON f.to_address = dst.address GROUP BY 1,2,3,4 HAVING from_entity != ''unknown'' AND to_entity != ''unknown'' ORDER BY amount DESC LIMIT 20"
            q03="SELECT COALESCE(src.entity, ''unknown'') AS from_entity, COALESCE(dst.entity, ''unknown'') AS to_entity, SUM(f.amount) AS total_flow, COUNT(*) AS num_pairs FROM flows f JOIN entity_map src ON f.from_address = src.address JOIN entity_map dst ON f.to_address = dst.address GROUP BY 1,2 ORDER BY total_flow DESC LIMIT 100"
            q04="SELECT f.date, SUM(f.amount) AS daily_flow FROM flows f JOIN entity_map src ON f.from_address = src.address JOIN entity_map dst ON f.to_address = dst.address WHERE src.entity = ''metamask'' AND dst.entity = ''uniswap'' GROUP BY 1 ORDER BY f.date"
            q05="WITH outflows AS (SELECT src.entity, SUM(f.amount) AS total_outflow FROM flows f JOIN entity_map src ON f.from_address = src.address GROUP BY 1), inflows AS (SELECT dst.entity, SUM(f.amount) AS total_inflow FROM flows f JOIN entity_map dst ON f.to_address = dst.address GROUP BY 1) SELECT COALESCE(o.entity, i.entity) AS entity, COALESCE(i.total_inflow,0) AS total_inflow, COALESCE(o.total_outflow,0) AS total_outflow FROM outflows o FULL OUTER JOIN inflows i ON o.entity = i.entity ORDER BY total_outflow DESC LIMIT 100"
            q06="SELECT COALESCE(src.category, ''unknown'') AS from_category, COALESCE(dst.category, ''unknown'') AS to_category, SUM(f.amount) AS total_flow FROM flows f JOIN entity_map src ON f.from_address = src.address JOIN entity_map dst ON f.to_address = dst.address GROUP BY 1,2 ORDER BY total_flow DESC"
        else
            q02="SELECT f.date, f.asset, COALESCE(src.entity, ''unknown'') AS from_entity, COALESCE(dst.entity, ''unknown'') AS to_entity, SUM(f.amount) AS amount FROM flows f LEFT JOIN entity_map src ON f.from_addr_id = src.addr_id LEFT JOIN entity_map dst ON f.to_addr_id = dst.addr_id GROUP BY 1,2,3,4 HAVING from_entity != ''unknown'' AND to_entity != ''unknown'' ORDER BY amount DESC LIMIT 20"
            q03="SELECT COALESCE(src.entity, ''unknown'') AS from_entity, COALESCE(dst.entity, ''unknown'') AS to_entity, SUM(f.amount) AS total_flow, COUNT(*) AS num_pairs FROM flows f JOIN entity_map src ON f.from_addr_id = src.addr_id JOIN entity_map dst ON f.to_addr_id = dst.addr_id GROUP BY 1,2 ORDER BY total_flow DESC LIMIT 100"
            q04="SELECT f.date, SUM(f.amount) AS daily_flow FROM flows f JOIN entity_map src ON f.from_addr_id = src.addr_id JOIN entity_map dst ON f.to_addr_id = dst.addr_id WHERE src.entity = ''metamask'' AND dst.entity = ''uniswap'' GROUP BY 1 ORDER BY f.date"
            q05="WITH outflows AS (SELECT src.entity, SUM(f.amount) AS total_outflow FROM flows f JOIN entity_map src ON f.from_addr_id = src.addr_id GROUP BY 1), inflows AS (SELECT dst.entity, SUM(f.amount) AS total_inflow FROM flows f JOIN entity_map dst ON f.to_addr_id = dst.addr_id GROUP BY 1) SELECT COALESCE(o.entity, i.entity) AS entity, COALESCE(i.total_inflow,0) AS total_inflow, COALESCE(o.total_outflow,0) AS total_outflow FROM outflows o FULL OUTER JOIN inflows i ON o.entity = i.entity ORDER BY total_outflow DESC LIMIT 100"
            q06="SELECT COALESCE(src.category, ''unknown'') AS from_category, COALESCE(dst.category, ''unknown'') AS to_category, SUM(f.amount) AS total_flow FROM flows f JOIN entity_map src ON f.from_addr_id = src.addr_id JOIN entity_map dst ON f.to_addr_id = dst.addr_id GROUP BY 1,2 ORDER BY total_flow DESC"
        fi

        local queries=("$q02" "$q03" "$q04" "$q05" "$q06")
        local names=("Q02_${variant}" "Q03_${variant}" "Q04_${variant}" "Q05_${variant}" "Q06_${variant}")

        echo ""
        echo "--- $variant / gpu_execution ---"

        for i in "${!queries[@]}"; do
            # Build full SQL with view creation + warmup + bench
            local sql_cmds=""
            sql_cmds+="CREATE VIEW flows AS SELECT * FROM read_parquet('$flows_pq');"$'\n'
            sql_cmds+="CREATE VIEW entity_map AS SELECT * FROM read_parquet('$entity_pq');"$'\n'
            sql_cmds+=".timer on"$'\n'

            for w in $(seq 1 $WARMUP_RUNS); do
                sql_cmds+="CALL gpu_execution('${queries[$i]}');"$'\n'
            done
            for r in $(seq 1 $BENCH_RUNS); do
                sql_cmds+="CALL gpu_execution('${queries[$i]}');"$'\n'
            done

            local output
            output=$(echo "$sql_cmds" | LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" \
                "$DUCKDB_BIN" 2>&1) || true

            local run_idx=0
            while IFS= read -r line; do
                if [[ "$line" =~ "Run Time" ]]; then
                    local real_time
                    real_time=$(echo "$line" | grep -oP 'real \K[0-9.]+')
                    local ms
                    ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}')
                    run_idx=$((run_idx + 1))
                    if [[ $run_idx -le $WARMUP_RUNS ]]; then
                        echo "  ${names[$i]} | gpu_execution | warmup_$run_idx | ${ms}ms"
                        echo "${names[$i]},gpu_execution,warmup_$run_idx,$ms" >> "$RESULTS_CSV"
                    else
                        local bench_num=$((run_idx - WARMUP_RUNS))
                        echo "  ${names[$i]} | gpu_execution | run $bench_num | ${ms}ms"
                        echo "${names[$i]},gpu_execution,$bench_num,$ms" >> "$RESULTS_CSV"
                    fi
                fi
            done <<< "$output"
        done
    }

    # Q01: Stage 1 on gpu_execution (reads raw eth_transfers parquets)
    ETH_PARQUET_GLOB="$DATA_DIR/eth_transfers/date=2025-1{0,1,2}-*/*.parquet"
    run_gpu_execution_q01() {
        if ! ls $ETH_PARQUET_GLOB &>/dev/null && [[ ! -f "$DATA_DIR/bench_token_transfers.parquet" ]]; then
            echo "  Q01 | gpu_execution | SKIP (no token_transfers parquet)"
            return
        fi

        echo ""
        echo "--- Q01 / gpu_execution ---"
        local q01_escaped
        q01_escaped="SELECT date, token_address AS asset, ''0x'' || SUBSTR(from_address, 27) AS from_addr, ''0x'' || SUBSTR(to_address, 27) AS to_addr, SUM(value) AS amount, COUNT(*) AS tx_count FROM transfers GROUP BY 1, 2, 3, 4 ORDER BY amount DESC LIMIT 1000"

        # Use raw eth_transfers parquets if available, else bench_token_transfers
        local parquet_src
        if ls $ETH_PARQUET_GLOB &>/dev/null; then
            parquet_src="read_parquet(['$DATA_DIR/eth_transfers/date=2025-10-*/*.parquet','$DATA_DIR/eth_transfers/date=2025-11-*/*.parquet','$DATA_DIR/eth_transfers/date=2025-12-*/*.parquet'], hive_partitioning=true)"
        else
            parquet_src="read_parquet('$DATA_DIR/bench_token_transfers.parquet')"
        fi

        local sql_cmds=""
        sql_cmds+="CREATE VIEW transfers AS SELECT * FROM $parquet_src;"$'\n'
        sql_cmds+=".timer on"$'\n'
        for w in $(seq 1 $WARMUP_RUNS); do
            sql_cmds+="CALL gpu_execution('$q01_escaped');"$'\n'
        done
        for r in $(seq 1 $BENCH_RUNS); do
            sql_cmds+="CALL gpu_execution('$q01_escaped');"$'\n'
        done

        local output
        output=$(echo "$sql_cmds" | LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}" \
            "$DUCKDB_BIN" 2>&1) || true

        local run_idx=0
        while IFS= read -r line; do
            if [[ "$line" =~ "Run Time" ]]; then
                local real_time ms
                real_time=$(echo "$line" | grep -oP 'real \K[0-9.]+')
                ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}')
                run_idx=$((run_idx + 1))
                if [[ $run_idx -le $WARMUP_RUNS ]]; then
                    echo "  Q01 | gpu_execution | warmup_$run_idx | ${ms}ms"
                    echo "Q01,gpu_execution,warmup_$run_idx,$ms" >> "$RESULTS_CSV"
                else
                    local bench_num=$((run_idx - WARMUP_RUNS))
                    echo "  Q01 | gpu_execution | run $bench_num | ${ms}ms"
                    echo "Q01,gpu_execution,$bench_num,$ms" >> "$RESULTS_CSV"
                fi
            fi
        done <<< "$output"
    }
    run_gpu_execution_q01

    run_gpu_execution_set "V1"
    run_gpu_execution_set "V3"
fi

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "============================="
echo " Results Summary"
echo "============================="
echo ""
echo "Raw CSV: $RESULTS_CSV"
echo ""

# Print summary table
echo "| Query | Engine | Median (ms) |"
echo "|-------|--------|-------------|"

# Group by query+engine, take median of non-warmup runs
awk -F',' 'NR>1 && $3 !~ /warmup/ && $4 != "FAILED" {
    key = $1 "|" $2
    vals[key] = vals[key] " " $4
    count[key]++
}
END {
    for (key in vals) {
        n = split(vals[key], arr, " ")
        # Simple median: sort and take middle
        for (i=1; i<=n; i++) for (j=i+1; j<=n; j++) if (arr[i]+0 > arr[j]+0) { t=arr[i]; arr[i]=arr[j]; arr[j]=t }
        mid = int((n+1)/2)
        split(key, parts, "|")
        printf "| %s | %s | %s |\n", parts[1], parts[2], arr[mid]
    }
}' "$RESULTS_CSV" | sort

echo ""
echo "Benchmark complete: $(date)"
