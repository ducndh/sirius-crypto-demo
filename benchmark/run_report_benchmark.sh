#!/usr/bin/env bash
# Focused benchmark for the final report tables:
# Table 1: 3-month (Q4) — CPU, gpu_processing (cold+hot), gpu_execution (cold+hot) on V3 DICT
# Table 2: 6-month (H2) — CPU, gpu_execution (cold+hot) on V3 DICT
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
SIRIUS_DIR="${SIRIUS_DIR:-$HOME/sirius-dev}"
DATA_DIR="$DEMO_DIR/data"
DUCKDB="$SIRIUS_DIR/build/release/duckdb"
PIXI_LIB="$SIRIUS_DIR/.pixi/envs/cuda12/lib"
export LD_LIBRARY_PATH="$PIXI_LIB:${LD_LIBRARY_PATH:-}"

RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTFILE="$RESULTS_DIR/report_benchmark_${TIMESTAMP}.csv"
echo "dataset,query,engine,run,time_ms" > "$OUTFILE"

DB_PATH="$DATA_DIR/crypto_demo_2025q4.duckdb"

# Dict queries (V3) — same for 3mo and 6mo since tables have same schema
Q02="SELECT f.date, f.asset_id, COALESCE(src.entity, 'unknown') AS from_entity, COALESCE(dst.entity, 'unknown') AS to_entity, SUM(f.amount) AS amount FROM address_flows_daily_dict f LEFT JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id LEFT JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id GROUP BY 1,2,3,4 HAVING from_entity != 'unknown' AND to_entity != 'unknown' ORDER BY amount DESC LIMIT 20"
Q03="SELECT COALESCE(src.entity, 'unknown') AS from_entity, COALESCE(dst.entity, 'unknown') AS to_entity, SUM(f.amount) AS total_flow, COUNT(*) AS num_pairs FROM address_flows_daily_dict f JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id GROUP BY 1,2 ORDER BY total_flow DESC LIMIT 100"
Q04="SELECT f.date, SUM(f.amount) AS daily_flow FROM address_flows_daily_dict f JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id WHERE src.entity = 'metamask' AND dst.entity = 'uniswap' GROUP BY 1 ORDER BY f.date"
Q05="WITH outflows AS (SELECT src.entity, SUM(f.amount) AS total_outflow FROM address_flows_daily_dict f JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id GROUP BY 1), inflows AS (SELECT dst.entity, SUM(f.amount) AS total_inflow FROM address_flows_daily_dict f JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id GROUP BY 1) SELECT CASE WHEN o.entity IS NOT NULL THEN o.entity ELSE i.entity END AS entity, CASE WHEN i.total_inflow IS NOT NULL THEN i.total_inflow ELSE 0 END AS total_inflow, CASE WHEN o.total_outflow IS NOT NULL THEN o.total_outflow ELSE 0 END AS total_outflow FROM outflows o FULL OUTER JOIN inflows i ON o.entity = i.entity ORDER BY total_outflow DESC LIMIT 100"
Q06="SELECT COALESCE(src.category, 'unknown') AS from_category, COALESCE(dst.category, 'unknown') AS to_category, SUM(f.amount) AS total_flow FROM address_flows_daily_dict f JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id GROUP BY 1,2 ORDER BY total_flow DESC"

QUERIES=("$Q02" "$Q03" "$Q04" "$Q05" "$Q06")
QNAMES=("Q02" "Q03" "Q04" "Q05" "Q06")

# gpu_execution dict queries — double-escaped for CALL gpu_execution('...')
Q02_GE="SELECT f.date, f.asset_id, CASE WHEN src.entity IS NOT NULL THEN src.entity ELSE ''unknown'' END AS from_entity, CASE WHEN dst.entity IS NOT NULL THEN dst.entity ELSE ''unknown'' END AS to_entity, SUM(f.amount) AS amount FROM flows f LEFT JOIN entity_map src ON f.from_addr_id = src.addr_id LEFT JOIN entity_map dst ON f.to_addr_id = dst.addr_id GROUP BY 1,2,3,4 HAVING from_entity != ''unknown'' AND to_entity != ''unknown'' ORDER BY amount DESC LIMIT 20"
Q03_GE="SELECT CASE WHEN src.entity IS NOT NULL THEN src.entity ELSE ''unknown'' END AS from_entity, CASE WHEN dst.entity IS NOT NULL THEN dst.entity ELSE ''unknown'' END AS to_entity, SUM(f.amount) AS total_flow, COUNT(*) AS num_pairs FROM flows f JOIN entity_map src ON f.from_addr_id = src.addr_id JOIN entity_map dst ON f.to_addr_id = dst.addr_id GROUP BY 1,2 ORDER BY total_flow DESC LIMIT 100"
Q04_GE="SELECT f.date, SUM(f.amount) AS daily_flow FROM flows f JOIN entity_map src ON f.from_addr_id = src.addr_id JOIN entity_map dst ON f.to_addr_id = dst.addr_id WHERE src.entity = ''metamask'' AND dst.entity = ''uniswap'' GROUP BY 1 ORDER BY f.date"
Q05_GE="WITH outflows AS (SELECT src.entity, SUM(f.amount) AS total_outflow FROM flows f JOIN entity_map src ON f.from_addr_id = src.addr_id GROUP BY 1), inflows AS (SELECT dst.entity, SUM(f.amount) AS total_inflow FROM flows f JOIN entity_map dst ON f.to_addr_id = dst.addr_id GROUP BY 1) SELECT CASE WHEN o.entity IS NOT NULL THEN o.entity ELSE i.entity END AS entity, CASE WHEN i.total_inflow IS NOT NULL THEN i.total_inflow ELSE 0 END AS total_inflow, CASE WHEN o.total_outflow IS NOT NULL THEN o.total_outflow ELSE 0 END AS total_outflow FROM outflows o FULL OUTER JOIN inflows i ON o.entity = i.entity ORDER BY total_outflow DESC LIMIT 100"
Q06_GE="SELECT CASE WHEN src.category IS NOT NULL THEN src.category ELSE ''unknown'' END AS from_category, CASE WHEN dst.category IS NOT NULL THEN dst.category ELSE ''unknown'' END AS to_category, SUM(f.amount) AS total_flow FROM flows f JOIN entity_map src ON f.from_addr_id = src.addr_id JOIN entity_map dst ON f.to_addr_id = dst.addr_id GROUP BY 1,2 ORDER BY total_flow DESC"

QUERIES_GE=("$Q02_GE" "$Q03_GE" "$Q04_GE" "$Q05_GE" "$Q06_GE")

# ============================================================
# Helper: run gpu_processing query (single session, cold+hot)
# ============================================================
run_gpu_processing() {
    local dataset="$1" qi="$2"
    local escaped
    escaped=$(echo "${QUERIES[$qi]}" | sed "s/'/''/g")

    # gpu_buffer_init + 1 cold + 1 hot in single session
    local sql_cmds="CALL gpu_buffer_init('10 GB', '10 GB');"$'\n'
    sql_cmds+=".timer on"$'\n'
    sql_cmds+="CALL gpu_processing('$escaped');"$'\n'  # cold
    sql_cmds+="CALL gpu_processing('$escaped');"$'\n'  # hot

    local output
    output=$(echo "$sql_cmds" | "$DUCKDB" -unsigned "$DB_PATH" 2>&1) || true

    # Check for fallback — indicates buffer sizes too small or config conflict
    local fallbacks
    fallbacks=$(echo "$output" | grep -c "fallback to DuckDB" || true)
    if [[ $fallbacks -gt 0 ]]; then
        echo "  WARNING: ${QNAMES[$qi]} had $fallbacks fallback(s) to CPU!"
    fi

    local run_idx=0
    while IFS= read -r line; do
        if [[ "$line" =~ "Run Time" ]]; then
            local real_time ms
            real_time=$(echo "$line" | grep -oP 'real \K[0-9.]+')
            ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}')
            run_idx=$((run_idx + 1))
            if [[ $run_idx -eq 1 ]]; then
                echo "  ${QNAMES[$qi]} | gpu_processing | cold | ${ms}ms"
                echo "$dataset,${QNAMES[$qi]},gpu_processing,cold,$ms" >> "$OUTFILE"
            else
                echo "  ${QNAMES[$qi]} | gpu_processing | hot | ${ms}ms"
                echo "$dataset,${QNAMES[$qi]},gpu_processing,hot,$ms" >> "$OUTFILE"
            fi
        fi
    done <<< "$output"
}

# ============================================================
# Helper: run gpu_execution query (cold+hot, parquet views)
# ============================================================
run_gpu_execution() {
    local dataset="$1" qi="$2" flows_pq="$3" entity_pq="$4"

    local sql_cmds=""
    sql_cmds+="CREATE VIEW flows AS SELECT * FROM read_parquet('$flows_pq');"$'\n'
    sql_cmds+="CREATE VIEW entity_map AS SELECT * FROM read_parquet('$entity_pq');"$'\n'
    sql_cmds+="SET scan_cache_level = 'table_gpu';"$'\n'
    sql_cmds+=".timer on"$'\n'
    sql_cmds+="CALL gpu_execution('${QUERIES_GE[$qi]}');"$'\n'  # cold
    sql_cmds+="CALL gpu_execution('${QUERIES_GE[$qi]}');"$'\n'  # hot

    local output
    output=$(echo "$sql_cmds" | "$DUCKDB" -unsigned 2>&1) || true

    local run_idx=0
    while IFS= read -r line; do
        if [[ "$line" =~ "Run Time" ]]; then
            local real_time ms
            real_time=$(echo "$line" | grep -oP 'real \K[0-9.]+')
            ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}')
            run_idx=$((run_idx + 1))
            if [[ $run_idx -eq 1 ]]; then
                echo "  ${QNAMES[$qi]} | gpu_execution | cold | ${ms}ms"
                echo "$dataset,${QNAMES[$qi]},gpu_execution,cold,$ms" >> "$OUTFILE"
            else
                echo "  ${QNAMES[$qi]} | gpu_execution | hot | ${ms}ms"
                echo "$dataset,${QNAMES[$qi]},gpu_execution,hot,$ms" >> "$OUTFILE"
            fi
        fi
    done <<< "$output"
}

# ============================================================
# Helper: run CPU query (warm — run twice, report second)
# ============================================================
run_cpu() {
    local dataset="$1" qi="$2" date_filter="$3"

    local query="${QUERIES[$qi]}"

    # Run warmup + measured in single DuckDB session with .timer on
    local sql_cmds=""
    sql_cmds+="$query;"$'\n'         # warmup (no timer)
    sql_cmds+=".timer on"$'\n'
    sql_cmds+="$query;"$'\n'         # measured

    local output
    output=$(echo "$sql_cmds" | "$DUCKDB" -unsigned "$DB_PATH" 2>&1) || true

    while IFS= read -r line; do
        if [[ "$line" =~ "Run Time" ]]; then
            local real_time ms
            real_time=$(echo "$line" | grep -oP 'real \K[0-9.]+')
            ms=$(echo "$real_time" | awk '{printf "%.0f", $1 * 1000}')
            echo "  ${QNAMES[$qi]} | cpu | warm | ${ms}ms"
            echo "$dataset,${QNAMES[$qi]},cpu,warm,$ms" >> "$OUTFILE"
            break  # only report the first (measured) timer line
        fi
    done <<< "$output"
}

# ============================================================
# Table 1: 3-month (Q4 Oct-Dec 2025)
# ============================================================
echo ""
echo "====================================="
echo " Table 1: 3-month (Q4 2025) — V3 DICT"
echo "====================================="

# For 3-month, we need to filter to Q4 only since DB now has 6 months
# Modify queries to add date filter
Q02_3MO="SELECT f.date, f.asset_id, COALESCE(src.entity, 'unknown') AS from_entity, COALESCE(dst.entity, 'unknown') AS to_entity, SUM(f.amount) AS amount FROM address_flows_daily_dict f LEFT JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id LEFT JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id WHERE f.date >= '2025-10-01' GROUP BY 1,2,3,4 HAVING from_entity != 'unknown' AND to_entity != 'unknown' ORDER BY amount DESC LIMIT 20"
Q03_3MO="SELECT COALESCE(src.entity, 'unknown') AS from_entity, COALESCE(dst.entity, 'unknown') AS to_entity, SUM(f.amount) AS total_flow, COUNT(*) AS num_pairs FROM address_flows_daily_dict f JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id WHERE f.date >= '2025-10-01' GROUP BY 1,2 ORDER BY total_flow DESC LIMIT 100"
Q04_3MO="SELECT f.date, SUM(f.amount) AS daily_flow FROM address_flows_daily_dict f JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id WHERE src.entity = 'metamask' AND dst.entity = 'uniswap' AND f.date >= '2025-10-01' GROUP BY 1 ORDER BY f.date"
Q05_3MO="WITH outflows AS (SELECT src.entity, SUM(f.amount) AS total_outflow FROM address_flows_daily_dict f JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id WHERE f.date >= '2025-10-01' GROUP BY 1), inflows AS (SELECT dst.entity, SUM(f.amount) AS total_inflow FROM address_flows_daily_dict f JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id WHERE f.date >= '2025-10-01' GROUP BY 1) SELECT CASE WHEN o.entity IS NOT NULL THEN o.entity ELSE i.entity END AS entity, CASE WHEN i.total_inflow IS NOT NULL THEN i.total_inflow ELSE 0 END AS total_inflow, CASE WHEN o.total_outflow IS NOT NULL THEN o.total_outflow ELSE 0 END AS total_outflow FROM outflows o FULL OUTER JOIN inflows i ON o.entity = i.entity ORDER BY total_outflow DESC LIMIT 100"
Q06_3MO="SELECT COALESCE(src.category, 'unknown') AS from_category, COALESCE(dst.category, 'unknown') AS to_category, SUM(f.amount) AS total_flow FROM address_flows_daily_dict f JOIN entity_address_map_dict src ON f.from_addr_id = src.addr_id JOIN entity_address_map_dict dst ON f.to_addr_id = dst.addr_id WHERE f.date >= '2025-10-01' GROUP BY 1,2 ORDER BY total_flow DESC"

QUERIES_3MO=("$Q02_3MO" "$Q03_3MO" "$Q04_3MO" "$Q05_3MO" "$Q06_3MO")

# gpu_execution 3-month queries
Q02_GE_3MO="SELECT f.date, f.asset_id, CASE WHEN src.entity IS NOT NULL THEN src.entity ELSE ''unknown'' END AS from_entity, CASE WHEN dst.entity IS NOT NULL THEN dst.entity ELSE ''unknown'' END AS to_entity, SUM(f.amount) AS amount FROM flows f LEFT JOIN entity_map src ON f.from_addr_id = src.addr_id LEFT JOIN entity_map dst ON f.to_addr_id = dst.addr_id WHERE f.date >= ''2025-10-01'' GROUP BY 1,2,3,4 HAVING from_entity != ''unknown'' AND to_entity != ''unknown'' ORDER BY amount DESC LIMIT 20"
Q03_GE_3MO="SELECT CASE WHEN src.entity IS NOT NULL THEN src.entity ELSE ''unknown'' END AS from_entity, CASE WHEN dst.entity IS NOT NULL THEN dst.entity ELSE ''unknown'' END AS to_entity, SUM(f.amount) AS total_flow, COUNT(*) AS num_pairs FROM flows f JOIN entity_map src ON f.from_addr_id = src.addr_id JOIN entity_map dst ON f.to_addr_id = dst.addr_id WHERE f.date >= ''2025-10-01'' GROUP BY 1,2 ORDER BY total_flow DESC LIMIT 100"
Q04_GE_3MO="SELECT f.date, SUM(f.amount) AS daily_flow FROM flows f JOIN entity_map src ON f.from_addr_id = src.addr_id JOIN entity_map dst ON f.to_addr_id = dst.addr_id WHERE src.entity = ''metamask'' AND dst.entity = ''uniswap'' AND f.date >= ''2025-10-01'' GROUP BY 1 ORDER BY f.date"
Q05_GE_3MO="WITH outflows AS (SELECT src.entity, SUM(f.amount) AS total_outflow FROM flows f JOIN entity_map src ON f.from_addr_id = src.addr_id WHERE f.date >= ''2025-10-01'' GROUP BY 1), inflows AS (SELECT dst.entity, SUM(f.amount) AS total_inflow FROM flows f JOIN entity_map dst ON f.to_addr_id = dst.addr_id WHERE f.date >= ''2025-10-01'' GROUP BY 1) SELECT CASE WHEN o.entity IS NOT NULL THEN o.entity ELSE i.entity END AS entity, CASE WHEN i.total_inflow IS NOT NULL THEN i.total_inflow ELSE 0 END AS total_inflow, CASE WHEN o.total_outflow IS NOT NULL THEN o.total_outflow ELSE 0 END AS total_outflow FROM outflows o FULL OUTER JOIN inflows i ON o.entity = i.entity ORDER BY total_outflow DESC LIMIT 100"
Q06_GE_3MO="SELECT CASE WHEN src.category IS NOT NULL THEN src.category ELSE ''unknown'' END AS from_category, CASE WHEN dst.category IS NOT NULL THEN dst.category ELSE ''unknown'' END AS to_category, SUM(f.amount) AS total_flow FROM flows f JOIN entity_map src ON f.from_addr_id = src.addr_id JOIN entity_map dst ON f.to_addr_id = dst.addr_id WHERE f.date >= ''2025-10-01'' GROUP BY 1,2 ORDER BY total_flow DESC"

QUERIES_GE_3MO=("$Q02_GE_3MO" "$Q03_GE_3MO" "$Q04_GE_3MO" "$Q05_GE_3MO" "$Q06_GE_3MO")

echo ""
echo "--- 3mo CPU ---"
for qi in "${!QNAMES[@]}"; do
    QUERIES=("${QUERIES_3MO[@]}")
    run_cpu "3mo" "$qi" ""
done

echo ""
echo "--- 3mo gpu_processing ---"
# Must move ~/.sirius/sirius.cfg aside — unsetting SIRIUS_CONFIG_FILE is not enough,
# Sirius falls back to ~/.sirius/sirius.cfg which steals GPU memory from gpu_buffer_init
unset SIRIUS_CONFIG_FILE 2>/dev/null || true
if [[ -f "$HOME/.sirius/sirius.cfg" ]]; then
    mv "$HOME/.sirius/sirius.cfg" "$HOME/.sirius/sirius.cfg.bak"
    RESTORE_CFG=true
else
    RESTORE_CFG=false
fi
for qi in "${!QNAMES[@]}"; do
    QUERIES=("${QUERIES_3MO[@]}")
    run_gpu_processing "3mo" "$qi"
done

# Restore sirius.cfg if we moved it
if [[ "$RESTORE_CFG" == "true" && -f "$HOME/.sirius/sirius.cfg.bak" ]]; then
    mv "$HOME/.sirius/sirius.cfg.bak" "$HOME/.sirius/sirius.cfg"
fi

echo ""
echo "--- 3mo gpu_execution ---"
CONFIG_PATH="$SCRIPT_DIR/sirius_rtx6000.cfg"
export SIRIUS_CONFIG_FILE="$CONFIG_PATH"
export SIRIUS_LOG_LEVEL=info
for qi in "${!QNAMES[@]}"; do
    QUERIES_GE=("${QUERIES_GE_3MO[@]}")
    run_gpu_execution "3mo" "$qi" "$DATA_DIR/bench_flows_dict_3mo.parquet" "$DATA_DIR/bench_entity_dict.parquet"
done

# ============================================================
# Table 2: 6-month (H2 Jul-Dec 2025)
# ============================================================
echo ""
echo "====================================="
echo " Table 2: 6-month (H2 2025) — V3 DICT"
echo "====================================="

echo ""
echo "--- 6mo CPU ---"
for qi in "${!QNAMES[@]}"; do
    # Use full table (no date filter) = 6 months
    run_cpu "6mo" "$qi" ""
done

echo ""
echo "--- 6mo gpu_execution ---"
export SIRIUS_CONFIG_FILE="$CONFIG_PATH"
for qi in "${!QNAMES[@]}"; do
    run_gpu_execution "6mo" "$qi" "$DATA_DIR/bench_flows_dict.parquet" "$DATA_DIR/bench_entity_dict_6mo.parquet"
done

echo ""
echo "====================================="
echo " Done"
echo "====================================="
echo "Results: $OUTFILE"
cat "$OUTFILE"
