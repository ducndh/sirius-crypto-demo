#!/bin/bash
# gpu_execution benchmark for 1mo and Q4 datasets
# Goal: understand out-of-core performance and compare cold/hot with gpu_processing
#
# gpu_execution uses ~/.sirius/sirius.cfg for configuration (no gpu_buffer_init).
# Cache level "table_gpu" keeps decoded tables in GPU memory for hot runs,
# but only holds one cached result — so we run 2 tries (cold + 1 hot).

set -uo pipefail

SIRIUS_DIR="/home/cc/sirius-dev"
DUCKDB="$SIRIUS_DIR/build/release/duckdb"
export LD_LIBRARY_PATH="$SIRIUS_DIR/.pixi/envs/cuda12/lib:${LD_LIBRARY_PATH:-}"

# gpu_execution cache only holds 1 previous result, so 3rd run crashes
TRIES=2

RESULTS_DIR="benchmark/results"
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTFILE="$RESULTS_DIR/gpu_execution_${TIMESTAMP}.csv"
echo "dataset,query,engine,run,time_s" > "$OUTFILE"

declare -a DB_LABELS=("1mo" "q4")
declare -a DB_PATHS=(
    "/home/cc/sirius-crypto-demo/data/crypto_demo_2025_oct.duckdb"
    "/home/cc/sirius-crypto-demo/data/crypto_demo_2025q4.duckdb"
)

QUERIES=(
    "SELECT e1.entity AS from_entity, e2.entity AS to_entity, f.date, f.asset, SUM(f.amount) AS total_amount, SUM(f.tx_count) AS total_tx FROM address_flows_daily_dict f LEFT JOIN entity_address_map_dict e1 ON f.from_addr_id = e1.addr_id LEFT JOIN entity_address_map_dict e2 ON f.to_addr_id = e2.addr_id GROUP BY 1, 2, 3, 4 ORDER BY total_amount DESC LIMIT 100"
    "SELECT e1.entity AS from_entity, e2.entity AS to_entity, SUM(f.amount) AS total_amount, COUNT(*) AS flow_count FROM address_flows_daily_dict f JOIN entity_address_map_dict e1 ON f.from_addr_id = e1.addr_id JOIN entity_address_map_dict e2 ON f.to_addr_id = e2.addr_id GROUP BY 1, 2 ORDER BY total_amount DESC LIMIT 100"
    "SELECT f.date, SUM(f.amount) AS daily_amount, SUM(f.tx_count) AS daily_tx FROM address_flows_daily_dict f JOIN entity_address_map_dict e1 ON f.from_addr_id = e1.addr_id JOIN entity_address_map_dict e2 ON f.to_addr_id = e2.addr_id WHERE e1.entity = 'binance' AND e2.entity = 'coinbase' GROUP BY 1 ORDER BY 1"
    "SELECT i.entity, i.inflow, o.outflow FROM (SELECT e.entity, SUM(f.amount) AS inflow FROM address_flows_daily_dict f JOIN entity_address_map_dict e ON f.to_addr_id = e.addr_id GROUP BY 1) i JOIN (SELECT e.entity, SUM(f.amount) AS outflow FROM address_flows_daily_dict f JOIN entity_address_map_dict e ON f.from_addr_id = e.addr_id GROUP BY 1) o ON i.entity = o.entity ORDER BY inflow DESC LIMIT 50"
    "SELECT e1.category AS from_cat, e2.category AS to_cat, SUM(f.amount) AS total_amount, COUNT(*) AS flow_count FROM address_flows_daily_dict f JOIN entity_address_map_dict e1 ON f.from_addr_id = e1.addr_id JOIN entity_address_map_dict e2 ON f.to_addr_id = e2.addr_id GROUP BY 1, 2 ORDER BY total_amount DESC"
)

for di in 0 1; do
    label="${DB_LABELS[$di]}"
    dbpath="${DB_PATHS[$di]}"
    echo "=== Dataset: $label ($dbpath) ==="

    for qi in "${!QUERIES[@]}"; do
        qnum=$((qi + 1))
        query="${QUERIES[$qi]}"
        echo "--- Q${qnum} on $label (gpu_execution) ---"

        # Drop OS caches
        sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

        # Build CLI params: no gpu_buffer_init needed for gpu_execution
        cli_params=()
        cli_params+=("-c" ".timer on")

        for i in $(seq 1 $TRIES); do
            cli_params+=("-c" "call gpu_execution(\"${query}\");")
        done

        # Run and capture output
        output=$("$DUCKDB" "$dbpath" "${cli_params[@]}" 2>&1) || true

        # Parse "Run Time (s): real X.XXX" lines
        mapfile -t times < <(echo "$output" | grep -oP 'Run Time \(s\): real\s+\K[0-9.]+')

        for ti in $(seq 0 $((TRIES-1))); do
            if [ $ti -eq 0 ]; then
                run_label="cold"
            else
                run_label="hot"
            fi
            if [ "$ti" -lt "${#times[@]}" ]; then
                echo "$label,Q${qnum},gpu_execution,$run_label,${times[$ti]}" | tee -a "$OUTFILE"
            else
                echo "$label,Q${qnum},gpu_execution,${run_label},error" | tee -a "$OUTFILE"
            fi
        done
    done
done

echo ""
echo "Results saved to: $OUTFILE"
cat "$OUTFILE"
