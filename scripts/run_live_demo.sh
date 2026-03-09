#!/bin/bash
# Live demo launcher for the TRM Labs pitch.
# Loads all tables into GPU VRAM, then drops into interactive Sirius shell.
# Presenter can run queries from queries/ or let audience type ad-hoc SQL.
#
# Usage:
#   bash scripts/run_live_demo.sh
#
# Environment:
#   SIRIUS_BIN       path to the Sirius duckdb binary
#   SIRIUS_PIXI_ENV  path to the pixi cuda12 env (for LD_LIBRARY_PATH)
#   GPU_CACHE_SIZE   e.g. "20 GB" (RTX 6000) or "70 GB" (H100)
#   GPU_PROC_SIZE    e.g. "15 GB" (RTX 6000) or "60 GB" (H100)
#   DATA_DIR         path to directory containing *.parquet files
set -euo pipefail

SIRIUS_BIN="${SIRIUS_BIN:-/home/cc/sirius-asof/build/release/duckdb}"
SIRIUS_PIXI_ENV="${SIRIUS_PIXI_ENV:-/home/cc/sirius-asof/.pixi/envs/cuda12}"
GPU_CACHE="${GPU_CACHE_SIZE:-20 GB}"
GPU_PROC="${GPU_PROC_SIZE:-15 GB}"
DATA_DIR="${DATA_DIR:-$(dirname "$0")/../data}"
DATA_DIR="$(realpath "$DATA_DIR")"
QUERIES_DIR="$(realpath "$(dirname "$0")/../queries")"

# Sanity checks
if [[ ! -f "$SIRIUS_BIN" ]]; then
  echo "ERROR: Sirius binary not found: $SIRIUS_BIN"
  echo "Build with: cd /home/cc/sirius-asof && ~/.pixi/bin/pixi run -e cuda12 make release"
  echo "Or set SIRIUS_BIN env var."
  exit 1
fi

for f in eth_transactions.parquet prices.parquet token_transfers.parquet; do
  if [[ ! -f "$DATA_DIR/$f" ]]; then
    echo "ERROR: $DATA_DIR/$f not found."
    echo "Run: bash scripts/download_eth_data.sh dev && python scripts/prepare_demo_data.py"
    exit 1
  fi
done

# Build the init SQL
INIT_SQL=$(mktemp /tmp/sirius_demo_XXXXXX.sql)
trap "rm -f $INIT_SQL" EXIT

cat > "$INIT_SQL" << EOF
-- Initialize GPU buffer
CALL gpu_buffer_init('${GPU_CACHE}', '${GPU_PROC}');

-- Load all tables into DuckDB (will be transferred to GPU on first query)
CREATE TABLE eth_transactions AS SELECT * FROM '${DATA_DIR}/eth_transactions.parquet';
CREATE TABLE prices           AS SELECT * FROM '${DATA_DIR}/prices.parquet';
CREATE TABLE token_transfers  AS SELECT * FROM '${DATA_DIR}/token_transfers.parquet';

-- Pre-warm: pull all data into GPU VRAM
CALL gpu_processing('SELECT COUNT(*) FROM eth_transactions');

-- Enable timing for all subsequent queries
.timer on

-- ─────────────────────────────────────────────────────────────────
-- DEMO READY. Run queries with:
--   CALL gpu_processing('your SQL here');
--
-- Example queries (see queries/ directory for full list):
--   CALL gpu_processing('SELECT COUNT(*) FROM eth_transactions WHERE block_timestamp >= ''2024-01-15''');
--   CALL gpu_processing('SELECT from_address, COUNT(*) FROM eth_transactions GROUP BY 1 ORDER BY 2 DESC LIMIT 10');
-- ─────────────────────────────────────────────────────────────────
EOF

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║          Sirius GPU Analytics — Live Demo                ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Hardware : $(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1 || echo 'GPU')"
echo "║  GPU cache: ${GPU_CACHE} / ${GPU_PROC}"
echo "║  Data dir : ${DATA_DIR}"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "Demo query files:  ls ${QUERIES_DIR}/"
echo "Run a query:       CALL gpu_processing('SELECT ...');"
echo ""
echo "Loading data into GPU VRAM (one-time cost)..."
echo ""

export LD_LIBRARY_PATH="${SIRIUS_PIXI_ENV}/lib:${LD_LIBRARY_PATH:-}"
exec "$SIRIUS_BIN" -unsigned -init "$INIT_SQL"
