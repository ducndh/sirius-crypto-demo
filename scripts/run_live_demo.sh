#!/bin/bash
# Live demo launcher for the TRM Labs pitch.
# Loads all tables into GPU VRAM, builds transaction graph, then drops into
# interactive Sirius shell.  Presenter can run queries from queries/ or let
# audience type ad-hoc SQL.
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
    echo "Run: python scripts/download_eth_data.py --scale dev && python scripts/download_prices.py"
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

-- Build transaction graph edge table (for graph analytics queries)
CREATE TABLE tx_edges AS
  SELECT from_address AS src, to_address AS dst
  FROM eth_transactions
  WHERE value > 0;

-- Pre-warm: pull all data into GPU VRAM
CALL gpu_processing('SELECT COUNT(*) FROM eth_transactions');

-- Enable timing for all subsequent queries
.timer on

-- ─────────────────────────────────────────────────────────────────
-- DEMO READY
--
-- Analytics queries (GPU-accelerated):
--   CALL gpu_processing('SELECT COUNT(*) FROM eth_transactions WHERE block_timestamp >= ''2024-01-15''');
--   CALL gpu_processing('SELECT from_address, COUNT(*) FROM eth_transactions GROUP BY 1 ORDER BY 2 DESC LIMIT 10');
--
-- ASOF JOIN (unique to Sirius — no other GPU DB supports this):
--   CALL gpu_processing('SELECT COUNT(*), SUM(t.value/1e18 * p.price_usd) FROM eth_transactions t ASOF JOIN prices p ON t.block_timestamp >= p.ts');
--
-- Graph analytics (cuGraph via CUDA IPC):
--   SELECT * FROM gpu_graph_pagerank('tx_edges', 'src', 'dst') ORDER BY pagerank DESC LIMIT 20;
--   SELECT component, COUNT(*) AS size FROM gpu_graph_wcc('tx_edges', 'src', 'dst') GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
--   SELECT * FROM gpu_graph_bfs('tx_edges', 'src', 'dst', '0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88', 3) ORDER BY distance LIMIT 50;
-- ─────────────────────────────────────────────────────────────────
EOF

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║          Sirius GPU Analytics — Live Demo                    ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
echo "║  Hardware : $(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1 || echo 'GPU')"
echo "║  GPU cache: ${GPU_CACHE} / proc: ${GPU_PROC}"
echo "║  Data dir : ${DATA_DIR}"
echo "╠═══════════════════════════════════════════════════════════════╣"
echo "║  ANALYTICS:  CALL gpu_processing('SELECT ...');              ║"
echo "║  GRAPH:      SELECT * FROM gpu_graph_pagerank('tx_edges',    ║"
echo "║              'src', 'dst') ORDER BY pagerank DESC LIMIT 20;  ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "Query files:  ls ${QUERIES_DIR}/"
echo ""
echo "Loading data into GPU VRAM + building transaction graph..."
echo ""

export LD_LIBRARY_PATH="${SIRIUS_PIXI_ENV}/lib:${LD_LIBRARY_PATH:-}"
exec "$SIRIUS_BIN" -unsigned -init "$INIT_SQL"
