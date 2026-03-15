#!/bin/bash
# Crypto TRM pipeline benchmark for Sirius (GPU-accelerated DuckDB extension)
#
# Benchmarks the TRM Labs counterparty flow pipeline:
#   address_flows_daily → double JOIN entity_address_map → entity_flows_daily
#
# Usage: ./benchmark.sh
# Prerequisites: NVIDIA GPU with CUDA driver, internet access
#
# Data: Ethereum token transfer flows (dict-encoded integer join keys)
#       Pre-built parquet files hosted on S3.

source dependencies.sh

# Verify pixi is available
if ! command -v pixi &> /dev/null; then
  echo "Error: pixi not found. Check dependencies.sh output."
  exit 1
fi

# ---------------------------------------------------------------------------
# 1. Build Sirius
# ---------------------------------------------------------------------------
rm -rf sirius
git clone --recurse-submodules https://github.com/sirius-db/sirius.git
cd sirius

set -e

pixi install
export LIBCUDF_ENV_PREFIX="$(pwd)/.pixi/envs/default"
pixi run make -j"$(nproc)"

# Make the build artifacts available
eval "$(pixi shell-hook)"
export PATH="$(pwd)/build/release:$PATH"
cd ..

set +e

# ---------------------------------------------------------------------------
# 2. Install Sirius config (required for gpu_execution)
# ---------------------------------------------------------------------------
mkdir -p ~/.sirius
cp sirius.cfg ~/.sirius/sirius.cfg
echo "Installed sirius.cfg to ~/.sirius/sirius.cfg"

# ---------------------------------------------------------------------------
# 3. Download data
# ---------------------------------------------------------------------------
mkdir -p data

# Pre-built parquet files for the benchmark
# TODO: host these on a public bucket. For now, build from raw eth_transfers.
# wget --continue 'https://example.com/bench_flows_dict.parquet' -O data/bench_flows_dict.parquet
# wget --continue 'https://example.com/bench_entity_dict.parquet' -O data/bench_entity_dict.parquet
# wget --continue 'https://example.com/bench_flows_varchar.parquet' -O data/bench_flows_varchar.parquet
# wget --continue 'https://example.com/bench_entity_varchar.parquet' -O data/bench_entity_varchar.parquet

echo "NOTE: Data download URLs not yet configured."
echo "      Copy parquet files to data/ manually or use scripts/prepare_tables.py"

# ---------------------------------------------------------------------------
# 4. Load data into DuckDB
# ---------------------------------------------------------------------------
echo -n "Load time: "
command time -f '%e' duckdb crypto.db -f create.sql -f load.sql

# ---------------------------------------------------------------------------
# 5. Run benchmark
# ---------------------------------------------------------------------------
./run.sh 2>&1 | tee log.txt

echo -n "Data size: "
wc -c crypto.db

# ---------------------------------------------------------------------------
# 6. Format results
# ---------------------------------------------------------------------------
# Output format: [cold, hot1, hot2] per query (same as ClickBench)
cat log.txt | \
  grep -P '^\d|Killed|Segmentation|^Run Time \(s\): real' | \
  sed -r -e 's/^.(Killed|Segmentation).$/null\nnull\nnull/; s/^Run Time \(s\): real\s*([0-9.]+).*$/\1/' | \
  awk '{
    buf[i++] = $1
    if (i == 4) {
      printf "[%s,%s,%s],\n", buf[1], buf[2], buf[3]
      i = 0
    }
  }'
