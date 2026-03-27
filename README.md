# Sirius Crypto Demo

GPU-accelerated blockchain counterparty flow analytics using
[Sirius](https://github.com/sirius-db/sirius), benchmarked against the
TRM Labs workload (entity flow rollup, multi-hop Sankey trace).

## Repository Layout

```
queries/trm_pipeline/       SQL queries (Q01-Q06 + multi-hop Sankey)
  setup_views.sql            Pluggable data-source views (swap DuckDB/Parquet/Iceberg)
  q_sankey_multihop.sql      3-hop entity flow trace (main demo query)
  q01-q06                    TRM counterparty flow pipeline queries

scripts/
  download_eth_transfers.py  Download Ethereum token transfers from AWS S3
  download_mbal.py           Download MBAL 10M address labels from Kaggle
  prepare_tables.py          Build DuckDB database with dictionary-encoded tables
  generate_2yr_data.py       Build full 2-year database (Jan 2024 – Mar 2026)
  scaling_validated.sh       Run GPU vs CPU scaling benchmark across data slices
  bench_trm.py               Quick Sankey benchmark (warm-cache timing)
  machines.env               Machine-specific config template

benchmark/
  bench_multihop.sh          Multi-hop trace benchmark script
  sirius.cfg                 Reference Sirius GPU config (A100, 40GB)
  results/                   Validated benchmark results (RTX 6000 + A100)

docs/                        Technical analysis and reports
```

## Quick Start

### Prerequisites

- **Sirius** built from source (see [build instructions](https://github.com/sirius-db/sirius))
- NVIDIA GPU with CUDA 12.8+ driver (565+)
- AWS CLI (`pip install awscli` — no credentials needed)
- Kaggle CLI (`pip install kaggle` + [API token](https://www.kaggle.com/docs/api))

> **Build tip:** Set `-DCUDA_ARCHS=XX` to your GPU's compute capability for
> faster builds (e.g., `86` for A5000, `80` for A100, `75` for RTX 6000).

### 1. Download data

```bash
# Ethereum token transfers — 3 months (~25 GB) for a quick test
python scripts/download_eth_transfers.py --start-date 2024-10-01 --end-date 2024-12-31

# MBAL entity address labels (~1 GB)
python scripts/download_mbal.py
```

### 2. Build the database

```bash
# 3-month database (~2 GB)
python scripts/prepare_tables.py --year 2024
```

This creates `data/crypto_demo_2024.duckdb` with:
- `token_transfers` — raw Ethereum transfers
- `address_flows_daily` / `address_flows_daily_dict` — daily aggregated flows
- `entity_address_map` / `entity_address_map_dict` — MBAL labels
- `address_dictionary` — address VARCHAR → INT32 mapping
- Parquet exports in `data/bench_*.parquet` for `gpu_execution` path

For the full **2-year database** (770M rows, ~17 GB), see [docs/data_guide.md](docs/data_guide.md).

### 3. Run queries

```bash
SIRIUS=~/sirius-dev/build/release/duckdb
PIXI_ENV=~/sirius-dev/.pixi/envs/cuda12
DB=data/crypto_demo_2024.duckdb

# Set up LD_LIBRARY_PATH for CUDA
export LD_LIBRARY_PATH=$PIXI_ENV/lib:$LD_LIBRARY_PATH

# CPU baseline
$SIRIUS $DB -c ".read queries/trm_pipeline/setup_views.sql" \
            -c ".timer on" \
            -c ".read queries/trm_pipeline/q_sankey_multihop.sql"

# GPU (gpu_processing) — move ~/.sirius/sirius.cfg aside first
mv ~/.sirius/sirius.cfg ~/.sirius/sirius.cfg.bak 2>/dev/null
$SIRIUS $DB -c ".read queries/trm_pipeline/setup_views.sql" \
            -c "CALL gpu_buffer_init('10 GB', '10 GB');" \
            -c ".timer on" \
            -c "CALL gpu_processing('.read queries/trm_pipeline/q_sankey_multihop.sql');"
mv ~/.sirius/sirius.cfg.bak ~/.sirius/sirius.cfg 2>/dev/null
```

### 4. Run the scaling benchmark

```bash
# Runs GPU vs CPU across 3-month to 12-month slices, validates correctness
bash scripts/scaling_validated.sh
```

Results are written to `benchmark/results/`.

## Benchmark Results

### RTX 6000 (24 GB, Turing) — gpu_processing, warm cache

Scaling across data sizes (Sankey + Q02-Q06, correctness-validated):

| Data | Rows | GPU range | CPU range | Speedup |
|------|-----:|----------:|----------:|--------:|
| 3 mo | 63M | 5-33 ms | 27-197 ms | 3.8-6.0x |
| 6 mo | 126M | 7-38 ms | 39-190 ms | 4.8-6.2x |
| 9 mo | 190M | 9-44 ms | 52-197 ms | 4.4-6.4x |
| 12 mo | 255M | 11-48 ms | 48-228 ms | 3.5-7.1x |

### A100 (40 GB, Ampere) — gpu_processing, warm cache

Scaling across data sizes (3-hop Sankey + Q02-Q06):

| Data | Rows | GPU range | CPU range | Speedup |
|------|-----:|----------:|----------:|--------:|
| 3 mo | 63M | 5-24 ms | 20-93 ms | 2.9-5.4x |
| 6 mo | 126M | 6-24 ms | 25-118 ms | 3.0-6.0x |
| 12 mo | 255M | 8-31 ms | 32-170 ms | 3.1-5.5x |
| 21 mo | 503M | 11-42 ms | 45-166 ms | 3.6-7.8x |
| 24 mo | 619M | 12-46 ms | 42-173 ms | 3.4-3.8x |

GPU memory limit: ~620M rows on 40 GB A100 (working pool exhaustion).

## GPU Execution Notes

- **gpu_processing**: Data cached in GPU VRAM. Fastest warm performance. Use
  when dataset fits in GPU memory.
- **gpu_execution**: Out-of-core via cucascade (GPU→host→disk tiering). Use for
  datasets larger than VRAM. Requires `~/.sirius/sirius.cfg`.
- Dictionary encoding (V3 DICT) is required for GPU — VARCHAR join keys and
  GROUP BY columns cause fallback or extreme slowdown.
- `COALESCE` must be rewritten as `CASE WHEN ... IS NOT NULL THEN ... ELSE ... END`.
- `DISTINCT` must be rewritten as `GROUP BY` with dummy aggregate.
- `UNION ALL` requires the `feature/union-all-gpu-processing` branch.

## Data Sources

| Dataset | Source | Notes |
|---------|--------|-------|
| Ethereum token transfers | [AWS Public Blockchain](https://registry.opendata.aws/aws-public-blockchain/) | ~300 MB/day, Parquet, no credentials |
| MBAL address labels | [Kaggle](https://www.kaggle.com/datasets/yidongchaintoolai/mbal-10m-crypto-address-label-dataset) | 10M addresses, requires Kaggle token |
