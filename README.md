# sirius-crypto-demo

End-to-end benchmark for [Sirius](https://github.com/sirius-db/sirius) GPU analytics
on blockchain data — targeting the TRM Labs counterparty flow workload.

## Repository Structure

```
├── data/
│   ├── eth_transfers/           # Ethereum token transfers (AWS, Parquet)
│   ├── mbal/                    # MBAL 10M address labels (Kaggle)
│   ├── bench_*.parquet          # Exported benchmark parquets (auto-generated)
│   └── crypto_demo_*.duckdb    # Built DuckDB database (auto-generated)
├── queries/trm_pipeline/        # TRM entity flow queries (Q01-Q06)
├── benchmark/
│   ├── run_benchmark.sh         # Main benchmark script (all engines)
│   └── results/                 # CSV outputs per GPU
├── scripts/
│   ├── download_eth_transfers.py
│   ├── download_mbal.py
│   └── prepare_tables.py
└── docs/                        # Analysis documents
```

## The TRM Pipeline

The core workload is **blockchain counterparty flow analytics**:

1. **Stage 1** (batch): Aggregate raw token transfers to daily address flows
2. **Stage 2** (interactive): Join with entity map + roll up to entity-level flows

We benchmark **both stages** — Q01 covers Stage 1 (heavy aggregation on raw data),
Q02-Q06 cover Stage 2 (join-heavy entity analytics).

### Queries Benchmarked

| Query | Stage | Description | Pattern |
|-------|-------|-------------|---------|
| Q01 | 1 | Daily address flow aggregation | GROUP BY on raw token_transfers |
| Q02 | 2 | Entity flow rollup | 2x LEFT JOIN + GROUP BY + ORDER + LIMIT |
| Q03 | 2 | Top counterparty pairs | 2x INNER JOIN + GROUP BY + ORDER |
| Q04 | 2 | Entity timeseries | 2x INNER JOIN + WHERE + GROUP BY + ORDER |
| Q05 | 2 | Entity inflow/outflow | CTE + FULL OUTER JOIN |
| Q06 | 2 | Category flow matrix | 2x INNER JOIN + GROUP BY + ORDER |

### Address Matching Strategies

| Version | Join Key | Pros | Cons |
|---------|----------|------|------|
| **V1 VARCHAR** | 42-char hex address | Exact, no prep | Slow string hashing |
| **V3 DICT** | INT32 dictionary ID | Fast integer join | Requires dictionary build step |

V2 (INT64 hash) was dropped — hash collisions confirmed across engines and no speed advantage over V3.

## Quick Start

### 1. Prerequisites

- Sirius built at `~/sirius-dev` (DuckDB 1.4.4 extension)
- NVIDIA GPU with driver 565+ (CUDA 12.8+)
- AWS CLI (`pip install awscli`)
- ~100GB disk space for data

### 2. Download Data

```bash
# MBAL entity labels (requires Kaggle token)
python scripts/download_mbal.py

# Ethereum token transfers (2025, ~365 days, ~100GB raw)
python scripts/download_eth_transfers.py --start-date 2025-01-01 --end-date 2025-12-31
```

### 3. Build Database

```bash
python scripts/prepare_tables.py --year 2025
```

This creates:
- `data/crypto_demo_2025.duckdb` — DuckDB database with all tables
- `data/bench_*.parquet` — Parquet exports for `gpu_execution` path
- Dictionary-encoded tables for V3 DICT benchmarks

### 4. Run Benchmarks

```bash
# Full benchmark (CPU + gpu_processing + gpu_execution)
bash benchmark/run_benchmark.sh

# With custom settings
bash benchmark/run_benchmark.sh \
    --sirius-dir ~/sirius-dev \
    --gpu-mem 24 \
    --runs 5
```

Results are saved to `benchmark/results/benchmark_<GPU>_<timestamp>.csv`.

## Running on Different GPUs

The benchmark auto-detects GPU and generates an appropriate config file.

### RTX 6000 (24GB VRAM)

```bash
bash benchmark/run_benchmark.sh --gpu-mem 24
```

### L40S (48GB VRAM)

```bash
bash benchmark/run_benchmark.sh --gpu-mem 48
```

### GH200 (96GB HBM3)

```bash
# GH200 has unified memory — can use more aggressively
bash benchmark/run_benchmark.sh --gpu-mem 96
```

### Benchmark Script Options

| Flag | Default | Description |
|------|---------|-------------|
| `--sirius-dir` | `~/sirius-dev` | Path to Sirius build directory |
| `--data-dir` | `./data` | Path to data directory |
| `--gpu-mem` | auto-detect | GPU memory in GB |
| `--pixi-env` | `cuda12` | Pixi environment name |
| `--runs` | `3` | Number of benchmark iterations |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SIRIUS_DIR` | `~/sirius-dev` | Override Sirius directory |
| `DATA_DIR` | `./data` | Override data directory |
| `GPU_MEM_GB` | auto | Override GPU memory detection |

## Architecture Notes

### Two GPU Execution Paths

| Path | Entry Point | Data Source | Caching | Out-of-Core |
|------|-------------|-------------|---------|-------------|
| `gpu_processing` | `CALL gpu_processing(...)` | DuckDB tables | GPU buffer manager | No |
| `gpu_execution` | `CALL gpu_execution(...)` | Parquet views | cucascade tiered memory | Yes |

**`gpu_processing`** is the stable path — fastest warm performance (data cached in GPU memory).
Use for datasets that fit in VRAM.

**`gpu_execution`** is the new path — supports out-of-core via cucascade (GPU → host → disk tiering).
Use for datasets larger than VRAM. Warm performance is ~8x slower than `gpu_processing` on same data,
but handles arbitrarily large datasets.

### CUDA Driver Requirements

The `gpu_execution` path uses `cudaMemcpyBatchAsync` (CUDA 12.8+). Requires:
- **NVIDIA driver 565+** (CUDA 12.8 support)
- pixi `cuda12` env provides toolkit 12.9 for compilation

Older drivers (560, CUDA 12.6) will fail with `cudaErrorCallRequiresNewerDriver`.

## Data Sources

| Dataset | Source | Size | Description |
|---------|--------|------|-------------|
| Ethereum token transfers | [AWS Public Blockchain](https://registry.opendata.aws/aws-public-blockchain/) | ~300MB/day | Parquet, partitioned by date |
| MBAL address labels | [Kaggle](https://www.kaggle.com/datasets/yidongchaintoolai/mbal-10m-crypto-address-label-dataset) | ~1GB | 10M labeled crypto addresses |

## Benchmark Results (RTX 6000, 1-week slice)

| Engine | Q02 V3 DICT (warm) |
|--------|-------------------|
| DuckDB CPU | 273ms |
| Sirius `gpu_processing` | **19ms** (14x faster) |
| Sirius `gpu_execution` | **150ms** (1.8x faster) |

Full results across all queries and data sizes generated by `run_benchmark.sh`.
