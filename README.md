# sirius-crypto-demo

End-to-end benchmark and demo harness for [Sirius](https://github.com/sirius-db/sirius)
GPU analytics on blockchain data — targeting the TRM Labs counterparty flow workload.

## Repository Structure

```
├── docs/                        # Analysis documents
│   ├── 2hop_problem_explain.md  # Multi-hop BFS analysis for Xiangyao
│   └── benchmark_plan.md        # Benchmark strategy
├── data/
│   ├── synthetic/               # Small parquet files for dev testing
│   ├── mbal/                    # MBAL 10M address labels (Kaggle)
│   └── eth_transfers/           # Ethereum token transfers (AWS)
├── queries/
│   ├── clickbench_style/        # Scan/filter/aggregation queries (q01-q10)
│   ├── graph/                   # Graph analytics queries (q11-q13)
│   └── trm_pipeline/            # TRM entity flow rollup queries
├── benchmark/
│   ├── run_duckdb.sh            # CPU baseline
│   ├── run_sirius.sh            # GPU (gpu_processing + gpu_execution)
│   ├── run_starrocks.sh         # StarRocks comparison
│   └── results/                 # Benchmark outputs
├── scripts/                     # Data download and preparation
├── starrocks/                   # Docker setup for StarRocks
└── slides/                      # Presentation materials
```

## The TRM Pipeline

The core workload is **blockchain counterparty flow analytics**:

1. Parse raw transactions into address-level transfers
2. Aggregate to daily grain: `from_address → to_address → amount` per asset per day
3. Join with entity attribution map (MBAL: 10M labeled addresses)
4. Roll up to entity-level flows for Sankey visualizations

The key query pattern is a **double hash-join + GROUP BY aggregation** — exactly the workload
where GPU acceleration should provide significant speedup over CPU-based systems like StarRocks.

## Data Sources

| Dataset | Source | Size | Description |
|---------|--------|------|-------------|
| Ethereum token transfers | [AWS Public Blockchain](https://registry.opendata.aws/aws-public-blockchain/) | ~GB/month | Parquet, partitioned by date |
| MBAL address labels | [Kaggle](https://www.kaggle.com/datasets/yidongchaintoolai/mbal-10m-crypto-address-label-dataset) | ~1GB | 10M labeled crypto addresses |
| ETH/USD prices | CoinGecko / CryptoDataDownload | <1MB | Hourly price data |

## Quickstart

```bash
# 1. Download data
python scripts/download_mbal.py
python scripts/download_eth_transfers.py --months 1

# 2. Prepare tables
python scripts/prepare_tables.py

# 3. Run benchmarks
bash benchmark/run_duckdb.sh       # CPU baseline
bash benchmark/run_sirius.sh       # Sirius GPU
bash benchmark/run_starrocks.sh    # StarRocks (requires Docker)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SIRIUS_BIN` | `~/sirius-dev/build/release/duckdb` | Sirius binary path |
| `SIRIUS_PIXI_ENV` | `~/sirius-dev/.pixi/envs/cuda12` | Pixi env for LD path |
| `GPU_CACHE_SIZE` | `20 GB` | GPU cache allocation |
| `GPU_PROC_SIZE` | `15 GB` | GPU processing buffer |

## Related Documents

- [2-Hop Problem Analysis](docs/2hop_problem_explain.md) — why recursive CTEs can't do bounded BFS efficiently
- [TRM Pipeline Spec](docs/trm_pipeline.md) — full TRM counterparty flow specification
