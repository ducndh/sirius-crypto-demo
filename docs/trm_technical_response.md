# SiriusDB Technical Response to TRM Labs

**Prepared by:** SiriusDB Research Team, University of Wisconsin–Madison
**Date:** March 2026

---

## 1. What We Built

We reproduced the TRM counterparty flow pipeline using public blockchain data:
- **Ethereum token transfers** (H2 2025) from AWS Public Blockchain S3
- **MBAL 10M address labels** from Kaggle as the entity address map
- **Dictionary-encoded integer join keys** — VARCHAR addresses mapped to 4-byte integer IDs

Tables:
- `address_flows_daily_dict`: daily from_addr_id → to_addr_id flows per asset (~117M rows for 3 months, ~228M rows for 6 months)
- `entity_address_map_dict`: addr_id → entity/category mapping (~3.2M rows)

Queries (Q02-Q06) match the patterns from the TRM brief: two JOINs on entity map + GROUP BY aggregation + ORDER BY LIMIT.

## 2. Benchmark Results

**Hardware:** 2x Intel Xeon Gold 6126 (48 threads) + Quadro RTX 6000 (24GB VRAM) + 187GB RAM

### 3-month (~117M flows)

| Query | Description | CPU (Parquet) | SiriusDB gpu_processing cold | SiriusDB gpu_processing hot |
|-------|-------------|---:|---:|---:|
| Q02 | Entity flow rollup (LEFT JOIN x2) | 259ms | 336ms | 126ms |
| Q03 | Top counterparty pairs (JOIN x2) | 176ms | 257ms | 127ms |
| Q04 | Time-series between entities | 146ms | 224ms | 94ms |
| Q05 | Inflow/outflow balance (FULL OUTER JOIN) | 313ms | 344ms | 173ms |
| Q06 | Category flow matrix (JOIN x2) | 178ms | 276ms | 119ms |

### 6-month (~228M flows)

| Query | Description | CPU (Parquet) | SiriusDB gpu_execution cold | SiriusDB gpu_execution hot |
|-------|-------------|---:|---:|---:|
| Q02 | Entity flow rollup (LEFT JOIN x2) | 414ms | 2,731ms | 1,090ms |
| Q03 | Top counterparty pairs (JOIN x2) | 248ms | 2,187ms | 547ms |
| Q04 | Time-series between entities | 226ms | 2,188ms | 566ms |
| Q05 | Inflow/outflow balance (FULL OUTER JOIN) | 494ms | 2,671ms | 999ms |
| Q06 | Category flow matrix (JOIN x2) | 261ms | 2,326ms | 546ms |

SiriusDB has two execution modes:
- **gpu_processing**: Loads data into GPU memory, caches across queries. Requires dataset to fit in VRAM. Hot runs are 1.4-1.6x faster than CPU on parquet for 3-month data.
- **gpu_execution**: Reads from Parquet directly, supports datasets larger than GPU memory.

### Reference

On TPC-H SF100 (~600M rows) on the same hardware, SiriusDB achieves ~8x speedup over CPU.

## 3. What SiriusDB Supports

Operators used in TRM queries:
- INNER JOIN, LEFT JOIN, FULL OUTER JOIN
- GROUP BY with SUM, COUNT, COUNT DISTINCT
- ORDER BY + LIMIT
- CTEs
- CASE WHEN (used as workaround for COALESCE, which is not yet implemented)

Storage:
- Reads Parquet natively (predicate pushdown, column pruning)
- Can also query DuckDB files

Current limitations:
- COALESCE not yet implemented in gpu_execution (CASE WHEN workaround works)
- VARCHAR join keys work but are slower — integer dictionary keys recommended

## 4. Reproducing These Results

```bash
cd sirius-crypto-demo

# Data preparation
python scripts/download_eth_transfers.py
python scripts/download_mbal.py
python scripts/prepare_tables.py

# Run benchmark
bash benchmark/run_report_benchmark.sh
```

Results output to `benchmark/results/` as CSV.

## 5. Next Steps

- Benchmark on larger datasets (1B+ rows)
- Benchmark on newer GPU hardware (A100, H100, GH200)
- Benchmark concurrent queries
- Implement native COALESCE support
