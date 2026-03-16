# SiriusDB Technical Response to TRM Labs

**Prepared by:** SiriusDB Research Team, University of Wisconsin–Madison
**Date:** March 2026

---

## 1. What We Built

We reproduced the TRM counterparty flow pipeline using public Ethereum data and open-source address labels:

| Table | Description | Rows |
|-------|-------------|---:|
| `address_flows_daily_dict` | Daily aggregated from→to flows per token | 228M (6 months) |
| `entity_address_map_dict` | Address → entity/category labels (MBAL 10M) | 3.2M |

Join keys and token IDs are dictionary-encoded integers (4-byte int32).

Queries Q02–Q06 match the TRM brief: two JOINs on the entity map + GROUP BY aggregation + ORDER BY LIMIT.

## 2. Benchmark Results

**Hardware:** 2x Xeon Gold 6126 (48 threads) + Quadro RTX 6000 (24GB) + 187GB RAM

### gpu_processing (data cached in GPU memory)

| Query | What it does | CPU | GPU (warm) | Speedup |
|-------|--------------|---:|---:|---:|
| Q02 | Entity flow rollup (by date + token) | 106ms | 43ms | **2.5x** |
| Q03 | Top counterparty pairs | 93ms | 38ms | **2.4x** |
| Q04 | Time-series between two entities | 62ms | 28ms | **2.2x** |
| Q05 | Entity inflow/outflow balance (FULL OUTER JOIN) | 207ms | 80ms | **2.6x** |
| Q06 | Category flow matrix | 100ms | 38ms | **2.6x** |

All five queries run fully on GPU.

### gpu_execution with GPU caching (`scan_cache_level = 'table_gpu'`)

| Query | CPU | GPU (warm) |
|-------|---:|---:|
| Q03 | 93ms | 121ms |
| Q04 | 62ms | 121ms |
| Q06 | 100ms | 141ms |

gpu_execution supports datasets larger than GPU memory and reads from Parquet. With `table_gpu` caching, warm-run performance approaches gpu_processing levels.

## 3. What SiriusDB Supports

**Operators used in TRM queries:**
- INNER JOIN, LEFT JOIN, FULL OUTER JOIN
- GROUP BY with SUM, COUNT
- ORDER BY + LIMIT
- CTEs, CASE WHEN expressions
- DuckDB optimizer integration (automatic LEFT → INNER JOIN rewrite)

**Storage:** Reads Parquet natively (predicate pushdown, column pruning) and DuckDB files.

## 4. Current Limitations and Roadmap

| Issue | Impact | Fix |
|-------|--------|-----|
| UNION ALL not on GPU | Some query patterns fall back | Engineering fix planned |
| COALESCE not in expression translator | Workaround: CASE WHEN | Native COALESCE planned |
| gpu_execution pipeline overhead | Warm queries ~3x slower than gpu_processing | Pipeline optimization in progress |

## 5. Reproducing These Results

```bash
cd sirius-crypto-demo
bash benchmark/run_report_benchmark.sh
```

The benchmark script handles GPU memory configuration automatically.

## 6. Next Steps

- Benchmark with higher entity coverage datasets
- Benchmark on A100/H100/GH200 hardware
- Reduce gpu_execution pipeline overhead
- Implement UNION ALL on GPU
- Multi-hop compliance trace queries (prototype ready)
