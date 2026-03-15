# Sirius Crypto Analytics Benchmark

**Date:** 2026-03-15
**Hardware:** Quadro RTX 6000 (24GB VRAM), NVIDIA driver 580.126.20 (CUDA 13.0)
**Dataset:** Ethereum token transfers (ERC-20), dictionary-encoded integer join keys
**Branch:** `fix/left-join-groupby-crash` (commit 8094aa1)

## Dataset

| Period | Rows (token_transfers) | Rows (daily_flows) | Entities | DuckDB size |
|--------|------------------------|--------------------|---------:|------------:|
| 3 months (Q4 2025) | 292M | 117M | 3.2M | 38 GB |
| 6 months (H2 2025) | 528M | 228M | 3.2M | 68 GB |

All queries use **dictionary-encoded integer join keys** (V3): flow addresses are mapped to integer `addr_id` via an entity lookup table. This reduces join key size from 42-byte hex strings to 4-byte integers, enabling GPU-efficient hash joins.

## Queries

| Query | Description | SQL Pattern |
|-------|-------------|-------------|
| Q02 | Entity flows by date and asset | `LEFT JOIN x2` + `GROUP BY` + `HAVING` + `ORDER BY` + `LIMIT` |
| Q03 | Entity-to-entity total flows | `JOIN x2` + `GROUP BY` + `ORDER BY` + `LIMIT` |
| Q04 | Daily flow between specific entities | `JOIN x2` + `WHERE` + `GROUP BY` + `ORDER BY` |
| Q05 | Entity inflow/outflow balance | 2x `CTE` (`JOIN` + `GROUP BY`) + `FULL OUTER JOIN` |
| Q06 | Category-to-category flow matrix | `JOIN x2` + `GROUP BY` + `ORDER BY` |

## Table 1 — 3 Months (Q4 2025, 117M flows)

All three engines can process this dataset. `gpu_processing` caches data in GPU memory for sub-100ms hot runs.

| Query | DuckDB CPU | gpu_processing cold | gpu_processing hot | gpu_execution cold | gpu_execution hot |
|-------|----------:|--------------------:|-------------------:|-------------------:|------------------:|
| Q02   |   2,290 ms |         11,171 ms |           **18 ms** |          2,216 ms |          556 ms |
| Q03   |   2,257 ms |          2,063 ms |           **13 ms** |          1,925 ms |          323 ms |
| Q04   |   2,135 ms |          2,136 ms |            **6 ms** |          1,884 ms |          334 ms |
| Q05   |   2,253 ms |          2,758 ms |          **143 ms** |       1,665 ms* |           N/A* |
| Q06   |   2,231 ms |          2,086 ms |           **13 ms** |          1,904 ms |          333 ms |

*Q05 gpu_execution: query completes but crashes during cleanup, preventing hot run measurement. Cold timing is valid. Query rewritten with `CASE WHEN` instead of `COALESCE` (not yet implemented in gpu_execution).

### Speedup vs CPU (hot runs)

| Query | gpu_processing | gpu_execution |
|-------|---------------:|--------------:|
| Q02   |       **127x** |        4.1x |
| Q03   |       **174x** |        7.0x |
| Q04   |       **356x** |        6.4x |
| Q05   |        **16x** |     1.4x (cold) |
| Q06   |       **172x** |        6.7x |

## Table 2 — 6 Months (H2 2025, 228M flows)

Dataset exceeds GPU memory for `gpu_processing`. Only `gpu_execution` (out-of-core pipeline) can run on GPU.

| Query | DuckDB CPU | gpu_execution cold | gpu_execution hot |
|-------|----------:|-------------------:|------------------:|
| Q02   |   8,678 ms |          2,874 ms |      **1,093 ms** |
| Q03   |   7,546 ms |          2,196 ms |        **566 ms** |
| Q04   |   8,405 ms |          2,106 ms |        **547 ms** |
| Q05   |   8,654 ms |       2,610 ms* |           N/A* |
| Q06   |   8,149 ms |          2,185 ms |        **555 ms** |

*Q05: same cleanup crash as 3-month. Cold timing is valid.

### Speedup vs CPU (hot runs)

| Query | gpu_execution |
|-------|----:|
| Q02   | **7.9x** |
| Q03   | **13.3x** |
| Q04   | **15.4x** |
| Q05   | **3.3x** (cold) |
| Q06   | **14.7x** |

## Key Takeaways

1. **gpu_processing delivers 16-356x speedup** when data fits in GPU memory. Hot runs complete in 6-143ms because data is fully cached on GPU.

2. **gpu_execution scales beyond VRAM** with 8-15x speedup on 6-month data (228M rows). It uses an out-of-core pipeline that streams data through GPU without requiring full dataset caching.

3. **Cold vs hot gap narrows at scale.** gpu_execution cold runs are only 2-4x slower than hot, since parquet I/O is a smaller fraction of total work at larger data sizes.

4. **Dictionary encoding is essential.** The same queries on VARCHAR join keys (42-char hex addresses) are 2-5x *slower* than CPU on GPU — string hashing and memory consumption dominate. Integer join keys reduce this by 10x.

5. **Scaling is near-linear.** From 3-month to 6-month (2x data), CPU slows 3.6x while gpu_execution hot slows ~1.7x, showing better GPU scaling efficiency.

## Known Limitations

- **COALESCE not implemented** in gpu_execution. Workaround: rewrite as `CASE WHEN col IS NOT NULL THEN col ELSE default END`.
- **FULL OUTER JOIN cleanup crash** in gpu_execution. The join itself produces correct results, but a crash during resource cleanup prevents hot runs and multi-query sessions.

## Engine Comparison

| Feature | gpu_processing | gpu_execution |
|---------|----------------|---------------|
| Data source | DuckDB tables | Parquet files |
| Memory model | Full GPU caching | Out-of-core streaming |
| Best for | Repeated queries, fits in VRAM | Large datasets, ad-hoc |
| LEFT JOIN | Supported (fixed 2026-03-15) | Supported |
| FULL OUTER JOIN | Supported | Supported (cleanup bug) |
| COALESCE | Supported | Not yet (use CASE WHEN) |
| Warm run latency | Sub-100ms | 300-1100ms |

## Methodology

- **CPU:** DuckDB (single-node, all cores), 1 warmup + 1 measured run
- **gpu_processing:** Single DuckDB session with `gpu_buffer_init('9 GB', '9 GB')`, 1 cold + 1 hot run
- **gpu_execution:** Parquet views via `read_parquet()`, config `sirius_rtx6000.cfg`, 1 cold + 1 hot run
- Cold = first execution (includes data loading and GPU format conversion)
- Hot = second execution (benefits from GPU caching)
- Q05 rewritten to use `CASE WHEN` instead of `COALESCE` for gpu_execution compatibility
- All times are wall-clock elapsed
