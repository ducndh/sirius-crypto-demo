# Crypto Demo Benchmark Analysis — 2026-03-15

## Overview

This document captures benchmark results and performance analysis for the TRM counterparty flow pipeline running on SiriusDB. It is intended for the internal team to understand where our system stands, what the numbers actually mean, and where the bottlenecks are.

**Hardware:** 48-core CPU, Quadro RTX 6000 (24GB VRAM), 256GB RAM

**Dataset:** Ethereum token transfers with dictionary-encoded integer join keys (V3 DICT)
- 3-month (Q4 2025): ~117M rows in `address_flows_daily_dict`, ~3.2M rows in `entity_address_map_dict`
- 6-month (H2 2025): ~228M rows in `address_flows_daily_dict`, ~3.2M rows in `entity_address_map_dict`
- Parquet files: `bench_flows_dict.parquet` (3mo), `bench_flows_dict_6mo.parquet` (6mo)
- DuckDB file: `crypto_demo_2025q4.duckdb` (76GB, contains all tables)

**Queries:** TRM pipeline Q02-Q06, all involve JOIN (2x LEFT or INNER) on integer keys + GROUP BY + aggregation. Q05 also uses FULL OUTER JOIN + COALESCE (rewritten to CASE WHEN for gpu_execution).

## Benchmark Results

### 3-month (Q4 2025, ~117M flows)

| Query | CPU (DuckDB file) | CPU (Parquet) | gpu_processing cold | gpu_processing hot | gpu_execution cold | gpu_execution hot |
|-------|---:|---:|---:|---:|---:|---:|
| Q02 | 115ms | 259ms | 336ms | 126ms | 2,237ms | 596ms |
| Q03 | 109ms | 176ms | 257ms | 127ms | 2,017ms | 313ms |
| Q04 | 76ms | 146ms | 224ms | 94ms | 1,973ms | 323ms |
| Q05 | 145ms | 313ms | 344ms | 173ms | 2,207ms | 596ms |
| Q06 | 98ms | 178ms | 276ms | 119ms | 1,965ms | 314ms |

### 6-month (H2 2025, ~228M flows)

| Query | CPU (DuckDB file) | CPU (Parquet) | gpu_execution cold | gpu_execution hot |
|-------|---:|---:|---:|---:|
| Q02 | 109ms | 414ms | 2,731ms | 1,090ms |
| Q03 | 94ms | 248ms | 2,187ms | 547ms |
| Q04 | 74ms | 226ms | 2,188ms | 566ms |
| Q05 | 146ms | 494ms | 2,671ms | 999ms |
| Q06 | 94ms | 261ms | 2,326ms | 546ms |

Note: `gpu_processing` is not benchmarked on 6-month because the dataset exceeds the 9GB+9GB GPU memory allocation (would need larger VRAM or spilling).

## Key Findings

### 1. DuckDB CPU is very fast on this workload

DuckDB warm runs on DuckDB-format files complete in **75-145ms** — faster than any GPU path. This is because:

- **48-core parallelism**: DuckDB uses all available cores. For example, Q02 on parquet shows 0.259s wall time but 7.95s user time — roughly 30x CPU parallelism.
- **Pre-optimized columnar format**: The DuckDB file stores data in a columnar layout with zone maps, compression, and statistics. Warm queries hit OS page cache and skip I/O entirely.
- **Dictionary-encoded integer joins**: The V3 DICT schema uses 4-byte integer join keys (`from_addr_id`, `to_addr_id`, `addr_id`). DuckDB's radix hash join is extremely efficient for small fixed-width keys.
- **Small result cardinality**: Most queries produce <1000 output rows after aggregation. The bottleneck is the join/scan, not materialization.

**Takeaway**: For datasets that fit in RAM with pre-built DuckDB files, the CPU baseline is strong. GPU advantage appears at larger scale or with raw storage formats.

### 2. CPU on Parquet is 2-3x slower than DuckDB file

| Dataset | DuckDB file avg | Parquet avg | Ratio |
|---------|---:|---:|---:|
| 3-month | 109ms | 214ms | 2.0x slower |
| 6-month | 103ms | 329ms | 3.2x slower |

The gap comes from Parquet decode overhead (decompression, page parsing) vs DuckDB's native format which is already in memory-friendly layout. This is a fairer comparison for `gpu_execution` which also reads from Parquet.

### 3. gpu_processing hot is comparable to CPU on DuckDB file

On 3-month data, `gpu_processing` hot runs (94-173ms) are in the same ballpark as CPU on DuckDB file (75-145ms). Neither is dramatically faster.

**Why gpu_processing is not faster despite hot GPU cache:**

The `gpu_processing` code path (`src/operator/gpu_physical_table_scan.cpp`) implements a **two-scan algorithm** for every query execution, even hot runs:

1. **First scan**: Streams through the entire DuckDB table to calculate per-column memory requirements (row counts, offset array sizes, mask sizes)
2. **Second scan**: Re-scans the table to copy data from DuckDB chunks into contiguous GPU-friendly buffers

This two-scan approach exists because pre-allocating GPU memory without knowing exact sizes risks OOM. The code comment at line 123 explains:
```
// Perform the first scan to get column size and mask size. Here we perform two scans
// streamingly since storing all scanned chunks together in memory is extremely slow.
```

Additional overhead factors:
- **Single-threaded scan**: The code sets `num_threads = 1` — no parallel scanning
- **Format conversion**: DuckDB stores data in 1024-4096 row chunks; these must be coalesced into contiguous GPU buffers
- **Result materialization**: GPU results must be copied back to CPU and converted to DuckDB format for display

So on a hot run, the GPU compute (hash join, aggregation) is fast, but the scan/convert/materialize overhead is ~50-100ms regardless of data size. On a 48-core machine where CPU finishes the entire query in 75-145ms, this overhead alone makes GPU non-competitive.

### 4. gpu_execution is slower than CPU on this dataset size

`gpu_execution` hot runs are **2-10x slower** than CPU on parquet:

| Dataset | CPU Parquet avg | gpu_execution hot avg | Ratio |
|---------|---:|---:|---:|
| 3-month | 214ms | 428ms | 2.0x slower |
| 6-month | 329ms | 750ms | 2.3x slower |

`gpu_execution` cold runs (~2 seconds) include Parquet scanning + GPU transfer. Hot runs still take 300-1000ms because:
- The task-based pipeline has scheduling overhead (task creator, GPU thread pool, memory reservation)
- Each query re-scans Parquet (though subsequent runs benefit from OS page cache)
- Data must be transferred to GPU each time unless the GPU cache is warm

### 5. Where GPU wins: extrapolating to larger scale

The GPU advantage is expected to appear at larger data sizes where:
- **CPU scan time grows linearly** with data size (more rows to process)
- **GPU compute stays flat** for cached data (hash join and aggregation are massively parallel)
- **48-core CPU becomes saturated** — already using 30x parallelism on 228M rows

Based on TPC-H benchmarks at SF100 (~600M rows), Sirius achieves **8x speedup** over CPU. The crypto demo dataset at 228M rows is below this crossover point, especially with the integer-key optimization that makes CPU joins very cheap.

**Expected crossover**: ~500M-1B rows for this query pattern on this hardware. TRM's production Bitcoin dataset (1B+ UTXOs) and multi-chain Ethereum (hundreds of millions of transfers per year) would be in the GPU-favorable range.

### 6. Cold run analysis

Cold runs represent the first-query experience, including data loading:

| Engine | 3-month cold | 6-month cold | What's included |
|--------|---:|---:|---|
| gpu_processing | 224-344ms | N/A | DuckDB scan + 2-scan GPU convert + GPU compute |
| gpu_execution | 1,965-2,237ms | 2,087-2,731ms | Parquet scan + GPU transfer + GPU compute |

`gpu_execution` cold is ~2 seconds because it reads raw Parquet (decode + decompress) and transfers to GPU. `gpu_processing` cold is faster because it reads from the pre-built DuckDB file.

For an interactive serving layer (TRM's use case), cold query latency matters for the first query after system start. Subsequent queries benefit from caching.

## Bug Fixes Applied

This benchmark was run on `local/all-fixes` branch which includes two crash fixes:

1. **TABLE_SCAN filter crash** (`fix/table-scan-filter-crash`): Fixed OOB access when DuckDB's optimizer eliminates an always-true WHERE filter, creating gaps in projection_ids. Previously caused SIGSEGV on queries like `WHERE f.date >= '2025-10-01'` with JOINs.

2. **LEFT JOIN materialize crash** (`fix/left-join-groupby-crash`): Fixed null handling in GPU materialize kernels for LEFT JOIN results. Previously caused SIGSEGV on LEFT JOIN + GROUP BY queries.

3. **COALESCE workaround**: Q02, Q03, Q05, Q06 use COALESCE in original SQL. Since COALESCE is not yet implemented in `gpu_execution`'s expression translator (`gpu_expression_translator.cpp:398`), the gpu_execution queries use `CASE WHEN col IS NOT NULL THEN col ELSE default END` as a semantically equivalent workaround.

## Measurement Methodology

- **CPU (DuckDB file)**: Single DuckDB session, warmup run + measured run with `.timer on`. Queries run against `crypto_demo_2025q4.duckdb`.
- **CPU (Parquet)**: Single DuckDB session, warmup run + measured run with `.timer on`. Queries run against Parquet files via `read_parquet()` views.
- **gpu_processing**: Single DuckDB session with `gpu_buffer_init('9 GB', '9 GB')`, cold + hot run with `.timer on`.
- **gpu_execution**: Single DuckDB session, Parquet views, cold + hot run with `.timer on`. Config: `sirius_rtx6000.cfg`.

All measurements use DuckDB's built-in `.timer on` to avoid process startup overhead.

## Recommendations for TRM Evaluation

1. **Report both DuckDB-file and Parquet baselines** — TRM's production data sits in Iceberg (Parquet-based), so Parquet baseline is the fair comparison for gpu_execution.

2. **Scale up the dataset** — The current 228M row dataset is below the GPU crossover point. Request or generate 1B+ row datasets to demonstrate GPU advantage.

3. **Benchmark concurrent queries** — GPU can serve multiple queries simultaneously with stream-per-thread. CPU performance degrades under concurrency since it already uses all 48 cores for a single query.

4. **Implement COALESCE natively** — 8 of 10 COALESCE calls across Q02-Q06 are simple null-replacement patterns. Translating to CASE WHEN internally in `gpu_expression_translator.cpp` would remove the workaround.

5. **Profile gpu_execution cold start** — The 2-second cold start is dominated by Parquet decoding. Investigating columnar caching or pre-loading could improve first-query latency.
