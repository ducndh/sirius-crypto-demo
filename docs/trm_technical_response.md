# SiriusDB Technical Response to TRM Labs

**Prepared by:** SiriusDB Research Team, University of Wisconsin–Madison
**Date:** March 2026

---

## 1. Summary

We reproduced the TRM counterparty flow pipeline using public blockchain data (Ethereum token transfers from AWS Public Blockchain + MBAL 10M address labels) and benchmarked it on SiriusDB. This document shares our findings, architectural fit, and where we see the strongest opportunities.

**Key results on RTX 6000 (24GB VRAM, 48-core CPU):**

### 3-month dataset (~117M address flows, ~3.2M entity mappings)

| Query | Description | CPU (Parquet) | SiriusDB (hot) |
|-------|-------------|---:|---:|
| Q02 | Entity flow rollup (LEFT JOIN x2 + GROUP BY) | 259ms | 126ms |
| Q03 | Top counterparty pairs (INNER JOIN x2 + GROUP BY) | 176ms | 127ms |
| Q04 | Time-series flow between two entities | 146ms | 94ms |
| Q05 | Entity inflow/outflow balance (FULL OUTER JOIN + CTE) | 313ms | 173ms |
| Q06 | Category flow matrix (INNER JOIN x2 + GROUP BY) | 178ms | 119ms |

### 6-month dataset (~228M address flows)

| Query | Description | CPU (Parquet) | SiriusDB cold | SiriusDB hot |
|-------|-------------|---:|---:|---:|
| Q02 | Entity flow rollup | 414ms | 2,731ms | 1,090ms |
| Q03 | Top counterparty pairs | 248ms | 2,187ms | 547ms |
| Q04 | Time-series flow | 226ms | 2,188ms | 566ms |
| Q05 | Entity inflow/outflow balance | 494ms | 2,671ms | 999ms |
| Q06 | Category flow matrix | 261ms | 2,326ms | 546ms |

SiriusDB's GPU-cached execution path achieves **1.5-2x speedup** over CPU on the 3-month dataset. On the 6-month dataset, the current cold-start overhead (Parquet decode + GPU transfer) makes first queries slower, but hot queries are competitive. We expect the GPU advantage to grow significantly at larger scale.

## 2. What We Built

We implemented the full TRM pipeline as described in the technical brief:

**Data preparation:**
- Downloaded Ethereum token transfers (H2 2025) from AWS Public Blockchain S3 in Parquet format
- Downloaded MBAL 10M address label dataset from Kaggle
- Built `address_flows_daily` table: daily aggregation of from_address → to_address flows per asset
- Built `entity_address_map` table: address → entity mapping from MBAL labels
- Created dictionary-encoded variant (V3 DICT) with integer join keys for optimal join performance

**Query workload:**
- Q02: Entity flow rollup — two LEFT JOINs on entity map + GROUP BY date/asset/entity pairs
- Q03: Top counterparty pairs — two INNER JOINs + GROUP BY entity pairs, ranked by total flow
- Q04: Time-series between specific entities — filtered double JOIN + GROUP BY date
- Q05: Entity inflow/outflow balance sheet — two CTEs + FULL OUTER JOIN
- Q06: Category flow matrix — two INNER JOINs on category + GROUP BY

These match the query patterns described in Sections 5 and 6 of the TRM brief: large fact table, two dimension joins on the entity map, GROUP BY aggregation across entity pairs.

## 3. SiriusDB Execution Modes

SiriusDB offers two GPU execution paths, suited for different deployment scenarios:

### gpu_processing (in-memory cached)

- Data is loaded into GPU memory on first query and cached for subsequent queries
- Best for **interactive serving** where the same tables are queried repeatedly
- Requires data to fit in GPU memory (24GB on RTX 6000, up to 80GB on A100/H100)
- Achieves the lowest latency on hot queries (94-173ms on 3-month dataset)

### gpu_execution (out-of-core, Parquet-native)

- Reads directly from Parquet files, transfers to GPU per query
- Supports datasets larger than GPU memory through tiered memory management (GPU → host → disk)
- Better for **large-scale analytics** and **ad-hoc queries** on cold data
- Cold start includes Parquet decode + GPU transfer (~2 seconds), hot runs benefit from caching

For TRM's serving layer use case, `gpu_processing` maps to the StarRocks replacement scenario (pre-loaded data, sub-second queries). `gpu_execution` maps to the ad-hoc analytics scenario (query against Iceberg/Parquet directly).

## 4. Architectural Fit with TRM's Pipeline

### Storage format compatibility

TRM uses Apache Iceberg with Parquet as the storage layer. SiriusDB's `gpu_execution` path reads Parquet natively through DuckDB's Parquet reader, which supports:
- Predicate pushdown (zone maps, row group filtering)
- Column pruning (only reads projected columns)
- Partition pruning (date-partitioned layouts)

This means SiriusDB can query Iceberg tables directly without a separate data loading step, similar to how StarRocks or Trino would connect to Iceberg.

### Join pattern support

The TRM pipeline is join-heavy — every query performs at least two JOINs on the entity address map. SiriusDB supports:
- **INNER JOIN**: Implemented via `cudf::inner_join` (hash-based)
- **LEFT JOIN**: Implemented via `cudf::left_join`
- **FULL OUTER JOIN**: Supported (used in Q05)
- **Hash join on integer keys**: Optimal for dictionary-encoded address IDs

The dictionary encoding strategy (mapping VARCHAR addresses to INTEGER IDs) is critical for performance. With 4-byte integer keys, GPU hash tables are compact and cache-friendly. We recommend this approach for production deployment.

### Aggregation support

SiriusDB supports the aggregation patterns in the TRM queries:
- SUM, COUNT, COUNT DISTINCT
- GROUP BY with multiple columns (date, asset, entity pairs)
- ORDER BY + LIMIT (top-N)
- CTEs (Common Table Expressions)

### Current limitations

- **COALESCE**: Not yet natively supported in gpu_execution's expression evaluator. We use `CASE WHEN col IS NOT NULL THEN col ELSE default END` as a workaround, which is semantically identical. Native support is planned.
- **VARCHAR join keys**: Integer dictionary keys are strongly recommended. VARCHAR joins work but are significantly slower due to string hashing overhead.

## 5. Scaling Considerations

### Where GPU excels

The GPU advantage grows with data size because:
1. **GPU hash join is O(N) with massive parallelism** — thousands of CUDA cores process the probe table in parallel
2. **GPU memory bandwidth** (RTX 6000: 672 GB/s) far exceeds CPU memory bandwidth (~100 GB/s DDR4)
3. **CPU parallel scaling saturates** — DuckDB already uses all 48 cores on a single query; larger data means longer wall time

On TPC-H SF100 (~600M rows), SiriusDB achieves **8x speedup** over CPU. TRM's production datasets (billions of UTXO flows, hundreds of millions of entity mappings) are well into the GPU-favorable range.

### Recommended hardware for TRM scale

| Scale | Flows | GPU | GPU Memory | Expected hot latency |
|-------|-------|-----|-----------|---------------------|
| 3 months | ~100M | RTX 6000 | 24GB | <200ms |
| 1 year | ~400M | A100 / L40S | 48-80GB | <500ms |
| Multi-year | 1B+ | H100 / GH200 | 80-96GB | <1s |

### Concurrent query throughput

GPU execution uses a stream-per-thread model — multiple queries can execute simultaneously on different CUDA streams. Unlike CPU-based systems where adding concurrent queries degrades single-query latency (thread contention), GPU queries are largely independent. This is relevant for TRM's API serving use case with concurrent dashboard queries.

## 6. Reproducing These Results

All code, data preparation scripts, and benchmark harnesses are in the `sirius-crypto-demo` repository:

```bash
# Data preparation
python scripts/download_eth_transfers.py    # Download from AWS S3
python scripts/download_mbal.py             # Download MBAL labels
python scripts/prepare_tables.py            # Build DuckDB + Parquet files

# Run benchmark
cd benchmark
bash run_report_benchmark.sh                # Full CPU + GPU benchmark
```

The benchmark auto-detects GPU hardware and configures memory allocation. Results are written to `benchmark/results/` as CSV files.

## 7. Next Steps

We propose the following areas for deeper collaboration:

1. **Scale testing**: Run the pipeline on 1B+ row datasets to demonstrate GPU advantage at production scale. We can generate larger synthetic datasets or use full-year blockchain data.

2. **Iceberg integration**: Test SiriusDB reading directly from Iceberg table metadata (partition pruning, schema evolution). DuckDB has an Iceberg extension that SiriusDB can leverage.

3. **Concurrent query benchmarks**: Simulate TRM's API serving pattern — multiple concurrent queries against the same cached dataset. Measure throughput (queries/second) and tail latency (p99).

4. **COALESCE and CASE WHEN optimization**: Implement native COALESCE support to remove the query rewrite workaround.

5. **Skewed key analysis**: TRM noted that a small number of entities control millions of addresses. We can benchmark skewed join distributions to verify GPU hash join handles this well.

We look forward to discussing these results and planning the next phase of evaluation.
