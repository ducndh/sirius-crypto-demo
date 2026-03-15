# Sirius Crypto Demo Benchmark Report

**Date:** 2026-03-15
**Hardware:** Quadro RTX 6000 (24GB VRAM), NVIDIA driver 580.126.20
**Dataset:** Q4 2025 Ethereum token transfers — 292M raw rows, 117M daily flows, 3.2M entities
**Database:** `crypto_demo_2025q4.duckdb` (38GB)
**Branch:** `fix/left-join-groupby-crash` (includes LEFT JOIN materialize fix)

## Benchmark Configuration

- **Warmup runs:** 1 (excluded from results)
- **Measured runs:** 3 (median reported)
- **Engines:** DuckDB CPU, gpu_processing (legacy), gpu_execution (new)
- **Query variants:**
  - V1: VARCHAR join keys (42-char hex Ethereum addresses)
  - V3: DICT integer join keys (dictionary-encoded addr_id)

## Query Descriptions

| Query | Description | Ops |
|-------|-------------|-----|
| Q01 | Stage 1 raw aggregation | GROUP BY + SUM + COUNT on 292M rows, SUBSTR |
| Q02 | Entity flows by date/asset | LEFT JOIN x2 + GROUP BY + HAVING + ORDER BY + LIMIT |
| Q03 | Entity-to-entity totals | JOIN x2 + GROUP BY + ORDER BY + LIMIT |
| Q04 | Specific entity pair daily flow | JOIN x2 + WHERE filter + GROUP BY + ORDER BY |
| Q05 | Entity inflow/outflow balance | 2x CTE (JOIN + GROUP BY) + FULL OUTER JOIN |
| Q06 | Category-to-category flows | JOIN x2 + GROUP BY + ORDER BY |

## Results — Median Runtime (ms)

### V3 (Dictionary-Encoded Integer Joins) — Recommended

| Query | CPU | gpu_processing | gpu_execution | GPU-P Speedup | GPU-E Speedup |
|-------|-----|----------------|---------------|---------------|---------------|
| Q02 | 2,290 | **18** | 587 | **127x** | 3.9x |
| Q03 | 2,257 | **13** | 302 | **174x** | 7.5x |
| Q04 | 2,135 | **6** | 314 | **356x** | 6.8x |
| Q05 | 2,253 | **143** | 516 | **16x** | 4.4x |
| Q06 | 2,231 | **13** | 334 | **172x** | 6.7x |

### V1 (VARCHAR Joins — 42-char hex addresses)

| Query | CPU | gpu_processing | gpu_execution | GPU-P vs CPU | GPU-E vs CPU |
|-------|-----|----------------|---------------|--------------|--------------|
| Q02 | 3,477 | 14,538 | 19,553 | 0.24x (slower) | 0.18x (slower) |
| Q03 | 3,301 | 10,646 | 12,289 | 0.31x (slower) | 0.27x (slower) |
| Q04 | 3,095 | 10,438 | 2,976 | 0.30x (slower) | **1.04x** |
| Q05 | 3,380 | 1,062 | 24,153 | **3.2x** | 0.14x (slower) |
| Q06 | 3,279 | 9,236 | 12,234 | 0.36x (slower) | 0.27x (slower) |

### Q01 (Raw 292M rows — no V1/V3 variant)

| Engine | Median (ms) | vs CPU |
|--------|-------------|--------|
| CPU | 17,260 | 1x |
| gpu_processing | 41,791 | 0.41x (slower) |
| gpu_execution | N/A (no parquet) | — |

## Key Findings

### 1. Dictionary encoding is critical for GPU performance

V3 (integer join keys) delivers **16-356x speedup** on gpu_processing vs CPU. The same queries on V1 (VARCHAR join keys with 42-char hex addresses) are **2-5x slower than CPU**. The bottleneck is materialization: 117M VARCHAR strings that are each 42 characters consume far more GPU memory and bandwidth than 4-byte integer IDs.

### 2. gpu_processing V3 hot runs are sub-20ms

With cached data, gpu_processing completes Q02-Q06 in 6-18ms (median). This is because:
- Integer join keys fit entirely in GPU cache (3.2M entities x 4 bytes = 12.8MB)
- Dictionary tables are compact (~100MB total vs multi-GB VARCHAR tables)
- GPU hash joins on integer keys are extremely efficient

### 3. gpu_execution is slower but more robust

gpu_execution (new pipeline engine) is 4-7x faster than CPU on V3, but 10-30x slower than gpu_processing. This is expected: gpu_execution uses out-of-core execution with `cudf::gather` and doesn't benefit from the same caching model as gpu_processing's `GPUBufferManager`.

### 4. VARCHAR JOIN performance gap

V1 VARCHAR queries are slower on GPU than CPU because:
- 42-character hex strings (Ethereum addresses) are expensive to hash and compare on GPU
- String data exceeds GPU cache capacity, causing spills
- CPU's radix hash join is cache-efficient for this pattern
- **Recommendation:** Always use dictionary-encoded tables for GPU analytics

### 5. LEFT JOIN fix validated

Q02 uses `LEFT JOIN` x2 (the previously-crashing pattern). After the materialize kernel fix:
- gpu_processing V3 Q02: 18ms (127x speedup) — previously crashed with exit code 188
- gpu_execution V3 Q02: 587ms (3.9x speedup) — was already working via cudf::gather

### 6. Q05 anomaly (gpu_processing V1)

Q05 shows 3.2x speedup on V1 gpu_processing while all other V1 queries are slower than CPU. Q05 uses CTEs with single-table JOINs (not self-join) and FULL OUTER JOIN, which avoids the VARCHAR cross-join explosion seen in Q02-Q06's dual self-joins.

## Cold vs Hot Performance

### gpu_processing (V3, representative)

| Query | Cold (warmup) | Hot (median) | Speedup |
|-------|---------------|--------------|---------|
| Q02 | 11,171 | 18 | 621x |
| Q03 | 2,063 | 13 | 159x |
| Q04 | 2,136 | 6 | 356x |
| Q05 | 2,758 | 143 | 19x |
| Q06 | 2,086 | 13 | 160x |

Cold runs load data from DuckDB storage and convert to GPU format. Hot runs execute entirely on cached GPU data.

### gpu_execution (V3)

| Query | Cold (warmup) | Hot (median) | Speedup |
|-------|---------------|--------------|---------|
| Q02 | 4,816 | 587 | 8.2x |
| Q03 | 1,853 | 302 | 6.1x |
| Q04 | 1,893 | 314 | 6.0x |
| Q05 | 2,091 | 516 | 4.1x |
| Q06 | 1,905 | 334 | 5.7x |

## Bugs Fixed During Benchmarking

### LEFT JOIN materialize crash (exit code 188)

**Root cause:** Two bugs in `src/cuda/operator/materialize.cu`:

1. **OOB sentinel access:** cudf LEFT JOIN produces `JoinNoMatch` (INT32_MIN = 0x80000000) for unmatched rows. After sign-extension to uint64, these were used as array indices in all 6 materialize kernels, causing out-of-bounds GPU memory access.

2. **Use-after-free on self-join:** `materializeExpression` and `materializeString` freed input data/offset/mask pointers after materialization. When a table is self-joined (both `src` and `dst` alias the same entity table), two columns share the same data pointer — first materialization freed it, second read freed memory.

**Fix:** Added bounds checks against `INVALID_ROW_ID` (derived from `cudf::JoinNoMatch`) in all 6 kernels, and removed the premature frees.

**Scope:** Only affects gpu_processing LEFT JOIN path. gpu_execution uses `cudf::gather` with `out_of_bounds_policy::NULLIFY` (not affected). TPC-H never caught this because Q13 (the only LEFT JOIN) has a non-equality condition that always falls back to CPU.

## Raw Data

CSV: `benchmark_Quadro_RTX_6000_20260315_015106.csv`
