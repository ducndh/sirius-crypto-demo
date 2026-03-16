# Crypto Demo Benchmark Analysis — 2026-03-15

## Hardware

- **CPU**: 2x Intel Xeon Gold 6126 @ 2.60GHz (24 physical cores, 48 threads)
- **RAM**: 187GB
- **GPU**: Quadro RTX 6000 (24GB GDDR6, Turing, compute capability 7.5)
- **Driver**: 580.126.20

## Dataset

Ethereum token transfers, dictionary-encoded integer join keys (V3 DICT):

| Dataset | Rows (flows) | Entity map rows | In-memory size | Parquet size |
|---------|---:|---:|---:|---:|
| 6-month (H2 2025) | 228M | 3.2M | ~3.6GB | ~1.9GB |

The `address_flows_daily_dict` table has 7 columns: `from_addr_id` (int32), `to_addr_id` (int32), `date` (date), `asset_id` (int32, dictionary-encoded token contract address), `amount` (double), `tx_count` (int64). The `asset` VARCHAR column (292K distinct 42-char ERC-20 contract addresses) has been dictionary-encoded to `asset_id`.

Entity coverage is very low: only 0.3% of flows (709K / 228M) have matching entities. DuckDB's optimizer rewrites `LEFT JOIN + HAVING != 'unknown'` → INNER JOIN, so Q02-Q04/Q06 effectively process only 709K rows after the join.

## Queries

TRM pipeline Q02–Q06. All involve 2x JOIN on integer keys + GROUP BY + aggregation.

| Query | Pattern | Join type after optimization |
|-------|---------|------------------------------|
| Q02 | Entity flow rollup (by date + asset_id) | INNER (optimizer rewrites LEFT+HAVING) |
| Q03 | Top counterparty pairs | INNER |
| Q04 | Time-series between two entities | INNER + WHERE filter |
| Q05 | Entity inflow/outflow balance | CTE → FULL OUTER JOIN |
| Q06 | Category flow matrix | INNER |

COALESCE rewritten to `CASE WHEN ... IS NOT NULL` for both gpu_processing and gpu_execution.

## Results — 228M rows

### gpu_processing (data cached in GPU memory, `gpu_buffer_init('10 GB', '10 GB')`)

| Query | CPU warm | GPU warm | Speedup |
|-------|---:|---:|---:|
| Q02 | 106ms | 43ms | **2.5x** |
| Q03 | 93ms | 38ms | **2.4x** |
| Q04 | 62ms | 28ms | **2.2x** |
| Q05 | 207ms | 80ms | **2.6x** |
| Q06 | 100ms | 38ms | **2.6x** |

Q02 previously fell back due to VARCHAR `asset` in GROUP BY. Dictionary-encoding to `asset_id` (int32) fixed this — Q02 now runs fully on GPU.

Q05 previously fell back due to FULL OUTER JOIN. Implemented `cudf::hash_join::full_join` route in gpu_processing — Q05 now runs fully on GPU at 80ms (2.6x speedup).

### gpu_execution with `table_gpu` cache

Setting `scan_cache_level = 'table_gpu'` keeps decoded tables in GPU memory across queries, similar to gpu_processing's `gpu_buffer_init`.

| Query | CPU warm | gpu_execution warm (no cache) | gpu_execution warm (table_gpu) |
|-------|---:|---:|---:|
| Q02 | 106ms | 1,129ms | 777ms |
| Q03 | 93ms | 545ms | **121ms** |
| Q04 | 62ms | 567ms | **121ms** |
| Q05 | 199ms | 1,010ms | 223ms |
| Q06 | 100ms | 546ms | **141ms** |

With `table_gpu` cache, Q03/Q04/Q06 drop from ~550ms to ~121-141ms — a 4x improvement from caching alone. The remaining gap vs gpu_processing (121ms vs 38ms) is pipeline coordination overhead.

Q02 and Q05 still show higher times (~777ms, ~223-1062ms) because their more complex plans (LEFT JOIN, FULL OUTER JOIN) incur more pipeline overhead.

## Measurement Methodology

- **CPU**: Single DuckDB session against `crypto_demo_2025q4.duckdb`, warmup + measured run, `.timer on`.
- **gpu_processing**: Single session, `gpu_buffer_init('10 GB', '10 GB')`, `~/.sirius/sirius.cfg` moved aside, cold + hot, `.timer on`. Session logs checked for "fallback to DuckDB".
- **gpu_execution**: Single DuckDB session with `-unsigned`, parquet views, `SET scan_cache_level = 'table_gpu'`, cold + warm runs, `.timer on`. Config: `sirius_rtx6000.cfg`.

## Data Pipeline

Raw `token_transfers` (528M rows, 11 columns) → pre-aggregated `address_flows_daily_dict` (228M rows, 7 columns). The aggregation groups by `(from_addr_id, to_addr_id, date, asset_id)` and sums `amount`/`tx_count`. Dropped columns: `transaction_hash`, `log_index`, `block_number`, `block_hash`, `block_timestamp`, `last_modified`.

Dictionary tables: `address_dictionary` (address → addr_id), `asset_dictionary` (token address → asset_id).
