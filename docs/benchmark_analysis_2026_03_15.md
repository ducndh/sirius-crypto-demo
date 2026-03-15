# Crypto Demo Benchmark Analysis — 2026-03-15

## Hardware

- **CPU**: 2x Intel Xeon Gold 6126 @ 2.60GHz (24 physical cores, 48 threads)
- **RAM**: 187GB
- **GPU**: Quadro RTX 6000 (24GB GDDR6, compute capability 7.5, Turing)
- **Driver**: 580.126.20

## Dataset

Ethereum token transfers with dictionary-encoded integer join keys (V3 DICT):
- 3-month (Q4 2025): ~117M rows in `address_flows_daily_dict`, ~3.2M rows in `entity_address_map_dict`
- 6-month (H2 2025): ~228M rows in `address_flows_daily_dict`, ~3.2M rows in `entity_address_map_dict`
- Parquet files: `bench_flows_dict.parquet` (3mo), `bench_flows_dict_6mo.parquet` (6mo)
- DuckDB file: `crypto_demo_2025q4.duckdb` (76GB)

## Queries

TRM pipeline Q02-Q06. All involve 2x JOIN on integer keys + GROUP BY + aggregation.
- Q02: Entity flow rollup (LEFT JOIN x2)
- Q03: Top counterparty pairs (INNER JOIN x2)
- Q04: Time-series between two entities (INNER JOIN x2, filtered)
- Q05: Entity inflow/outflow balance (CTE + FULL OUTER JOIN)
- Q06: Category flow matrix (INNER JOIN x2)

COALESCE in Q02/Q03/Q05/Q06 rewritten to `CASE WHEN ... IS NOT NULL` for gpu_execution (not yet implemented in expression translator).

## Results

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

`gpu_processing` not benchmarked on 6-month — dataset exceeds 9GB+9GB GPU memory config.

### Reference: TPC-H SF100 on same hardware

On TPC-H SF100 (~600M rows), Sirius achieves ~8x speedup over CPU (21/22 queries pass).

## Bug Fixes Applied

Benchmark ran on `local/all-fixes` branch with two crash fixes:

1. **TABLE_SCAN filter crash** (`fix/table-scan-filter-crash`): OOB access when optimizer eliminates an always-true WHERE filter. Previously SIGSEGV on `WHERE f.date >= '2025-10-01'` queries.

2. **LEFT JOIN materialize crash** (`fix/left-join-groupby-crash`): Null handling in GPU materialize kernels. Previously SIGSEGV on LEFT JOIN + GROUP BY.

## Measurement Methodology

- **CPU (DuckDB file)**: Single DuckDB session, warmup + measured run with `.timer on` against `crypto_demo_2025q4.duckdb`.
- **CPU (Parquet)**: Single DuckDB session, warmup + measured run with `.timer on` against parquet files via `read_parquet()`.
- **gpu_processing**: Single session, `gpu_buffer_init('9 GB', '9 GB')`, cold + hot with `.timer on`.
- **gpu_execution**: Single session, parquet views, cold + hot with `.timer on`. Config: `sirius_rtx6000.cfg`.

## Open Questions

- How do these numbers change on stronger GPU hardware (A100, H100, GH200)?
- At what dataset size does GPU start winning on this hardware?
- Would concurrent query benchmarks show different results?
