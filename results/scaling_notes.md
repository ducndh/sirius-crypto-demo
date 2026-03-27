# Scaling Benchmark Notes — RTX 6000 (2026-03-27)

## Setup
- GPU: Quadro RTX 6000, 24GB VRAM, sm_75
- CPU: Xeon Gold 6126, 2×12c @ 2.6GHz
- Data: `/tmp/crypto_slim.duckdb` — address_flows_daily_dict + entity_address_map_int
- Build: `feature/union-all-gpu-processing` branch, ENABLE_LEGACY_SIRIUS=ON
- Cache: `gpu_buffer_init('12 GB', '8 GB')`
- Script: `scripts/scaling_validated.sh`

## Results summary (GPU speedup range: 3.5–7.1x)

| Slice | Rows | GPU limit |
|-------|------|-----------|
| 3mo | 63M | OK |
| 6mo | 126M | OK |
| 9mo | 190M | OK |
| 12mo | 255M | OK |
| 15mo | 320M | OOM — working pool |

## Memory constraint
The bottleneck is the **working memory pool (8 GB)**, not the data cache (12 GB).
Projection pushdown works — only needed columns are loaded (~16B/row).
At 320M rows, intermediate join results overflow 8 GB working pool.
Error: `maximum pool size exceeded: current/max/try = 8GB, 8GB, 2.38GB`

Extending to `gpu_buffer_init('10 GB', '10 GB')` should allow 15mo+ to run.

## Known measurement caveats
- CPU timing uses median of 3 samples after 2 warmups — can have ~15ms variance
- Q02 CPU at 12mo reported 48ms but true warm median is ~73-80ms (confirmed with 10-run test)
- Data distribution is uneven: Q2 2024 has 8x fewer entity-143 matching rows than Q1
  (2,477 vs 16,032) — causes irregular scaling patterns, not a bug
- GPU timings are more stable (less OS scheduling variance)

## Workarounds baked into queries
- `CAST(SUM(...) AS DOUBLE)` — bare SUM() returns DECIMAL which crashes gpu_physical_result_collector
- `CASE WHEN x IS NOT NULL THEN x ELSE y END` — COALESCE not implemented in gpu_processing
- UNION ALL requires `feature/union-all-gpu-processing` branch (commits 7139a05, 4d1c2ac)

## To replicate on another machine
1. Pull source data to `/tmp/crypto_slim.duckdb` (or update SRC_DB in script)
2. Build sirius on `feature/union-all-gpu-processing` with `pixi run -e cuda12 make release`
3. `bash scripts/scaling_validated.sh`
