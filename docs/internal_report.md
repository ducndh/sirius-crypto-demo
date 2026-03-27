# Internal Report: TRM Crypto Demo — What We Have and Where to Go

**Date:** 2026-03-27
**Author:** SiriusDB Team

---

## 1. Summary

We built an end-to-end demo of Sirius GPU-accelerated analytics on a real
blockchain workload matching TRM Labs' counterparty flow pipeline. The demo
uses public Ethereum data (AWS) and open-source address labels (MBAL 10M)
to reproduce TRM's entity flow rollup and multi-hop Sankey trace queries.

**Bottom line:** On the A100, Sirius delivers **3.5-7.8x speedup** over DuckDB
CPU on TRM-representative queries across datasets from 63M to 620M rows, with
all results validated for correctness.

---

## 2. What We Built

### Data Pipeline

```
AWS S3 (Ethereum token transfers, Parquet)
    ↓  download_eth_transfers.py
Raw token_transfers table
    ↓  prepare_tables.py / generate_2yr_data.py
address_flows_daily_dict (dictionary-encoded daily flows)
    +  entity_address_map_int (MBAL labels, integer-encoded)
    ↓  setup_views.sql (pluggable view layer)
Benchmark queries (Q02-Q06, Sankey multi-hop)
```

### Databases

| Database | Rows | Size | Use case |
|----------|-----:|-----:|----------|
| 3-month slim | 63-116M | 2 GB | Quick demo, fits RTX 6000 (24 GB) |
| 2-year full | 770M | 17 GB | Scaling benchmarks, needs A100 (40 GB) |

### Queries

| Query | Description | SQL pattern |
|-------|-------------|-------------|
| Q02 | Entity flow rollup (by date + token) | 2x LEFT JOIN + GROUP BY + ORDER BY LIMIT |
| Q03 | Top counterparty pairs | 2x INNER JOIN + GROUP BY + ORDER BY |
| Q04 | Time-series between two entities | 2x INNER JOIN + WHERE + GROUP BY |
| Q05 | Entity inflow/outflow balance | CTE + FULL OUTER JOIN |
| Q06 | Category flow matrix | 2x INNER JOIN + GROUP BY |
| Sankey | 3-hop entity trace | 3x (JOIN + GROUP BY) + UNION ALL + NOT IN |

All queries use integer-encoded join keys (V3 DICT). VARCHAR join keys
cause GPU fallback or ~100x slowdown.

### GPU Workarounds Baked In

| Issue | Workaround |
|-------|------------|
| VARCHAR join keys | Dictionary encoding (address → INT32 addr_id) |
| COALESCE | `CASE WHEN x IS NOT NULL THEN x ELSE y END` |
| DISTINCT | `GROUP BY col, COUNT(*) AS _c` |
| DECIMAL overflow | `CAST(SUM(...) AS DOUBLE)` |
| UNION ALL | Requires `feature/union-all-gpu-processing` branch |
| TopN with NULLs | Falls back to CPU — avoid ORDER BY LIMIT on nullable columns |

---

## 3. Validated Benchmark Results

### RTX 6000 (24 GB Turing, 2x Xeon Gold 6126)

**Dataset:** 228M rows, 6-month Ethereum flows (dictionary-encoded)
**Mode:** gpu_processing, warm cache, `gpu_buffer_init('10 GB', '10 GB')`

| Query | CPU (ms) | GPU (ms) | Speedup |
|-------|---:|---:|---:|
| Q02 | 106 | 43 | **2.5x** |
| Q03 | 93 | 38 | **2.4x** |
| Q04 | 62 | 28 | **2.2x** |
| Q05 | 207 | 80 | **2.6x** |
| Q06 | 100 | 38 | **2.6x** |

Scaling (3mo-12mo, correctness-validated):

| Slice | Rows | Speedup range |
|-------|-----:|:---:|
| 3 mo | 63M | 3.8-6.0x |
| 6 mo | 126M | 4.8-6.2x |
| 9 mo | 190M | 4.4-6.4x |
| 12 mo | 255M | 3.5-7.1x |
| 15 mo | 320M | OOM (8 GB work pool) |

### A100 (40 GB Ampere SXM4)

**Mode:** gpu_processing, warm cache, `gpu_buffer_init('18 GB', '18 GB')`

Scaling across 11 data slices (all correctness-validated):

| Slice | Rows | SANKEY | Q02 | Q03 | Q04 | Q05 | Q06 |
|-------|-----:|---:|---:|---:|---:|---:|---:|
| 3 mo | 63M | 3.9x | 2.9x | 3.6x | 4.0x | 2.9x | 5.4x |
| 6 mo | 126M | 4.9x | 5.0x | 4.5x | 6.0x | 3.0x | 4.2x |
| 12 mo | 255M | 5.5x | 4.0x | 3.7x | 5.4x | 3.1x | 4.0x |
| 21 mo | 503M | 4.0x | 4.5x | 4.4x | 7.8x | 3.6x | 4.1x |
| 24 mo | 619M | 3.8x | 3.5x | 4.1x | — | 3.4x | 3.4x |
| 27 mo | 770M | FALLBACK (all queries) |

**3-hop Sankey on A100 (specific timing):**

| Dataset | Rows | GPU (ms) | CPU (ms) | Speedup |
|---------|-----:|---:|---:|---:|
| 3mo (VARCHAR asset) | 116M | 22 | 107 | 4.9x |
| 2yr (INT asset) | 770M | 54 | 213 | 3.9x |
| 2yr (pre-aggregated) | 19M | 34 | 131 | 3.9x |

### gpu_execution Path (RTX 6000, with table_gpu caching)

| Query | CPU | gpu_execution (cached) |
|-------|---:|---:|
| Q03 | 93 ms | 121 ms |
| Q04 | 62 ms | 121 ms |
| Q06 | 100 ms | 141 ms |

gpu_execution is currently slower than CPU on warm runs due to pipeline
coordination overhead (~84ms gap). The `scan_cache_level = 'table_gpu'` setting
provides a 4x improvement over uncached gpu_execution.

---

## 4. GPU Capability Inventory

What Sirius can run on GPU for this workload:

| Operator | Status | Notes |
|----------|--------|-------|
| INNER JOIN | Supported | Integer keys only |
| LEFT JOIN | Supported | Integer keys only |
| FULL OUTER JOIN | Supported | Via `cudf::hash_join::full_join` |
| GROUP BY (int) | Supported | Fast path |
| GROUP BY (varchar) | Slow | ~100x overhead vs integer GROUP BY |
| ORDER BY + LIMIT | Supported | Fails with NULLs (TopN limitation) |
| CTE | Supported | Materialized |
| CASE WHEN | Supported | |
| UNION ALL | Supported | Requires feature branch |
| NOT IN (subquery) | Supported | Via MARK join |
| FILTER (WHERE/HAVING) | Supported | |

What falls back to CPU:

| Feature | Status |
|---------|--------|
| VARCHAR hash join | Falls back (type 12 error) |
| COALESCE | Not in expression translator |
| DISTINCT | Not supported (use GROUP BY) |
| EXCEPT / ANTI JOIN | Not supported |
| Recursive CTE | Falls back |
| SUBSTR / string ops | Deadlock on gpu_processing |
| TopN with NULL columns | Falls back |

---

## 5. How to Extend for Future TRM Requests

### If TRM wants us to benchmark new queries

1. Write the query against the `v_flows` / `v_entity_map` / `v_emap` views
   (defined in `setup_views.sql`). No table name changes needed.
2. Ensure all join keys are INTEGER (dictionary-encoded). If new columns need
   encoding, extend `address_dictionary` or create a new dictionary table.
3. Replace COALESCE → CASE WHEN, DISTINCT → GROUP BY, bare SUM → CAST AS DOUBLE.
4. Test on CPU first, then `CALL gpu_processing('...')` and check
   `tail log/sirius_*.log` for fallback messages.
5. Add to `scaling_validated.sh` for automated scaling + correctness testing.

### If TRM wants larger datasets

- **More chains**: Download Bitcoin/Polygon/Tron from the same AWS bucket.
  `download_eth_transfers.py` can be adapted (change S3 prefix).
- **Higher entity coverage**: MBAL covers ~0.3% of flows. TRM could provide
  a higher-coverage label set (under NDA) or we could supplement with
  Etherscan/Arkham public labels.
- **Longer time ranges**: `generate_2yr_data.py` supports arbitrary date
  ranges. For >27 months on A100 (>770M rows), need H100 (80 GB) or GH200 (96 GB).

### If TRM wants to evaluate against StarRocks

The query patterns are identical — swap `setup_views.sql` to point at
StarRocks tables via DuckDB's `postgres_scanner` or export parquet and load
into StarRocks. The queries themselves don't change.

### Known improvements that would help

| Improvement | Impact | Effort |
|-------------|--------|--------|
| Fix gpu_execution pipeline overhead | Make out-of-core competitive with warm cache | Medium (Sirius core) |
| Native COALESCE in expression translator | Remove CASE WHEN workaround | Small |
| VARCHAR hash join support | Remove dictionary encoding requirement | Large (cuDF limitation) |
| UNION ALL on dev branch | Remove feature branch requirement | PR in progress |
| Larger GPU (H100/GH200) | Handle 770M+ rows, show better scaling | Hardware |

---

## 6. Repo State and Reproducibility

### Files that matter

| File | Purpose |
|------|---------|
| `scripts/prepare_tables.py` | Build 3-month database from raw data |
| `scripts/generate_2yr_data.py` | Build 2-year database quarter-by-quarter |
| `scripts/scaling_validated.sh` | Run scaling benchmark with correctness checks |
| `queries/trm_pipeline/setup_views.sql` | Define data sources for all queries |
| `queries/trm_pipeline/q_sankey_multihop.sql` | Main demo query (3-hop Sankey) |
| `benchmark/sirius.cfg` | Reference GPU config (A100) |

### Branch requirements

- **dev**: All Q02-Q06 queries work. Sankey (UNION ALL) falls back to CPU.
- **feature/union-all-gpu-processing**: Full GPU support including Sankey.
  Build with `ENABLE_LEGACY_SIRIUS=ON`.

### Existing databases

- `data/crypto_demo_2025_slim.duckdb` (2.1 GB) — on JuiceFS, accessible from
  both A5000 and A100 machines
- `/tmp/crypto_2yr.duckdb` (17.3 GB) — on A100 local SSD only, must be
  regenerated if lost (`python scripts/generate_2yr_data.py`)
