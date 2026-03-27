# Blockchain Counterparty Flow on SiriusDB: Technical Response

**Prepared by:** SiriusDB Research Team, University of Wisconsin–Madison
**Date:** March 2026
**In response to:** TRM Labs — Blockchain Counterparty Flow Computation: Technical Collaboration Brief

---

## 1. What We Did

We reproduced TRM's counterparty flow pipeline end-to-end using public data and
ran it through SiriusDB's GPU execution engine. The goal was to evaluate whether
SiriusDB can accelerate the join-heavy entity flow queries described in your
brief.

**Data sources:**
- **Ethereum token transfers** from [AWS Public Blockchain Data](https://registry.opendata.aws/aws-public-blockchain/) — up to 27 months (Jan 2024 – Mar 2026, 770M aggregated flow rows)
- **Entity address labels** from [MBAL 10M](https://www.kaggle.com/datasets/yidongchaintoolai/mbal-10m-crypto-address-label-dataset) — 3.2M Ethereum mainnet address→entity mappings

**Pipeline implemented:**
```
Raw Ethereum token_transfers (Parquet, AWS S3)
    ↓  Aggregate to daily grain
address_flows_daily (date, asset, from_address, to_address, amount)
    ↓  Join with entity_address_map (2x JOIN — sender + receiver)
entity_flows (date, asset, from_entity, to_entity, amount)
    ↓  3-hop Sankey trace (iterative frontier expansion)
Multi-hop entity flow graph (hop, from_entity, to_entity, total_amount)
```

This matches Sections 3-6 of your brief. We implemented six query patterns
covering the full TRM serving-layer workload:

| Query | Your brief section | What it does |
|-------|--------------------|--------------|
| Q02 | §5 Entity flow rollup | 2x LEFT JOIN + GROUP BY date/asset/entity |
| Q03 | §6 Top counterparties | 2x INNER JOIN + GROUP BY entity pairs |
| Q04 | §6 Time-series | INNER JOIN + WHERE filter + GROUP BY date |
| Q05 | §5 Inflow/outflow | CTE + FULL OUTER JOIN for net flow |
| Q06 | §5 Category matrix | 2x INNER JOIN + GROUP BY category |
| Sankey | Beyond §6 | 3-hop entity trace (compliance use case) |

All queries read from pluggable SQL views. Swapping the data source (DuckDB
tables, Parquet files, or Iceberg) requires changing one file (`setup_views.sql`)
— no query modifications needed.

---

## 2. Benchmark Results

We benchmarked on two GPU configurations against DuckDB CPU (single-node,
same machine). All GPU results were validated for correctness against CPU output.

### NVIDIA A100 (40 GB HBM2e) — Scaling

SiriusDB `gpu_processing` path, warm cache.

| Data size | Rows | GPU query time | CPU query time | Speedup |
|-----------|-----:|---------------:|---------------:|--------:|
| 3 months | 63M | 5–24 ms | 20–93 ms | **2.9–5.4x** |
| 6 months | 126M | 6–24 ms | 25–118 ms | **3.0–6.0x** |
| 12 months | 255M | 8–31 ms | 32–170 ms | **3.1–5.5x** |
| 21 months | 503M | 11–42 ms | 45–166 ms | **3.6–7.8x** |
| 24 months | 619M | 12–46 ms | 42–173 ms | **3.4–3.8x** |

Ranges span all six query patterns. The 3-hop Sankey query — the most complex
pattern, involving three rounds of JOIN + GROUP BY + frontier deduplication —
runs in **24 ms on 63M rows** and **46 ms on 619M rows**.

### Quadro RTX 6000 (24 GB GDDR6) — Detailed Q02-Q06

228M-row dataset (6-month Ethereum, dictionary-encoded):

| Query | CPU | GPU (warm) | Speedup |
|-------|----:|---:|--------:|
| Q02 Entity rollup | 106 ms | 43 ms | **2.5x** |
| Q03 Top pairs | 93 ms | 38 ms | **2.4x** |
| Q04 Timeseries | 62 ms | 28 ms | **2.2x** |
| Q05 Inflow/outflow (FULL OUTER JOIN) | 207 ms | 80 ms | **2.6x** |
| Q06 Category matrix | 100 ms | 38 ms | **2.6x** |

### What Drives the Speedup

The TRM workload is dominated by **hash joins on high-cardinality integer keys**
(3.2M entity addresses × 63-770M flow rows) followed by **GROUP BY
aggregation**. These are memory-bandwidth-bound operations where GPU HBM
(2 TB/s on A100) has a structural advantage over DDR4/DDR5.

The speedup scales with GPU memory bandwidth relative to CPU parallelism:
- RTX 6000 (900 GB/s, Turing): 2.2–2.6x
- A100 (2 TB/s, Ampere): 2.9–7.8x
- GH200 (4 TB/s, Grace Hopper): expected 6–8x (from TPC-H calibration)

---

## 3. Multi-Hop Compliance Trace (Sankey)

Beyond the entity flow rollup in your brief, we implemented a **3-hop
counterparty trace** — a compliance use case where you want to know: *"Which
entities did funds reach within 3 hops of a flagged entity?"*

This is computationally harder than single-hop rollup because naive path
enumeration is O(|E|^k) — at 3 hops on our dataset, that produces 4.1 billion
intermediate rows. We use **iterative frontier expansion** (BFS-style
semi-joins) which is O(|V| + |E|), reducing intermediate rows by 680x.

Each hop runs as a GPU-accelerated JOIN + GROUP BY. The full 3-hop trace
completes in **22 ms on GPU vs 107 ms on CPU** (A100, 116M rows).

```sql
-- Simplified structure (full query in queries/trm_pipeline/q_sankey_multihop.sql)
WITH seed_addrs AS (SELECT addr_id FROM entity_map WHERE entity_id = 143),
hop1_edges AS (
    SELECT 143 AS from_eid, dst.entity_id AS to_eid, SUM(f.amount) AS total
    FROM flows f
    JOIN seed_addrs s ON f.from_addr_id = s.addr_id
    JOIN entity_map dst ON f.to_addr_id = dst.addr_id
    WHERE dst.entity_id != 143
    GROUP BY 1, 2
),
-- hop2, hop3 expand frontier with NOT IN deduplication
...
SELECT * FROM hop1_edges UNION ALL hop2_edges UNION ALL hop3_edges;
```

The output is a weighted edge list suitable for **Sankey visualization** —
each row is (hop, from_entity, to_entity, total_flow).

---

## 4. Answering Your Questions (Section 9)

> **Can SiriusDB efficiently handle large-scale join + aggregation workloads over Iceberg tables?**

Yes. SiriusDB reads Parquet natively with predicate pushdown and column
pruning. Iceberg support is available through DuckDB's `iceberg_scan()`.
On the TRM workload (2x JOIN + GROUP BY over 63-619M rows), GPU query times
range from 5–46 ms (warm cache). The `gpu_execution` path supports
out-of-core processing for datasets larger than GPU memory via tiered caching
(GPU → host → disk).

> **How does query latency compare to StarRocks on entity flow rollup queries?**

We have not yet run a head-to-head StarRocks comparison on identical hardware.
The DuckDB CPU baseline (single-node, same machine) represents a strong
analytical engine. SiriusDB's 2.2–7.8x speedup over DuckDB on this workload
is a meaningful result. We are open to running StarRocks on the same queries
and hardware for a direct comparison.

> **What physical layout or indexing strategies best suit high-cardinality address lookups?**

**Dictionary encoding is the key optimization.** Replacing 42-byte VARCHAR
address strings with 4-byte INT32 dictionary IDs reduced GPU query time from
seconds to milliseconds. This is because GPU hash join performance is
dominated by key width — narrower keys mean more keys per cache line and
less memory bandwidth consumed.

For the entity address map (3.2M rows), the entire table fits in GPU L2 cache
on modern GPUs. No additional indexing is needed for broadcast-join patterns.

> **How does SiriusDB behave with skewed join keys?**

The TRM workload is inherently skewed — a few entities (exchanges like
Binance, Coinbase) control millions of addresses while most entities have
fewer than 100. SiriusDB handles this via cuDF's hash-based join
implementation, which distributes work across GPU threads by hash partition
rather than by key. We observed consistent performance across all data slices
including the skewed distribution.

> **Are there query patterns where SiriusDB's architecture offers a structural advantage?**

Yes — the multi-hop Sankey trace. Each hop involves a JOIN + GROUP BY on the
full flow table, and the hops are sequential (hop 2 depends on hop 1's
frontier). On CPU, each hop takes 30-50 ms. On GPU, each hop takes 5-10 ms.
The cumulative advantage compounds: 3 hops at 5x per hop gives a large wall-
clock speedup on the full trace.

More generally, SiriusDB is strongest on:
- **Hash joins on integer keys** against large fact tables
- **GROUP BY aggregation** with moderate cardinality (hundreds to millions of groups)
- **Repeated queries on cached data** (interactive analytics, dashboard serving)

---

## 5. SQL Compatibility

Operators exercised in the TRM benchmark:

| Operator | Status |
|----------|--------|
| INNER JOIN | Fully supported |
| LEFT JOIN | Fully supported |
| FULL OUTER JOIN | Fully supported |
| GROUP BY + SUM/COUNT | Fully supported |
| ORDER BY + LIMIT | Supported (avoid NULLable sort keys) |
| Common Table Expressions (CTE) | Fully supported |
| CASE WHEN expressions | Fully supported |
| NOT IN (subquery) | Supported (via MARK join) |
| UNION ALL | Supported (feature branch, merging to dev) |
| Parquet / DuckDB / Iceberg reads | Supported |
| DuckDB optimizer integration | Automatic LEFT → INNER rewrite |

Current workarounds (minor):
- `COALESCE(a, b)` → `CASE WHEN a IS NOT NULL THEN a ELSE b END`
- `SELECT DISTINCT x` → `SELECT x, COUNT(*) FROM ... GROUP BY x`

---

## 6. Reproducing These Results

All code, queries, and data download scripts are in our
[sirius-crypto-demo](https://github.com/sirius-db/sirius-crypto-demo) repository.

```bash
# 1. Download public data (~25 GB for 3 months)
python scripts/download_eth_transfers.py --start-date 2024-10-01 --end-date 2024-12-31
python scripts/download_mbal.py

# 2. Build database
python scripts/prepare_tables.py --year 2024

# 3. Run benchmark
bash scripts/scaling_validated.sh
```

No proprietary data or credentials are needed beyond a free Kaggle API token.
Full reproduction instructions are in the repository README.

---

## 7. Next Steps

We propose the following to deepen the evaluation:

1. **Higher entity coverage** — MBAL covers ~0.3% of flows. A higher-coverage
   label set (from TRM or supplementary sources) would stress the GPU join
   path more realistically and likely show even stronger speedup ratios.

2. **H100/GH200 benchmarks** — Our A100 results cap at 619M rows due to memory.
   H100 (80 GB) and GH200 (96 GB HBM3) would demonstrate scaling on the full
   2-year dataset and beyond.

3. **StarRocks head-to-head** — Run TRM's actual serving-layer queries on
   StarRocks and SiriusDB on the same hardware for a direct comparison.

4. **Iceberg integration test** — Validate the full pipeline reading directly
   from Iceberg tables (vs current Parquet/DuckDB) to match TRM's production
   storage layer.

5. **UTXO chain support** — The current demo uses Ethereum (account model).
   Bitcoin UTXO data with proportional-spend attribution (your Section 3)
   would add a second workload dimension.

We look forward to discussing these next steps and are happy to run additional
benchmarks on specific query patterns or data distributions that TRM provides.
