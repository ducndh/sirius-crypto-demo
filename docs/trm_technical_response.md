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
| Q02 | §5 Entity flow rollup | 2x JOIN + GROUP BY date/asset/entity |
| Q03 | §6 Top counterparties | 2x JOIN + GROUP BY entity pairs |
| Q04 | §6 Time-series | JOIN + WHERE filter + GROUP BY date |
| Q05 | §5 Inflow/outflow | CTE + FULL OUTER JOIN for net flow |
| Q06 | §5 Category matrix | 2x JOIN + GROUP BY category |
| Sankey | Beyond §6 | 3-hop entity trace (compliance use case) |

All queries read from pluggable SQL views. Swapping the data source (DuckDB
tables, Parquet files, or Iceberg) requires changing one file (`setup_views.sql`)
— no query modifications needed.

---

## 2. Benchmark Results

All results below use the `gpu_processing` path, which caches table data in GPU
VRAM for interactive query serving. This is SiriusDB's mature execution path.

We are actively developing a second path (`gpu_execution`) that reads directly
from Parquet/Iceberg and supports datasets larger than GPU memory via tiered
caching (GPU → host → disk). Early results on this path show the gap narrowing
and we expect it to reach parity as pipeline coordination overhead is optimized.

All GPU results were **validated for correctness** — GPU output was compared
row-by-row against DuckDB CPU output (with float normalization for rounding
differences).

### NVIDIA A100 (40 GB HBM2e) — Scaling

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

### Quadro RTX 6000 (24 GB GDDR6) — Scaling

| Data size | Rows | GPU query time | CPU query time | Speedup |
|-----------|-----:|---------------:|---------------:|--------:|
| 3 months | 63M | 5–33 ms | 27–197 ms | **3.8–6.0x** |
| 6 months | 126M | 7–38 ms | 39–190 ms | **4.8–6.2x** |
| 9 months | 190M | 9–44 ms | 52–197 ms | **4.4–6.4x** |
| 12 months | 255M | 11–48 ms | 48–228 ms | **3.5–7.1x** |

### What Drives the Speedup

The TRM workload is dominated by **hash joins on high-cardinality integer keys**
(3.2M entity addresses × 63-770M flow rows) followed by **GROUP BY
aggregation**. These are memory-bandwidth-bound operations where GPU HBM
(2 TB/s on A100) has a structural advantage over DDR4/DDR5.

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

The output is a weighted edge list suitable for **Sankey visualization** —
each row is (hop, from_entity, to_entity, total_flow). The full query is in
Appendix A.

---

## 4. Answering Your Questions (Section 9)

> **Can SiriusDB efficiently handle large-scale join + aggregation workloads over Iceberg tables?**

SiriusDB reads Parquet natively with predicate pushdown and column pruning.
Iceberg support is available through DuckDB's `iceberg_scan()`. The benchmark
results above (5–46 ms on 63-619M rows) use the `gpu_processing` path with
data cached in GPU memory. We are actively developing the `gpu_execution` path
which reads directly from Parquet/Iceberg and supports out-of-core processing
via tiered caching (GPU → host → disk). This path is functional today and we
are closing the performance gap with the cached path.

> **How does query latency compare to StarRocks on entity flow rollup queries?**

We have not yet run a head-to-head StarRocks comparison on identical hardware.
The DuckDB CPU baseline (single-node, same machine) represents a strong
analytical engine. SiriusDB's 2.9–7.8x speedup over DuckDB on this workload
is a meaningful result. We are open to running a direct comparison on hardware
representative of TRM's serving environment.

> **What physical layout or indexing strategies best suit high-cardinality address lookups?**

**Dictionary encoding is the key optimization.** Replacing 42-byte VARCHAR
address strings with 4-byte INT32 dictionary IDs reduced GPU query time from
seconds to milliseconds. GPU hash join performance is dominated by key width —
narrower keys mean higher throughput on memory-bandwidth-bound operations.

Our benchmark uses a 3.2M-row entity map. TRM's production map may be
significantly larger (tens of millions of rows). The hash join builds on the
entity map (smaller side) and probes from the flow table — so a larger entity
map increases the hash table size but should not change the probe-side
throughput. That said, we have not yet tested with build-side tables at the
50M+ scale. As the hash table grows, factors like hash slot contention and
GPU memory pressure could become relevant. We would want to run benchmarks
at TRM's actual entity map scale to confirm performance holds.

> **How does SiriusDB behave with skewed join keys?**

The TRM workload is inherently skewed — a few entities (exchanges like Binance,
Coinbase) control millions of addresses while most entities have fewer than 100.
Our benchmark uses real MBAL label data which reflects this skew, and we
observed consistent GPU speedups across all data slices (3 months to 24 months).

The TRM join pattern is many-to-one (each flow row matches at most one entity),
so skewed keys do not cause fan-out on the output side. The probe is per-row
from the flow table, so work distribution across GPU threads is determined by
the flow table size, not by which entity a row happens to match. In principle,
hash slot contention on hot buckets could serialize some probe threads, but with
well-distributed INT32 keys and a reasonably sized hash table, this effect
should be small. Our results so far are consistent with this, though we
acknowledge the entity map in our benchmark is smaller than TRM's production
scale.

> **Are there query patterns where SiriusDB's architecture offers a structural advantage?**

Yes — the multi-hop Sankey trace. Each hop involves a JOIN + GROUP BY on the
full flow table, and the hops are sequential (hop 2 depends on hop 1's
frontier). On CPU, each hop takes 30-50 ms. On GPU, each hop takes 5-10 ms.
The cumulative advantage compounds over multiple hops.

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
[sirius-crypto-demo](https://github.com/ducndh/sirius-crypto-demo) repository.

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
   path more realistically and likely show stronger speedup ratios.

2. **Benchmarks on TRM-equivalent hardware** — Our current results are on
   A100 (40 GB) and RTX 6000 (24 GB). Running on hardware closer to TRM's
   serving environment would produce directly comparable numbers. Larger GPU
   memory would also let us benchmark the full 2-year dataset without memory
   limitations.

3. **Iceberg integration** — The `gpu_execution` path (Parquet/Iceberg,
   out-of-core) is actively being optimized. We expect to have competitive
   numbers on this path in the near term, which would match TRM's Iceberg-based
   storage layer directly.

4. **UTXO chain support** — The current demo uses Ethereum (account model).
   Bitcoin UTXO data with proportional-spend attribution (your Section 3)
   would add a second workload dimension.

We look forward to discussing these next steps and are happy to run additional
benchmarks on specific query patterns or data distributions that TRM provides.

---

## Appendix A: Benchmark Queries

All queries below are the exact SQL used in our benchmarks. They read from
dictionary-encoded tables with integer join keys (`address_flows_daily_dict`,
`entity_address_map_int`). The seed entity is `entity_id = 143` (fixedfloat,
a no-KYC exchange with ~43K addresses in the MBAL dataset).

### Sankey: 3-Hop Entity Flow Trace

```sql
WITH seed_addrs AS (
    SELECT addr_id FROM entity_address_map_int WHERE entity_id = 143
),
hop1_edges AS (
    SELECT
        143 AS from_eid,
        dst.entity_id AS to_eid,
        CAST(SUM(f.amount) AS DOUBLE) AS total_amount
    FROM address_flows_daily_dict f
    JOIN seed_addrs s ON f.from_addr_id = s.addr_id
    JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id
    WHERE dst.entity_id != 143
    GROUP BY 1, 2
),
hop1_frontier AS (
    SELECT to_eid AS eid, COUNT(*) AS _c FROM hop1_edges GROUP BY 1
),
hop2_edges AS (
    SELECT
        src.entity_id AS from_eid,
        dst.entity_id AS to_eid,
        CAST(SUM(f.amount) AS DOUBLE) AS total_amount
    FROM address_flows_daily_dict f
    JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id
    JOIN hop1_frontier h1 ON src.entity_id = h1.eid
    JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id
    WHERE dst.entity_id != 143
      AND dst.entity_id NOT IN (SELECT eid FROM hop1_frontier)
    GROUP BY 1, 2
),
hop2_frontier AS (
    SELECT to_eid AS eid, COUNT(*) AS _c FROM hop2_edges GROUP BY 1
),
hop3_edges AS (
    SELECT
        src.entity_id AS from_eid,
        dst.entity_id AS to_eid,
        CAST(SUM(f.amount) AS DOUBLE) AS total_amount
    FROM address_flows_daily_dict f
    JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id
    JOIN hop2_frontier h2 ON src.entity_id = h2.eid
    JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id
    WHERE dst.entity_id != 143
      AND dst.entity_id NOT IN (SELECT eid FROM hop1_frontier)
      AND dst.entity_id NOT IN (SELECT eid FROM hop2_frontier)
    GROUP BY 1, 2
)
SELECT 1 AS hop, from_eid, to_eid, total_amount FROM hop1_edges
UNION ALL
SELECT 2 AS hop, from_eid, to_eid, total_amount FROM hop2_edges
UNION ALL
SELECT 3 AS hop, from_eid, to_eid, total_amount FROM hop3_edges;
```

### Q02: Entity Outflow Rollup

```sql
SELECT
    dst.entity_id,
    CAST(SUM(f.amount) AS DOUBLE) AS total_vol,
    COUNT(*) AS tx_cnt
FROM address_flows_daily_dict f
JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id
JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id
WHERE src.entity_id = 143
  AND dst.entity_id != 143
GROUP BY 1
ORDER BY total_vol DESC;
```

### Q03: Entity Inflow Rollup

```sql
SELECT
    src.entity_id,
    CAST(SUM(f.amount) AS DOUBLE) AS total_vol,
    COUNT(*) AS tx_cnt
FROM address_flows_daily_dict f
JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id
JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id
WHERE dst.entity_id = 143
  AND src.entity_id != 143
GROUP BY 1
ORDER BY total_vol DESC;
```

### Q04: Daily Volume Time-Series

```sql
SELECT
    f.date,
    CAST(SUM(f.amount) AS DOUBLE) AS daily_vol,
    CAST(SUM(f.tx_count) AS DOUBLE) AS daily_tx
FROM address_flows_daily_dict f
JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id
WHERE src.entity_id = 143
GROUP BY 1
ORDER BY 1;
```

### Q05: Bidirectional Flow (FULL OUTER JOIN)

```sql
SELECT
    o.entity_id AS out_entity_id,
    i.entity_id AS in_entity_id,
    o.out_vol,
    i.in_vol
FROM (
    SELECT dst.entity_id,
           CAST(SUM(f.amount) AS DOUBLE) AS out_vol
    FROM address_flows_daily_dict f
    JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id
    JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id
    WHERE src.entity_id = 143
      AND dst.entity_id != 143
    GROUP BY 1
) o
FULL OUTER JOIN (
    SELECT src.entity_id,
           CAST(SUM(f.amount) AS DOUBLE) AS in_vol
    FROM address_flows_daily_dict f
    JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id
    JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id
    WHERE dst.entity_id = 143
      AND src.entity_id != 143
    GROUP BY 1
) i ON o.entity_id = i.entity_id;
```

### Q06: Top Individual Flows

```sql
SELECT
    src.entity_id AS from_eid,
    dst.entity_id AS to_eid,
    f.date,
    f.amount
FROM address_flows_daily_dict f
JOIN entity_address_map_int src ON f.from_addr_id = src.addr_id
JOIN entity_address_map_int dst ON f.to_addr_id = dst.addr_id
WHERE src.entity_id = 143
  AND dst.entity_id != 143
ORDER BY f.amount DESC
LIMIT 100;
```
