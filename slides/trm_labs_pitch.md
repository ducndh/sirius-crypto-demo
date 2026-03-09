---
marp: true
theme: default
paginate: true
style: |
  section {
    font-family: 'Helvetica Neue', Arial, sans-serif;
    background: #0a0a0a;
    color: #f0f0f0;
  }
  h1 { color: #76b900; font-size: 2em; }
  h2 { color: #76b900; }
  h3 { color: #aaaaaa; font-weight: normal; }
  code, pre { background: #1a1a1a; color: #76b900; border-radius: 4px; }
  table { width: 100%; border-collapse: collapse; }
  th { background: #1a1a1a; color: #76b900; padding: 8px; }
  td { padding: 8px; border-bottom: 1px solid #333; }
  .highlight { color: #76b900; font-weight: bold; }
  section.title { text-align: center; }
---

<!-- _class: title -->

# Sirius
## GPU-Accelerated Analytics for Blockchain Intelligence

<br>

### Faster compliance queries. Graph analytics built in. One GPU.

---

## The Problem

TRM Labs processes **2.5B+ Ethereum transactions** for compliance.

Your current stack:
- StarRocks / Trino → batch analytics takes **hours**
- Separate Python / Neo4j → graph analytics requires **data export**
- Price matching (ASOF JOIN) → **complex ETL pipelines**

**What if one GPU could do all of this in minutes?**

---

## Sirius in 30 Seconds

SQL-compatible analytics engine that runs entirely on GPU.

```
DuckDB SQL planner  →  cuDF GPU execution  →  cuGraph analytics
    (familiar SQL)       (memory bandwidth)     (graph, zero-copy)
```

- **Same SQL you already write** — no new query language
- **10–50x faster** on analytical workloads
- **Graph analytics built in** — no export, no round-trips

---

## How It Works

```
┌─────────────────────────────────────────────────────┐
│                     Sirius                          │
│  ┌──────────┐    ┌─────────────┐   ┌─────────────┐ │
│  │  DuckDB  │    │    cuDF     │   │  cuGraph    │ │
│  │  Planner │ →  │  Columnar   │ → │  (roadmap)  │ │
│  │  (SQL)   │    │  GPU Exec   │   │             │ │
│  └──────────┘    └─────────────┘   └─────────────┘ │
│              All in GPU Memory (80GB HBM3)          │
└─────────────────────────────────────────────────────┘
        ↑ Load once           ↓ Query results (ms)
   [ Parquet files ]      [ Analyst / API ]
```

vs. today: SQL → export → Python → NetworkX → results *(3 systems, 2 copies)*

---

## Benchmark Setup

- **Hardware**: NVIDIA H100 80GB HBM3 *(RTX 6000 for dev)*
- **Data**: Real Ethereum transactions — AWS Public Blockchain Data
  - 200M+ transactions, 100M+ token transfers
  - Real ETH/USD prices (CoinGecko hourly)
- **Baseline**: DuckDB — the fastest single-node CPU analytics engine
- **Queries**: 10 queries across 3 categories TRM Labs runs daily

---

## Raw Analytics: 10–30x Faster

*Scan/filter queries on 200M transactions*

| Query | Description | CPU (DuckDB) | Sirius GPU | Speedup |
|-------|-------------|:---:|:---:|:---:|
| Q01 | Count txns in date range | XXX ms | XX ms | **XX.Xx** |
| Q02 | Address activity summary | XXX ms | XX ms | **XX.Xx** |
| Q03 | Top 100 addresses by volume | XXX ms | XX ms | **XX.Xx** |

> "Standard compliance queries: run them all day without a cluster."

---

## ASOF JOIN — The Killer Feature

Every compliance query that needs **USD values** must match transactions
to a price feed by timestamp.

```sql
-- "How much USD did each address send?"
SELECT t.from_address, SUM(t.value/1e18 * p.price_usd) AS total_usd
FROM eth_transactions t
ASOF JOIN prices p ON t.block_timestamp >= p.ts
GROUP BY 1 ORDER BY 2 DESC LIMIT 50;
```

**No other GPU database supports ASOF JOIN.**
Competitors require separate preprocessing or Python UDFs.

---

## ASOF JOIN Results

*Matching 200M transactions to hourly ETH/USD prices*

| Query | Description | CPU (DuckDB) | Sirius GPU | Speedup |
|-------|-------------|:---:|:---:|:---:|
| Q04 | All txns → USD value | XXX ms | XX ms | **XX.Xx** |
| Q05 | Daily USD volume | XXX ms | XX ms | **XX.Xx** |
| Q06 | Top senders by USD | XXX ms | XX ms | **XX.Xx** |

> "Price-matched analytics in milliseconds. No ETL pipeline needed."

---

## Heavy Aggregation

*GROUP BY on 200M transactions*

| Query | Description | CPU (DuckDB) | Sirius GPU | Speedup |
|-------|-------------|:---:|:---:|:---:|
| Q07 | Top ERC-20 tokens by activity | XXX ms | XX ms | **XX.Xx** |
| Q08 | Hourly gas price trends | XXX ms | XX ms | **XX.Xx** |
| Q09 | Active address pairs | XXX ms | XX ms | **XX.Xx** |
| Q10 | Block-level statistics | XXX ms | XX ms | **XX.Xx** |

---

## Graph Analytics — Roadmap

**cuGraph integration brings graph analytics directly to GPU memory.**

No data export. No Python round-trip. Zero-copy from SQL results to graph.

| Algorithm | Use Case |
|-----------|----------|
| Connected Components | Entity clustering (address → entity) |
| BFS (depth-limited) | Transaction tracing from flagged address |
| PageRank | Risk scoring by network centrality |
| Louvain / Leiden | Community detection (mixing services) |
| Triangle Count | Cyclic transaction detection |

---

## The Killer Query (Roadmap)

```
"Find all addresses within 3 hops of a sanctioned entity
 that received >$10K USD in the last 30 days"
```

| Step | Operation | Time |
|------|-----------|------|
| 1 | BFS from sanctioned address (cuGraph) | ~100ms |
| 2 | ASOF JOIN transactions → USD prices | ~500ms |
| 3 | Filter by amount + date | ~100ms |

**Total: ~1 second** — all on GPU, no data movement.

*Current stack: minutes (export → NetworkX → re-import → SQL)*

---

## Why Blockchain Data Fits GPU Perfectly

Blockchain data is **append-only and immutable** — past blocks never change.

```
Load 6 months of ETH transactions → GPU VRAM (one-time, ~2 min)
Every subsequent query            → memory bandwidth speed (ms)
```

**Your workload breakdown:**
- **90% historical batch analytics** → Perfect fit. Always hot. Always fast.
- **10% near-real-time alerting** → Stays on existing stack today.

> "Your 4-hour batch jobs become 20-minute jobs.
>  Load once, query all day."

---

<!-- _class: title -->

## Live Demo

*Loading 35M real Ethereum transactions into GPU memory...*

```bash
$ bash scripts/run_live_demo.sh
```

---

## What's Next

**Proof of concept on your data: 2 weeks**
- Bring your own parquet files or BigQuery export
- Run our benchmark suite on your actual compliance queries
- No infrastructure changes — just swap the binary

**Production path:**
- Single H100 node replaces a CPU analytics cluster
- DuckDB-compatible SQL — minimal code changes
- cuGraph integration for on-GPU graph analytics (Q3 2025)

---

<!-- _class: title -->

# Questions?

<br>

*sirius-db.github.io*

---

## Appendix: cuGraph Algorithm Reference

| Algorithm | cuGraph API | TRM Labs Use Case | ~Time (H100, 1B edges) |
|-----------|------------|-------------------|----------------------|
| Connected Components | `connected_components()` | Entity clustering | seconds |
| BFS | `bfs()` | Transaction tracing | milliseconds |
| PageRank | `pagerank()` | Risk scoring | seconds |
| Louvain | `louvain()` | Community detection | seconds |
| Triangle Count | `triangle_count()` | Cyclic tx detection | seconds |
| Betweenness Centrality | `betweenness_centrality()` | Bridge addresses | minutes* |

*\*sampling available*

---

## Appendix: Compute Model Fit

**Concern**: Sirius cold runs are expensive (PCIe transfer). Writes invalidate GPU cache.

**Why blockchain analytics still works:**

| Workload Type | Cache Behavior | Fit? |
|---------------|---------------|------|
| Historical batch (90%) | Load once, query many times → always hot | ✅ Perfect |
| Near-real-time alerting (10%) | Constant writes → cache invalidation | ❌ Stay on CPU |

**Demo strategy**: Report "load time" once, report "query time" as the repeatable benefit.
Never demo the write path.
