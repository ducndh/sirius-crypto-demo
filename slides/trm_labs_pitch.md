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
    (familiar SQL)       (memory bandwidth)     (graph, via CUDA IPC)
```

- **Same SQL you already write** — no new query language
- **Up to 350x faster** on analytical workloads
- **Graph analytics built in** — PageRank, BFS, WCC from SQL

---

## How It Works

```
┌─────────────────────────────────────────────────────┐
│                     Sirius                          │
│  ┌──────────┐    ┌─────────────┐   ┌─────────────┐ │
│  │  DuckDB  │    │    cuDF     │   │  cuGraph    │ │
│  │  Planner │ →  │  Columnar   │ → │  (via IPC)  │ │
│  │  (SQL)   │    │  GPU Exec   │   │             │ │
│  └──────────┘    └─────────────┘   └─────────────┘ │
│              All in GPU Memory (24GB / 80GB)        │
└─────────────────────────────────────────────────────┘
        ↑ Load once           ↓ Query results (ms)
   [ Parquet files ]      [ Analyst / API ]
```

vs. today: SQL → export → Python → NetworkX → results *(3 systems, 2 copies)*

---

## Benchmark Setup

- **Hardware**: NVIDIA Quadro RTX 6000 (24GB) — *scales to H100 80GB*
- **Data**: Real Ethereum transactions — AWS Public Blockchain Data
  - 10M+ transactions, 8M+ token transfers
  - Real ETH/USD prices (CoinGecko hourly)
- **Baseline**: DuckDB v1.4.4 — the fastest single-node CPU analytics engine
- **Queries**: 10 analytics + 3 graph queries

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

*Matching 10M transactions to hourly ETH/USD prices (RTX 6000)*

| Query | Description | CPU (DuckDB) | Sirius GPU | Speedup |
|-------|-------------|:---:|:---:|:---:|
| Q04 | All txns → USD value | 24.0s | 67ms | **358x** |
| Q05 | Daily USD volume | 24.1s | 149ms | **162x** |

> "Price-matched analytics in milliseconds. No ETL pipeline needed."

---

## Analytics Results

*Scan, filter, aggregation on 10M transactions (RTX 6000)*

| Query | Description | CPU (DuckDB) | Sirius GPU | Speedup |
|-------|-------------|:---:|:---:|:---:|
| Q03 | Top 100 addresses by volume | 1,186ms | 266ms | **4.5x** |
| Q08 | Hourly gas price trends | 248ms | 206ms | **1.2x** |
| Q10 | Block-level statistics | 161ms | 83ms | **1.9x** |

Expect **10–50x** on H100 80GB with 200M+ rows (memory bandwidth scales).

---

## Graph Analytics — From SQL

**cuGraph integration brings graph analytics directly to SQL.**

No data export. No Python round-trip. Results via CUDA IPC.

```sql
-- PageRank: find most important addresses
SELECT * FROM gpu_graph_pagerank('tx_edges', 'src', 'dst')
ORDER BY pagerank DESC LIMIT 20;

-- BFS: trace transactions from a flagged address (3 hops)
SELECT * FROM gpu_graph_bfs('tx_edges', 'src', 'dst',
    '0x75e89d...', 3) ORDER BY distance LIMIT 50;

-- Connected Components: cluster addresses into entities
SELECT component, COUNT(*) AS size
FROM gpu_graph_wcc('tx_edges', 'src', 'dst')
GROUP BY 1 ORDER BY 2 DESC;
```

---

## The Killer Query

```
"Find all addresses within 3 hops of a sanctioned entity
 that received >$10K USD in the last 30 days"
```

```sql
-- Step 1: BFS from sanctioned address
CREATE TABLE suspects AS
  SELECT vertex FROM gpu_graph_bfs('tx_edges','src','dst','0xSanctioned',3);

-- Step 2: ASOF JOIN + filter by USD amount and date
CALL gpu_processing('
  SELECT t.to_address, SUM(t.value/1e18 * p.price_usd) AS usd
  FROM eth_transactions t
  ASOF JOIN prices p ON t.block_timestamp >= p.ts
  WHERE t.block_timestamp >= ''2024-02-01''
    AND t.to_address IN (SELECT vertex FROM suspects)
  GROUP BY 1 HAVING SUM(t.value/1e18 * p.price_usd) > 10000
  ORDER BY 2 DESC
');
```

**Graph + ASOF JOIN + aggregation — all on one GPU, seconds total.**

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

*Loading 10M real Ethereum transactions into GPU memory...*

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
- cuGraph: PageRank, BFS, WCC today — Louvain, triangle count next

---

<!-- _class: title -->

# Questions?

<br>

*sirius-db.github.io*

---

## Appendix: cuGraph Algorithms

| Algorithm | Status | SQL Function | TRM Use Case |
|-----------|:------:|------------|------------|
| PageRank | **Live** | `gpu_graph_pagerank()` | Risk scoring by centrality |
| BFS | **Live** | `gpu_graph_bfs()` | Transaction tracing |
| Connected Components | **Live** | `gpu_graph_wcc()` | Entity clustering |
| Louvain / Leiden | Roadmap | — | Community detection |
| Triangle Count | Roadmap | — | Cyclic tx detection |

---

## Appendix: Architecture

```
┌────────────────── Sirius Process ──────────────────┐
│  DuckDB planner → cuDF execution (GPU VRAM)        │
│       ↕ edge table via binary pipe                  │
│  ┌──────────────── cuGraph Worker ───────────────┐  │
│  │  Python subprocess (fork+exec)                │  │
│  │  Builds CSR graph → runs cuGraph algorithm    │  │
│  │  Returns results via CUDA IPC (zero-copy GPU) │  │
│  └───────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

- **Why out-of-process?** cuGraph and cuDF have CUDA context conflicts
- **Why CUDA IPC?** Results stay on GPU — no D2H → H2D round-trip
- **Overhead:** ~100ms subprocess startup, amortized over algorithm runtime
