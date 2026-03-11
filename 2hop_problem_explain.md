# Why Multi-Hop Queries Explode — And How to Fix It

## Setup

**Data**: Real Ethereum transactions (AWS Public Blockchain Data)
- 51.3M edges, 19.3M vertices
- Source vertex: Binance hot wallet (`0x28c6c...d60`) — a hub with 265K direct neighbors

**Engine**: DuckDB 1.4.4, 64 vCPUs, 32GB memory limit

---

## The Two Queries

### Query A: Naive k-hop JOIN (path enumeration)

```sql
-- 2-hop
SELECT COUNT(*) FROM edges e1
JOIN edges e2 ON e1.dst = e2.src
WHERE e1.src = '0x28c6c06298d514db089934071355e5743bf21d60';

-- 3-hop
SELECT COUNT(*) FROM edges e1
JOIN edges e2 ON e1.dst = e2.src
JOIN edges e3 ON e2.dst = e3.src
WHERE e1.src = '0x28c6c06298d514db089934071355e5743bf21d60';
```

### Query B: Iterative semi-join (BFS semantics)

```sql
WITH hop0 AS (
    SELECT DISTINCT '0x28c6c06298d514db089934071355e5743bf21d60' AS vertex
),
hop1 AS (
    SELECT DISTINCT e.dst AS vertex
    FROM edges e JOIN hop0 ON e.src = hop0.vertex
    WHERE e.dst NOT IN (SELECT vertex FROM hop0)
),
hop2 AS (
    SELECT DISTINCT e.dst AS vertex
    FROM edges e JOIN hop1 ON e.src = hop1.vertex
    WHERE e.dst NOT IN (SELECT vertex FROM hop0)
      AND e.dst NOT IN (SELECT vertex FROM hop1)
),
hop3 AS (
    SELECT DISTINCT e.dst AS vertex
    FROM edges e JOIN hop2 ON e.src = hop2.vertex
    WHERE e.dst NOT IN (SELECT vertex FROM hop0)
      AND e.dst NOT IN (SELECT vertex FROM hop1)
      AND e.dst NOT IN (SELECT vertex FROM hop2)
)
SELECT 'hop0' AS hop, COUNT(*) AS cnt FROM hop0
UNION ALL SELECT 'hop1', COUNT(*) FROM hop1
UNION ALL SELECT 'hop2', COUNT(*) FROM hop2
UNION ALL SELECT 'hop3', COUNT(*) FROM hop3;
```

---

## Results

| Hops | Naive JOIN (paths) | Iterative Semi-Join (vertices) | Blowup Factor |
|------|--------------------|-------------------------------|----------------|
| 1    | 265,054            | 265,054                       | 1x             |
| 2    | **4,097,821**      | **1,909,232**                 | 2.1x           |
| 3    | **4,085,659,463**  | **6,013,616**                 | **680x**       |

| Hops | Naive JOIN Time | Iterative Semi-Join Time | Speedup  |
|------|-----------------|--------------------------|----------|
| 2    | 0.18s           | 0.33s                    | naive wins (small output) |
| 3    | **15.2s**       | **0.91s**                | **17x faster** |

---

## Query Plans

### Naive 3-hop (Query A)

```
UNGROUPED_AGGREGATE (count_star)
  └── HASH_JOIN (e2.dst = e3.src)        ← 2nd expansion: 4.1M × avg_degree → 4.1B rows
        ├── SEQ_SCAN edges               ← full 51M edge scan
        └── HASH_JOIN (e1.dst = e2.src)  ← 1st expansion: 265K × avg_degree → 4.1M rows
              ├── SEQ_SCAN edges         ← full 51M edge scan
              └── SEQ_SCAN edges         ← filtered to source: 265K rows
                    (src = '0x28c6c...')
```

Each HASH_JOIN expands every intermediate row by the out-degree of its destination vertex.
The intermediate result between the two joins is **4.1M rows** (all 2-hop paths).
The final output is **4.1 billion rows** (all 3-hop paths).
There is no deduplication — if vertex B is reachable via 100 different 2-hop paths,
it generates 100 × out_degree(B) rows at hop 3.

### Iterative Semi-Join (Query B)

```
CTE hop0 (1 row)
  └── CTE hop1
        └── HASH_GROUP_BY (DISTINCT)     ← deduplicate to unique vertices
              └── FILTER (NOT IN hop0)   ← anti-join: skip already-visited
                    └── HASH_JOIN MARK   ← mark join against hop0
                          └── HASH_JOIN INNER (src = vertex)
                                ├── SEQ_SCAN edges
                                └── CTE_SCAN hop0
  └── CTE hop2
        └── HASH_GROUP_BY (DISTINCT)     ← deduplicate to unique vertices
              └── FILTER (NOT IN hop1)   ← anti-join: skip hop1 vertices
                    └── FILTER (NOT IN hop0)  ← anti-join: skip hop0 vertices
                          └── HASH_JOIN INNER (src = vertex)
                                ├── SEQ_SCAN edges
                                └── CTE_SCAN hop1
  └── CTE hop3
        └── HASH_GROUP_BY (DISTINCT)
              └── FILTER (NOT IN hop2)
                    └── FILTER (NOT IN hop1)
                          └── FILTER (NOT IN hop0)
                                └── HASH_JOIN INNER (src = vertex)
                                      ├── SEQ_SCAN edges
                                      └── CTE_SCAN hop2
```

Each CTE materializes only the **unique new vertices** at that hop distance.
The DISTINCT + NOT IN filters mean:
- hop1 produces 265K vertices (not paths)
- hop2 produces 1.9M vertices (excluding hop0 and hop1)
- hop3 produces 6.0M vertices (excluding hop0, hop1, hop2)
Total across all hops: 8.2M vertices. Compare to 4.1B paths from the naive approach.

---

## Why It Explodes

The fan-out from the Binance hot wallet:

| Hop | Unique Vertices with Outgoing Edges | Total Edges to Next Hop | Avg Out-Degree | Max Out-Degree |
|-----|-------------------------------------|-------------------------|----------------|----------------|
| 1   | 244,890                             | 4,097,821               | 16.7           | **289,683**    |
| 2   | 1,813,801                           | 16,521,973              | 9.1            | —              |

The max out-degree of 289,683 at hop 1 is the problem. One hub vertex at hop 1 fans out to
290K edges. In the naive JOIN, every path *to* that hub gets multiplied by 290K. If 50 paths
reach that hub at hop 2, the naive approach produces 50 × 290K = 14.5M rows from that single
vertex. The iterative approach produces exactly 1 row for that vertex (it's already visited).

**The fundamental difference:**
- Naive JOIN: O(|paths|) = O(|E|^k) for k hops
- Iterative semi-join: O(|V| + |E|) total — each vertex and edge visited at most once

This is an asymptotic difference, not a constant factor. On real transaction networks, the naive approach is exponential in hop count since we have centralized exchange and disperse effort.

---

## Why DuckDB's Recursive CTE Can't Do Correct Bounded BFS

The best recursive CTE attempt is:

```sql
WITH RECURSIVE bfs AS (
    SELECT '0x28c6c...' AS vertex, 0 AS dist
    UNION
    SELECT e.dst, b.dist + 1
    FROM edges e JOIN bfs b ON e.src = b.vertex
    WHERE b.dist < 3
)
SELECT vertex, MIN(dist) AS dist FROM bfs GROUP BY vertex;
```

The `MIN(dist)` post-processing gives **correct final output**. But the recursive CTE
still internally materializes **10.4M rows** for only **8.2M unique vertices** (27%
overhead at 3-hop), and takes **19.1s** vs the manual CTE's **0.91s** (21x slower).

The problem is that `UNION` deduplicates on the full row `(vertex, dist)`. A vertex found
at dist=1 and again at dist=2 produces `(vertex, 1)` and `(vertex, 2)` — different tuples,
so UNION keeps both. The recursive step re-expands already-visited vertices at each
iteration because it doesn't know they were already found at an earlier distance.

The root cause: the recursive CTE cannot push the visited-set anti-join **into** the
recursive step. It always does: expand all neighbors first → deduplicate after. The manual
CTE filters out already-visited vertices **before** the join expansion at each hop — this
is "recursive predicate pushdown" and it's why the manual CTE is 21x faster with zero
intermediate bloat.

---

## GPU Execution via `gpu_processing` (Sirius)

The iterative BFS can run on GPU today using `gpu_processing` with super hacky things below here

```sql
-- Hop 1 (GPU: filter + group by)
CREATE TABLE hop1 AS
SELECT * FROM gpu_processing('
    SELECT dst AS vertex FROM edges
    WHERE src = ''0x28c6c...'' GROUP BY dst
');

-- Hop 2 (GPU: join + group by)
CREATE TABLE hop2_raw AS
SELECT * FROM gpu_processing('
    SELECT e.dst AS vertex FROM edges e
    JOIN hop1 ON e.src = hop1.vertex GROUP BY e.dst
');
-- CPU anti-join (cheap set subtraction)
CREATE TABLE hop2 AS
SELECT vertex FROM hop2_raw EXCEPT SELECT vertex FROM hop0 EXCEPT SELECT vertex FROM hop1;

-- Hop 3 (GPU: join + group by)
CREATE TABLE hop3_raw AS
SELECT * FROM gpu_processing('
    SELECT e.dst AS vertex FROM edges e
    JOIN hop2 ON e.src = hop2.vertex GROUP BY e.dst
');
CREATE TABLE hop3 AS
SELECT vertex FROM hop3_raw
EXCEPT SELECT vertex FROM hop0 EXCEPT SELECT vertex FROM hop1 EXCEPT SELECT vertex FROM hop2;
```

**Results** (51M edges, 19M vertices, RTX 6000 24GB):

| Step | Operation | Time | Engine |
|------|-----------|------|--------|
| hop1 | filter + GROUP BY | 1.76s | GPU |
| hop2 | JOIN + GROUP BY | 0.52s | GPU |
| hop2 anti-join | EXCEPT | 0.36s | CPU |
| hop3 | JOIN + GROUP BY | 1.74s | GPU |
| hop3 anti-join | EXCEPT | 0.86s | CPU |
| **Total** | | **~5.2s** | |

For comparison, the pure CPU CTE approach takes **0.91s** for the same 3-hop BFS. The GPU
version is infinitly slower here since we don't have an anti join yet and VARCHAR GROUP BY is super expensive.

Some reasoning on this hacky 
- `SELECT DISTINCT` not supported — use `GROUP BY` instead
- `NOT IN (subquery)` not supported — use CPU `EXCEPT` or temp table subtraction

---

## Implications for Sirius

### Fix `gpu_processing` anti-join support (if we move onto gpu_execution, this might be redundant work)
Add `NOT IN` subquery support or `LEFT JOIN ... IS NULL` to the GPU executor. This would
let the full BFS run on GPU in a single `gpu_processing` call per hop, eliminating the
CPU EXCEPT roundtrip. If this is fast, we can guide the user to write n-hop query by keep copy pasting their query again and again.

### What I have already implemented
The `gpu_graph_bfs()` table function via cuGraph worker does this in 28-108ms (hot).
BFS runs natively in CUDA with a visited bitmask but we spawn a whole cugraph process inside our GPU

### The best option for n-hop
Implement scatter-gather over CSR adjacency directly in Sirius's CUDA context (~200 lines
for BFS). The same primitive handles all other graph algorithms:

```
frontier = {source vertices}
visited = {source vertices}
for each hop:
    next_frontier = DISTINCT(neighbors(frontier)) - visited
    visited = visited ∪ next_frontier
    frontier = next_frontier
```

This eliminates the cuGraph subprocess, avoids per-hop SQL query, and keeps memory
bounded by O(|V|) regardless of graph structure.
