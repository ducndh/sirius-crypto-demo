# TRM Labs Crypto Analytics — Build & Demo Plan

**Goal**: Build a fully working end-to-end demo on RTX 6000 (small data) that can be
re-run on H100 (full data) with zero code changes. Deliverables: live interactive demo
+ slide deck for the TRM Labs pitch.

**Hardware targets**:
- **Dev/test (now)**: RTX 6000 24GB VRAM, `/home/cc/sirius-asof`
- **Pitch (later)**: H100 80GB VRAM — same scripts, bigger data

---

## Phase 0: Real Data Acquisition

**Use real blockchain data.** Synthetic data is not credible for a pitch. Real Ethereum
transactions have power-law address distributions, actual gas prices, real block patterns
— things synthetic `hash(i) % 100000` can never replicate.

### Data Source 1: AWS Public Blockchain Data (Ethereum Transactions)

**Free, no account needed, already in Parquet, partitioned by date.**

```
S3 bucket: s3://aws-public-blockchain/v1.0/eth/
Access:    --no-sign-request (public, anonymous)
Format:    Compressed Parquet, partitioned by date
Updated:   Daily
```

#### Step 0.1: Install AWS CLI (if not present)
```bash
# Check if available
aws --version
# If not: pip install awscli  (or: sudo apt install awscli)
```

#### Step 0.2: Explore the bucket structure
```bash
# List top-level directories
aws s3 ls --no-sign-request s3://aws-public-blockchain/v1.0/eth/

# Expected structure:
#   blocks/
#   contracts/
#   logs/
#   token_transfers/
#   traces/
#   transactions/

# List transaction partitions (by date)
aws s3 ls --no-sign-request s3://aws-public-blockchain/v1.0/eth/transactions/ | head -20

# Check size of one day's data
aws s3 ls --no-sign-request --summarize --human-readable \
    s3://aws-public-blockchain/v1.0/eth/transactions/date=2024-01-15/
```

#### Step 0.3: Download transaction data

The script `scripts/download_eth_data.sh` should download real Ethereum data.
Scale is controlled by how many days you download.

```bash
#!/bin/bash
# Download real Ethereum transaction data from AWS Public Blockchain
set -e

SCALE="${1:-dev}"       # dev | rtx6000 | h100
DATA_DIR="${2:-data/raw/eth_transactions}"
S3_BASE="s3://aws-public-blockchain/v1.0/eth"

mkdir -p "$DATA_DIR"

case "$SCALE" in
  dev)
    # 7 days ≈ ~8-10M transactions (perfect for pipeline testing)
    START="2024-01-01"; END="2024-01-07"
    ;;
  rtx6000)
    # 30 days ≈ ~35-40M transactions (fits 24GB VRAM)
    START="2024-01-01"; END="2024-01-31"
    ;;
  h100)
    # 180 days ≈ ~200M+ transactions (fits 80GB VRAM)
    START="2024-01-01"; END="2024-06-30"
    ;;
  h100-max)
    # 365 days ≈ ~400M+ transactions
    START="2024-01-01"; END="2024-12-31"
    ;;
esac

echo "Downloading ETH transactions: $START to $END → $DATA_DIR"

# Download transactions
current="$START"
while [[ "$current" < "$END" ]] || [[ "$current" == "$END" ]]; do
  echo "  Downloading date=$current ..."
  aws s3 cp --no-sign-request --recursive \
    "${S3_BASE}/transactions/date=${current}/" \
    "${DATA_DIR}/date=${current}/" \
    --quiet
  current=$(date -d "$current + 1 day" +%Y-%m-%d)
done

# Also download token_transfers for the same range
TOKEN_DIR="${2:-data/raw/token_transfers}"
mkdir -p "$TOKEN_DIR"
echo "Downloading token transfers: $START to $END → $TOKEN_DIR"

current="$START"
while [[ "$current" < "$END" ]] || [[ "$current" == "$END" ]]; do
  echo "  Downloading date=$current ..."
  aws s3 cp --no-sign-request --recursive \
    "${S3_BASE}/token_transfers/date=${current}/" \
    "${TOKEN_DIR}/date=${current}/" \
    --quiet
  current=$(date -d "$current + 1 day" +%Y-%m-%d)
done

echo "Done. Check disk usage:"
du -sh "$DATA_DIR" "$TOKEN_DIR"
```

#### Step 0.4: Verify downloaded data
```bash
# Quick check with DuckDB
duckdb -c "
  SELECT COUNT(*) AS num_transactions,
         MIN(block_timestamp) AS first_ts,
         MAX(block_timestamp) AS last_ts,
         COUNT(DISTINCT from_address) AS unique_senders
  FROM 'data/raw/eth_transactions/**/*.parquet';
"
```

### Data Source 2: Price Data (CoinGecko / CryptoDataDownload)

For ASOF JOIN, we need historical crypto prices at minute or hourly granularity.

#### Option A: CryptoDataDownload (easiest — bulk CSV, no API key)
```bash
# ETH hourly prices (free, no auth)
# Visit https://www.cryptodatadownload.com/data/binance/ and download:
#   Binance_ETHUSDT_1h.csv
# Or use wget if direct link is available:
mkdir -p data/raw/prices
# Download and convert to parquet using DuckDB:
duckdb -c "
  COPY (
    SELECT
      date AS ts,
      'ETH' AS symbol,
      close AS price_usd
    FROM read_csv_auto('data/raw/prices/Binance_ETHUSDT_1h.csv', header=true)
    WHERE date >= '2024-01-01' AND date <= '2024-12-31'
  ) TO 'data/prices.parquet' (FORMAT PARQUET);
"
```

#### Option B: CoinGecko API (free tier, 365 days history)
```python
# scripts/download_prices.py
import requests, json, time
import duckdb

coins = {
    'ethereum': 'ETH',
    'bitcoin': 'BTC',
    'tether': 'USDT',
    'usd-coin': 'USDC',
    'dai': 'DAI',
    'chainlink': 'LINK',
    'uniswap': 'UNI',
    'wrapped-bitcoin': 'WBTC',
}

all_rows = []
for coin_id, symbol in coins.items():
    print(f"Fetching {symbol}...")
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": 365, "interval": "hourly"}  # free tier: hourly
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    prices = resp.json()["prices"]  # [[timestamp_ms, price], ...]
    for ts_ms, price in prices:
        all_rows.append({"ts": ts_ms // 1000, "symbol": symbol, "price_usd": price})
    time.sleep(6)  # Rate limit: 10-15 calls/min on free tier

# Write to parquet via DuckDB
con = duckdb.connect()
con.execute("CREATE TABLE prices AS SELECT * FROM all_rows")
con.execute("""
    COPY (
        SELECT epoch_ms(ts * 1000) AS ts, symbol, price_usd
        FROM prices ORDER BY symbol, ts
    ) TO 'data/prices.parquet' (FORMAT PARQUET)
""")
print(f"Wrote {len(all_rows)} price rows to data/prices.parquet")
```

#### Option C: CoinGecko CSV Download (manual, no code)
Go to https://www.coingecko.com/en/coins/ethereum/historical_data, click Export,
download CSV. Repeat for BTC, USDT, etc. Convert to parquet with DuckDB.

### Step 0.5: Consolidate data into demo-ready parquet files

Create `scripts/prepare_demo_data.py` that:
1. Reads the raw partitioned parquet from `data/raw/`
2. Selects/renames columns to match the demo query schema
3. Writes consolidated single parquet files to `data/`

```python
# scripts/prepare_demo_data.py
import duckdb
import os

con = duckdb.connect()

# --- ETH Transactions ---
# AWS schema may differ slightly — inspect and adapt column names
print("Inspecting raw ETH transactions schema...")
con.execute("DESCRIBE SELECT * FROM 'data/raw/eth_transactions/**/*.parquet' LIMIT 0").show()

# Expected AWS columns: hash, block_timestamp, from_address, to_address, value, gas_price, block_number, ...
# Adapt column names if needed (e.g., 'gas' vs 'gas_price')
print("Consolidating eth_transactions...")
con.execute("""
    COPY (
        SELECT
            hash AS tx_hash,
            block_timestamp,
            from_address,
            to_address,
            CAST(value AS BIGINT) AS value,
            CAST(gas_price AS BIGINT) AS gas_price,
            block_number
        FROM 'data/raw/eth_transactions/**/*.parquet'
        ORDER BY block_timestamp  -- Pre-sort for ASOF JOIN performance
    ) TO 'data/eth_transactions.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 1000000)
""")

# --- Token Transfers ---
print("Inspecting raw token transfers schema...")
con.execute("DESCRIBE SELECT * FROM 'data/raw/token_transfers/**/*.parquet' LIMIT 0").show()

print("Consolidating token_transfers...")
con.execute("""
    COPY (
        SELECT
            token_address,
            from_address,
            to_address,
            CAST(value AS BIGINT) AS value,
            block_timestamp
        FROM 'data/raw/token_transfers/**/*.parquet'
        ORDER BY block_timestamp
    ) TO 'data/token_transfers.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 1000000)
""")

# Report sizes
for f in ['data/eth_transactions.parquet', 'data/token_transfers.parquet', 'data/prices.parquet']:
    if os.path.exists(f):
        size_mb = os.path.getsize(f) / 1e6
        count = con.execute(f"SELECT COUNT(*) FROM '{f}'").fetchone()[0]
        print(f"  {f}: {count:,} rows, {size_mb:.1f} MB")
```

**Note on token_symbol**: AWS token_transfers may not include token symbol names. If not,
create a mapping table from the top 50 token contract addresses to symbols, and join it
in during preparation. Or simplify: use `token_address` as the ASOF JOIN partition key
instead of `token_symbol` (more realistic for TRM Labs anyway — they work with addresses).

### Step 0.6: Scale targets (real data)

| Scale    | Date Range     | Est. ETH Txns  | Est. Token Transfers | Disk   | Target HW  |
|----------|---------------|----------------|---------------------|--------|------------|
| Dev      | 7 days        | ~8-10M         | ~5-8M               | ~3 GB  | Any GPU    |
| RTX 6000 | 30 days       | ~35-40M        | ~20-30M             | ~12 GB | 24GB VRAM  |
| H100     | 180 days      | ~200M+         | ~150M+              | ~70 GB | 80GB VRAM  |
| H100-max | 365 days      | ~400M+         | ~300M+              | ~140 GB| 80GB VRAM* |

*H100-max may require column projection (don't load all columns) to fit in 80GB.

### Fallback: Synthetic Data (for pipeline testing only)

If AWS download is slow or blocked, use `scripts/generate_synthetic_data.py` to create
fake data with the same schema. This is ONLY for testing the pipeline — never use
synthetic data in the actual demo or slides. The synthetic script should generate data
matching the exact same column names/types as the real data so all queries work unchanged.

The script should accept a `--scale` argument to pick row counts.

---

## Phase 1: Demo Query Suite

**Goal**: 10 queries that a TRM Labs analyst can run interactively. Each query should
complete in <5 seconds on RTX 6000 (hot) so the demo feels snappy.

### Category A: Scan & Filter (3 queries)

These prove raw OLAP speed. TRM Labs runs billions of these.

```
queries/q01_count_range.sql
────────────────────────────
-- "How many transactions in Q1 2024?"
SELECT COUNT(*) FROM eth_transactions
WHERE block_timestamp BETWEEN '2024-01-01' AND '2024-03-31';

queries/q02_address_activity.sql
────────────────────────────────
-- "Show me this address's activity"
-- NOTE: Replace address with a real high-activity address from the data.
-- Run: SELECT from_address FROM eth_transactions GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1;
-- to find one. Uniswap Router, Tether, or major exchanges work well.
SELECT COUNT(*) AS tx_count,
       SUM(value/1e18) AS total_eth,
       MIN(block_timestamp) AS first_seen,
       MAX(block_timestamp) AS last_seen
FROM eth_transactions
WHERE from_address = '0x__REPLACE_WITH_REAL_ADDRESS__';

queries/q03_top_addresses.sql
─────────────────────────────
-- "Top 100 addresses by transaction count"
SELECT from_address, COUNT(*) AS tx_count
FROM eth_transactions
GROUP BY from_address
ORDER BY tx_count DESC LIMIT 100;
```

### Category B: ASOF JOIN (4 queries — the headline feature)

These are the queries no other GPU database can do.

```
queries/q04_price_match.sql
───────────────────────────
-- "Match every transaction to the prevailing ETH price"
SELECT COUNT(*),
       AVG(t.value/1e18 * p.price_usd) AS avg_usd_value,
       SUM(t.value/1e18 * p.price_usd) AS total_usd_volume
FROM eth_transactions t
ASOF JOIN prices p ON t.block_timestamp >= p.ts;

queries/q05_daily_volume.sql
────────────────────────────
-- "Daily USD volume over time"
SELECT DATE_TRUNC('day', t.block_timestamp) AS day,
       COUNT(*) AS num_transactions,
       SUM(t.value/1e18 * p.price_usd) AS daily_usd_volume
FROM eth_transactions t
ASOF JOIN prices p ON t.block_timestamp >= p.ts
GROUP BY 1 ORDER BY 1;

queries/q06_top_senders_usd.sql
───────────────────────────────
-- "Top 50 addresses by USD volume sent"
SELECT t.from_address,
       COUNT(*) AS tx_count,
       SUM(t.value/1e18 * p.price_usd) AS total_usd_sent
FROM eth_transactions t
ASOF JOIN prices p ON t.block_timestamp >= p.ts
GROUP BY 1 ORDER BY 3 DESC LIMIT 50;

queries/q07_token_volumes.sql
─────────────────────────────
-- "Top token contracts by transfer count and volume"
-- Note: AWS token_transfers uses token_address (contract address), not symbol.
-- For the ASOF JOIN demo, we join ETH transactions to prices (Q04-Q06).
-- This query shows token transfer analytics without ASOF (still GPU-accelerated).
SELECT token_address,
       COUNT(*) AS num_transfers,
       COUNT(DISTINCT from_address) AS unique_senders,
       COUNT(DISTINCT to_address) AS unique_receivers
FROM token_transfers
GROUP BY 1
ORDER BY 2 DESC LIMIT 50;
```

### Category C: Aggregation (3 queries)

Heavy GROUP BY — bread and butter analytics.

```
queries/q08_hourly_gas.sql
──────────────────────────
-- "Hourly gas price trends"
SELECT DATE_TRUNC('hour', block_timestamp) AS hour,
       AVG(gas_price) AS avg_gas,
       MAX(gas_price) AS max_gas,
       COUNT(*) AS tx_count
FROM eth_transactions
GROUP BY 1 ORDER BY 1;

queries/q09_address_pairs.sql
─────────────────────────────
-- "Most active address pairs"
SELECT from_address, to_address, COUNT(*) AS tx_count,
       SUM(value/1e18) AS total_eth
FROM eth_transactions
GROUP BY 1, 2
ORDER BY 3 DESC LIMIT 100;

queries/q10_block_stats.sql
───────────────────────────
-- "Block-level statistics"
SELECT block_number,
       COUNT(*) AS tx_count,
       SUM(value/1e18) AS total_eth,
       AVG(gas_price) AS avg_gas
FROM eth_transactions
GROUP BY 1
ORDER BY 2 DESC LIMIT 100;
```

### Query files

Create each query as a standalone `.sql` file in `queries/` directory.
Each file should have:
- A comment explaining what the query does in analyst terms
- The SQL query
- No trailing semicolons issues (test both DuckDB CLI and gpu_processing)

---

## Phase 2: Benchmark Runner

**Goal**: Single script that runs all queries on CPU and GPU, produces a results table.

### Step 2.1: Create `scripts/run_demo_benchmark.py`

The script should:

1. **Accept arguments**:
   - `--data-dir` (default: `data/`)
   - `--sirius-binary` (default: `./build/release/duckdb`)
   - `--queries-dir` (default: `queries/`)
   - `--gpu-cache-size` (default: `'20 GB'` for RTX 6000, `'70 GB'` for H100)
   - `--gpu-processing-size` (default: `'15 GB'` for RTX 6000, `'60 GB'` for H100)
   - `--warmup-runs` (default: 1)
   - `--benchmark-runs` (default: 3)
   - `--output` (default: `results/benchmark_results.json`)

2. **CPU baseline**: Use `duckdb` Python package to run each query, measure wall time.

3. **GPU (Sirius)**: Use subprocess to run Sirius binary with:
   ```sql
   CALL gpu_buffer_init('{cache_size}', '{processing_size}');
   -- Load data into tables from parquet
   CREATE TABLE eth_transactions AS SELECT * FROM 'data/eth_transactions.parquet';
   CREATE TABLE prices AS SELECT * FROM 'data/prices.parquet';
   CREATE TABLE token_transfers AS SELECT * FROM 'data/token_transfers.parquet';
   -- Warmup: run query once to cache data on GPU
   CALL gpu_processing('SELECT 1 FROM eth_transactions LIMIT 1');
   -- Then for each query:
   .timer on
   CALL gpu_processing('{query}');
   ```
   Parse `.timer on` output for timing.

4. **Output**: JSON + printed table like:
   ```
   Query  | CPU (ms) | GPU (ms) | Speedup | Status
   -------|----------|----------|---------|-------
   Q01    |   1,234  |      45  |  27.4x  | PASS
   Q02    |     567  |      23  |  24.7x  | PASS
   ...
   ```

5. **Correctness check**: Compare GPU row count to CPU row count for each query.
   Full value comparison is a stretch goal (floating point differences).

### Step 2.2: Create `scripts/run_live_demo.sh`

A shell script that starts the interactive demo session:

```bash
#!/bin/bash
# Live demo launcher for TRM Labs pitch
set -e

SIRIUS_BIN="${SIRIUS_BIN:-./build/release/duckdb}"
DATA_DIR="${DATA_DIR:-data}"
GPU_CACHE="${GPU_CACHE:-20 GB}"
GPU_PROC="${GPU_PROC:-15 GB}"

# Create init SQL that loads data and sets up GPU
cat > /tmp/demo_init.sql << 'EOF'
CALL gpu_buffer_init('${GPU_CACHE}', '${GPU_PROC}');
CREATE TABLE eth_transactions AS SELECT * FROM '${DATA_DIR}/eth_transactions.parquet';
CREATE TABLE prices AS SELECT * FROM '${DATA_DIR}/prices.parquet';
CREATE TABLE token_transfers AS SELECT * FROM '${DATA_DIR}/token_transfers.parquet';
.timer on
-- Data loaded! Try these queries:
-- CALL gpu_processing('SELECT COUNT(*) FROM eth_transactions');
-- See queries/ directory for demo queries
EOF

# Use envsubst to expand variables, then launch
envsubst < /tmp/demo_init.sql > /tmp/demo_init_expanded.sql

echo "=== Sirius GPU Demo ==="
echo "Data: ${DATA_DIR}"
echo "GPU cache: ${GPU_CACHE}"
echo ""
echo "Launching Sirius..."
echo "Run queries with: CALL gpu_processing('your SQL here');"
echo ""

LD_LIBRARY_PATH=".pixi/envs/cuda12/lib:$LD_LIBRARY_PATH" \
  ${SIRIUS_BIN} -unsigned -init /tmp/demo_init_expanded.sql
```

For the demo, the presenter can:
1. Launch the script
2. Run pre-written queries from `queries/` directory
3. Let TRM Labs analysts type their own ad-hoc queries
4. Show `.timer on` results in real time

---

## Phase 3: Implementation Checklist

Things that must work before the demo. Test each on RTX 6000.

### 3a. Already Done
- [x] `GPUPhysicalAsOfJoin` operator (sort + binary search algorithm)
- [x] `LOGICAL_ASOF_JOIN` dispatch fix in `gpu_physical_plan_generator.cpp`
- [x] INNER JOIN compaction (thrust::copy_if)
- [x] LEFT JOIN with NULLs (cudf NULLIFY policy)

### 3b. Must Fix/Test Before Demo
- [ ] **Q04 (ASOF JOIN, no partition key)**: Test that ASOF JOIN works without equality
      keys (only timestamp comparison). Currently untested — may need code fix if
      `lhs_partition_col_idxs` is empty.
- [ ] **Q05/Q06 (ASOF JOIN + GROUP BY)**: Test that pipeline works when ASOF JOIN feeds
      into a GROUP BY operator. This is a pipeline composition test.
- [ ] **Real data column mapping**: AWS parquet schema may differ from expected column
      names. Run `DESCRIBE` on the raw parquet and adapt `prepare_demo_data.py` to
      rename/cast columns correctly. Common issues: `value` may be STRING (uint256),
      `block_timestamp` may be INT64 (unix epoch) vs TIMESTAMP.
- [ ] **Data loading**: Verify parquet → `CREATE TABLE` → `gpu_processing` roundtrip works
      for all tables. The data must be in DuckDB tables (not read from parquet inside
      `gpu_processing`).
- [ ] **Row count validation**: Run each query on CPU and GPU, compare row counts.
      Fix any mismatches before the demo.
- [ ] **Replace placeholder address in Q02**: After loading data, find a real high-activity
      address and hardcode it into `q02_address_activity.sql`.

### 3c. Nice-to-Have (if time permits)
- [ ] Hash partition by eq_key for better ASOF JOIN cache behavior (Phase 2 Option B)
- [ ] Pre-sorted right table caching (sort once, reuse)
- [ ] Window function operator (for rolling averages — defer to post-demo if needed)
- [ ] cuGraph integration (defer to post-demo — mention in slides as roadmap)

### 3d. H100 Adaptation (when hardware arrives)
- [ ] Build with `sm_90a` target (already in CMake)
- [ ] Change `gpu_buffer_init` sizes: `('70 GB', '60 GB')`
- [ ] Re-run `generate_crypto_data.py --scale h100` for 200M+ rows
- [ ] Re-run benchmark — expect larger speedups due to HBM3 bandwidth
- [ ] Profile with nsys, run `/optimization-advisor` if bottlenecks found

---

## Phase 4: Slide Deck Outline

**Format**: 12-15 slides, clean design. Each slide has a single point.

### Slide 1: Title
```
Sirius: GPU-Accelerated Analytics for Blockchain Intelligence
─────────────────────────────────────────────────────────────
[Sirius logo]                           [Date] | [Your name]
```

### Slide 2: The Problem
```
TRM Labs processes 2.5B+ Ethereum transactions for compliance.

Current stack: StarRocks / Trino / Spark on CPU clusters
  → Batch jobs take hours
  → Graph analytics require separate systems (NetworkX, Neo4j)
  → Price matching (ASOF JOIN) is slow and complex

What if one system on one GPU could do it all?
```

### Slide 3: Sirius in 30 Seconds
```
SQL-compatible analytics engine that runs entirely on GPU.

Built on:
  • DuckDB (SQL parsing, optimizer, catalog)
  • NVIDIA cuDF (columnar GPU execution)
  • NVIDIA cuGraph (graph analytics) [coming soon]

Drop-in replacement: same SQL, 10-50x faster.
```

### Slide 4: Architecture Diagram
```
┌──────────────────────────────────────────────┐
│                  Sirius                       │
│  ┌──────────┐  ┌──────────┐  ┌────────────┐ │
│  │ DuckDB   │  │ cuDF     │  │ cuGraph    │ │
│  │ SQL      │→ │ Columnar │→ │ Graph      │ │
│  │ Planner  │  │ Engine   │  │ Analytics  │ │
│  └──────────┘  └──────────┘  └────────────┘ │
│           All in GPU Memory (80GB)           │
└──────────────────────────────────────────────┘
         ↑ Load once                ↓ Query results
    ┌─────────┐              ┌──────────┐
    │ Parquet │              │ Analyst  │
    │ Files   │              │ (SQL)    │
    └─────────┘              └──────────┘

vs. Current TRM Labs stack:
  Parquet → StarRocks (SQL) → Export → Python → NetworkX (graph) → Results
  [3 systems, 2 data copies, hours of latency]
```

### Slide 5: Benchmark Setup
```
Hardware: NVIDIA H100 80GB  (or RTX 6000 24GB for dev)
Data: Real Ethereum transactions (AWS Public Blockchain Data)
  • 200M+ transactions, 100M+ token transfers
  • Real ETH/USD prices (CoinGecko hourly)
Baseline: DuckDB (fastest single-node CPU analytics engine)
```

### Slide 6: Raw Analytics Speed
```
[Bar chart: Q01-Q03 CPU vs GPU times]

Query                              CPU      GPU     Speedup
─────────────────────────────────────────────────────────────
Count transactions in date range   XXX ms   XX ms   XX.Xx
Address activity lookup            XXX ms   XX ms   XX.Xx
Top 100 addresses by tx count      XXX ms   XX ms   XX.Xx

"Standard analytics queries: 10-30x faster."
```

### Slide 7: ASOF JOIN — The Killer Feature
```
Every blockchain analytics query that needs USD values
requires joining transactions to price feeds by timestamp.

    SELECT SUM(t.value * p.price_usd)
    FROM eth_transactions t
    ASOF JOIN prices p
      ON t.block_timestamp >= p.ts;

"Match 200M transactions to the nearest price quote — on GPU."
```

### Slide 8: ASOF JOIN Results
```
[Bar chart: Q04-Q07 CPU vs GPU times]

Query                              CPU      GPU     Speedup
─────────────────────────────────────────────────────────────
Price match (all transactions)     XXX ms   XX ms   XX.Xx
Daily USD volume                   XXX ms   XX ms   XX.Xx
Top senders by USD volume          XXX ms   XX ms   XX.Xx
Token transfer volumes             XXX ms   XX ms   XX.Xx

"No other GPU database supports ASOF JOIN."
```

### Slide 9: Aggregation Performance
```
[Bar chart: Q08-Q10 CPU vs GPU times]

Heavy GROUP BY queries — the daily workload.
Address pairs, hourly trends, block statistics.

XX-XXx faster across the board.
```

### Slide 10: Graph Analytics Roadmap (cuGraph)
```
Coming soon: SQL → Graph in one system, one GPU.

Entity clustering     → Connected Components
Transaction tracing   → BFS from flagged address
Risk scoring          → PageRank on tx graph
Community detection   → Louvain/Leiden

All on the same GPU memory as SQL — zero data movement.

[This replaces: StarRocks → Export → NetworkX pipeline]
```

### Slide 11: The Hybrid Query (Vision)
```
"Find all addresses within 3 hops of a sanctioned entity
 that received >$10K USD in the last 30 days"

Step 1: BFS from sanctioned address (cuGraph, <100ms)
Step 2: ASOF JOIN for USD values (cuDF, <500ms)
Step 3: Filter by amount and date (cuDF, <100ms)

Total: <1 second on GPU
Current stack: minutes (export + NetworkX + re-import)
```

### Slide 12: Why Blockchain Data Fits GPU Perfectly
```
Blockchain is append-only and immutable.
  → Historical data never changes
  → Load once into GPU memory, query all day
  → No write-invalidation problem

Your workload:
  • 90% historical batch analytics  → Perfect fit (always hot)
  • 10% near-real-time alerting     → Stays on existing CPU stack

"Load 6 months of ETH transactions into 80GB VRAM.
 Every subsequent query runs at memory bandwidth speed."
```

### Slide 13: Live Demo
```
[Switch to terminal — run queries interactively]

Let the audience pick queries or type their own.
```

### Slide 14: Pricing & Next Steps
```
[Your commercial terms here]

• Proof of concept: 2 weeks on your data
• Integration: DuckDB-compatible SQL — minimal code changes
• Deployment: single H100 node replaces a CPU cluster
```

### Slide 15: Q&A
```
[Contact info]
```

---

## Phase 5: Compute Model Fit Analysis

**Key constraint**: Sirius cold runs are expensive (PCIe transfer dominates). Writes
invalidate the GPU cache and force a full re-transfer.

**Why blockchain analytics works for us**:
- Blockchain data is **append-only and immutable** — past blocks never change
- 90% of compute cost is **historical batch analytics** — load once, query many times
- 10% near-real-time alerting stays on their existing CPU stack

**Demo positioning**:
- Report "load time" separately and honestly (one-time cost)
- Report query times as hot-run performance (the repeated benefit)
- Pitch: "Your 4-hour batch jobs take 20 minutes. Load once, analyze all day."

**Data sizing for H100 (80GB VRAM)**:
- 200M ETH transactions (~40-60GB projected columns) fits in VRAM
- Full 2.5B rows is a multi-GPU / streaming story — don't promise it yet

**What NOT to demo**:
- Do not demo incremental updates (write path is expensive)
- Do not demo real-time streaming ingestion
- Do not claim sub-second latency on cold data

---

## File Structure (what the agent should create)

```
sirius-crypto-demo/
├── data/                          # Parquet files (gitignored)
│   ├── raw/                       # Raw downloads from AWS S3
│   │   ├── eth_transactions/      # Partitioned by date=YYYY-MM-DD/
│   │   └── token_transfers/       # Partitioned by date=YYYY-MM-DD/
│   ├── eth_transactions.parquet   # Consolidated, column-mapped
│   ├── token_transfers.parquet    # Consolidated, column-mapped
│   └── prices.parquet             # From CoinGecko / CryptoDataDownload
├── queries/                       # Demo query files
│   ├── q01_count_range.sql
│   ├── q02_address_activity.sql
│   ├── q03_top_addresses.sql
│   ├── q04_price_match.sql
│   ├── q05_daily_volume.sql
│   ├── q06_top_senders_usd.sql
│   ├── q07_token_volumes.sql
│   ├── q08_hourly_gas.sql
│   ├── q09_address_pairs.sql
│   └── q10_block_stats.sql
├── scripts/
│   ├── download_eth_data.py       # Download from AWS S3 (no account/CLI needed)
│   ├── download_prices.py         # Download price data from CoinGecko
│   ├── generate_synthetic_data.py # FALLBACK ONLY: synthetic data for pipeline testing
│   ├── run_demo_benchmark.py      # Automated benchmark runner
│   ├── run_live_demo.sh           # Interactive demo launcher
│   └── validate_queries.py        # CPU vs GPU correctness check
├── results/                       # Benchmark output (gitignored)
│   └── benchmark_results.json
├── slides/                        # Slide deck source
│   └── trm_labs_pitch.md          # Marp/reveal.js markdown slides
└── docs/
    └── trm_labs_benchmark_plan.md # This file
```

---

## Execution Order for Agent

Follow these steps in order. Each step has a **verification check** — do not proceed
until the check passes.

### Step 1: Download real Ethereum data
```bash
cd /home/cc/sirius-crypto-demo

# No AWS account needed — public bucket, DuckDB reads S3 directly
# 7 days ≈ 8-10M transactions, ~2GB download
python scripts/download_eth_data.py --scale dev
```
**Check**: Script prints row count and date range on completion.
```bash
~/.pixi/bin/pixi run -e cuda12 python -c "
import duckdb; con = duckdb.connect()
print(con.execute(\"SELECT COUNT(*), MIN(block_timestamp)::DATE, MAX(block_timestamp)::DATE FROM 'data/eth_transactions.parquet'\").fetchone())
"
```

**If S3 download fails**: Fall back to synthetic data (pipeline testing only — not for demos):
```bash
~/.pixi/bin/pixi run -e cuda12 python scripts/generate_synthetic_data.py --scale dev
```

### Step 2: Download price data
```bash
~/.pixi/bin/pixi run -e cuda12 python scripts/download_prices.py
```
**Check**: Script prints row count, date range, and symbols on completion.

**If CoinGecko API fails** (rate limit or down): re-run after 60 seconds.
Free tier allows ~10 calls/min; the script fetches 8 coins with 6s sleep between each.

### Step 3: Find a real address for Q02 and update the query
```bash
duckdb -c "
  SELECT from_address, COUNT(*) AS cnt
  FROM 'data/eth_transactions.parquet'
  GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
"
# Take the top address and replace the placeholder in queries/q02_address_activity.sql
```
**Check**: `queries/q02_address_activity.sql` contains a real `0x...` address.

### Step 5: Verify CPU queries work
```bash
python scripts/validate_queries.py --mode cpu-only
```
**Check**: All 10 queries return results without errors. Save row counts.

### Step 6: Build Sirius
```bash
cd /home/cc/sirius-asof
~/.pixi/bin/pixi run -e cuda12 make release
```
**Check**: `./build/release/duckdb --version` prints version string.

### Step 7: Verify GPU queries work
```bash
python scripts/validate_queries.py --mode gpu-only \
  --sirius-binary ./build/release/duckdb
```
**Check**: All 10 queries return results. Row counts match CPU ±0 for non-FP queries.
If any query fails, check `build/release/log/sirius_*.log` for the actual error.
Common failures:
- "Asof join not supported" → dispatch bug (should be fixed already)
- GPU speedup ≈ 1.0x → GPU is falling back to CPU silently
- Row count mismatch → INNER vs LEFT join semantics

### Step 8: Run benchmark (dev scale)
```bash
python scripts/run_demo_benchmark.py
```
**Check**: Results table printed. GPU speedup > 1.0x for all queries (proves GPU is
actually executing, not falling back to CPU).

### Step 9: Scale up to RTX 6000
```bash
# Download 30 days of real data (~35-40M transactions)
~/.pixi/bin/pixi run -e cuda12 python scripts/download_eth_data.py --scale rtx6000
python scripts/run_demo_benchmark.py
```
**Check**: Results table shows meaningful speedups (expect 5-30x on 30M+ rows).

### Step 10: Test live demo
```bash
bash scripts/run_live_demo.sh
# In the interactive session, paste a few queries from queries/ directory
# Let it run interactively — this simulates the actual pitch
```
**Check**: Queries return results interactively with `.timer on` showing times.

### Step 11: Generate slide deck
```bash
# Create slides/trm_labs_pitch.md with content from Phase 4 above
# Fill in actual benchmark numbers from Step 9 results
# If using Marp: npm install -g @marp-team/marp-cli && marp slides/trm_labs_pitch.md --html
```
**Check**: Slide deck renders with real benchmark numbers filled in.

### Step 12 (when H100 available): Scale up
```bash
# Download 180 days of real data (~200M+ transactions)
bash scripts/download_eth_data.sh h100
python scripts/prepare_demo_data.py
# Update gpu_buffer_init sizes
python scripts/run_demo_benchmark.py \
  --gpu-cache-size '70 GB' --gpu-processing-size '60 GB'
```
**Check**: Results table with H100 numbers. Update slide deck with final numbers.

---

## cuGraph Integration (Post-Demo Roadmap)

Not included in the initial demo — mentioned in slides as "coming soon."

| Algorithm | cuGraph API | TRM Labs Use Case |
|-----------|------------|-------------------|
| Connected Components | `cugraph.components.connected_components()` | Entity clustering |
| BFS | `cugraph.traversal.bfs()` | Transaction tracing |
| PageRank | `cugraph.link_analysis.pagerank()` | Risk scoring |
| Louvain | `cugraph.community.louvain()` | Community detection |
| Triangle Count | `cugraph.community.triangle_count()` | Cyclic tx detection |

**Key advantage**: cuGraph operates on same GPU memory as cuDF — zero-copy SQL→Graph.
No other SQL database offers this.

---

## Systems to Compare (in benchmark)

| System | Role | How to Run |
|--------|------|------------|
| **Sirius GPU** | Our system | `scripts/run_demo_benchmark.py` |
| **DuckDB CPU** | Baseline (fastest single-node) | Python `duckdb` package |
| Trino | Optional — what TRM might use | Docker (add if time) |
| StarRocks | Optional — what TRM might use | Docker (add if time) |

For the initial demo, CPU DuckDB vs Sirius GPU is sufficient. Add Trino/StarRocks
comparison only if time permits — the CPU DuckDB baseline is already compelling since
DuckDB is the fastest single-node CPU analytics engine.
