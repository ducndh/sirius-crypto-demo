# Data Guide: Building the Crypto Demo Database

This document explains how to create the databases used in our benchmarks.
There are two sizes:

| Database | Rows (flows) | Disk size | GPU memory needed |
|----------|-------------:|---------:|------------------:|
| 3-month slim | 63M-116M | ~2 GB | 10 GB (RTX 6000+) |
| 2-year full | 770M | ~17 GB | 36 GB (A100 40GB) |

## Prerequisites

All paths assume:
- Sirius binary at `~/sirius-dev/build/release/duckdb`
- Pixi env at `~/sirius-dev/.pixi/envs/cuda12`
- AWS CLI installed (`pip install awscli`)
- Kaggle CLI installed (`pip install kaggle`) with API token at `~/.kaggle/kaggle.json`

Override with env vars: `SIRIUS_BIN`, `SIRIUS_PIXI_ENV`, `AWS_CLI`.

## Option A: 3-Month Slim Database

Best for quick testing on GPUs with 24 GB VRAM (RTX 6000, A5000, L40S).

```bash
# 1. Download 3 months of Ethereum token transfers (~25 GB raw parquet)
python scripts/download_eth_transfers.py --start-date 2025-10-01 --end-date 2025-12-31

# 2. Download MBAL entity labels (~1 GB)
python scripts/download_mbal.py

# 3. Build database
python scripts/prepare_tables.py --year 2025

# Output: data/crypto_demo_2025.duckdb (~2 GB)
# Also exports: data/bench_*.parquet for gpu_execution path
```

### What prepare_tables.py does

1. Loads raw `token_transfers` from parquet (hive-partitioned by date)
2. Loads `entity_address_map` from MBAL CSV (Ethereum mainnet only)
3. Materializes `address_flows_daily` — GROUP BY (date, token, from, to)
4. Normalizes addresses: AWS uses 66-char zero-padded, MBAL uses 42-char
5. Builds `address_dictionary` (VARCHAR address → INT32 addr_id)
6. Creates dictionary-encoded tables (`*_dict`) for GPU benchmarks
7. Exports parquet files for the `gpu_execution` path

### Tables created

| Table | Description | Key columns |
|-------|-------------|-------------|
| `token_transfers` | Raw Ethereum transfers | from_address, to_address, value, date |
| `entity_address_map` | MBAL labels (VARCHAR) | address, entity, category |
| `address_flows_daily` | Daily aggregated flows (VARCHAR) | from_address, to_address, amount |
| `address_dictionary` | Address → INT32 mapping | address, addr_id |
| `address_flows_daily_dict` | Daily flows (INT keys) | from_addr_id, to_addr_id, amount |
| `entity_address_map_dict` | Entity map (INT keys) | addr_id, entity, category |

## Option B: 2-Year Full Database

Used for scaling benchmarks. Requires A100 (40 GB) or larger.
Downloads data quarter-by-quarter to manage disk budget.

**Disk budget**: ~37 GB per quarter download (cleaned after aggregation).
Peak usage ~57 GB, well within a typical /tmp SSD.

```bash
# Build the full 2-year database (Jan 2024 – Mar 2026)
# Output is written to /tmp/crypto_2yr.duckdb to avoid JuiceFS slowdown
python scripts/generate_2yr_data.py --start 2024-01-01 --end 2026-03-24

# If interrupted, resume without re-downloading completed quarters:
python scripts/generate_2yr_data.py --resume
```

### How generate_2yr_data.py works

1. Copies entity tables from the slim DB (`data/crypto_demo_2025_slim.duckdb`)
2. For each 3-month quarter:
   - Downloads raw parquet from AWS S3 to `/tmp/eth_transfers_raw/`
   - Aggregates into `address_flows_daily` (VARCHAR → normalized 42-char)
   - Dictionary-encodes addresses using `address_dictionary`
   - Appends to `address_flows_daily_dict` in the output DB
   - Deletes raw parquet to free disk
3. Verifies row counts and date ranges

### Additional tables for multi-hop queries

The 2-year database also needs these tables (copied from slim DB):

| Table | Rows | Description |
|-------|-----:|-------------|
| `entity_address_map_int` | 3.2M | entity_id BIGINT + addr_id INT (GPU-only join keys) |
| `entity_lookup` | 322 | entity_id → entity name (decode after GPU query) |

These are created by the slim DB build and copied automatically by `generate_2yr_data.py`.

## Configuring GPU Memory

The `gpu_buffer_init(cache, work)` call partitions GPU VRAM:

| GPU | VRAM | Recommended cache | Recommended work |
|-----|-----:|------------------:|-----------------:|
| RTX 6000 | 24 GB | 12 GB | 8 GB |
| A5000 | 24 GB | 12 GB | 8 GB |
| A100 | 40 GB | 18 GB | 18 GB |
| H100 | 80 GB | 40 GB | 30 GB |

The **working pool** is the bottleneck for large queries — it holds intermediate
join results. At 8 GB work pool, OOM occurs around 320M rows. At 18 GB, around
620M rows.

## Important Notes

- **Always use the Sirius DuckDB binary** to create databases, not system
  duckdb. The Sirius fork is based on DuckDB 1.4.4 and the on-disk format must
  match.
- **Dictionary encoding is required for GPU execution.** VARCHAR join keys and
  GROUP BY columns cause GPU fallback or extreme slowdown. All benchmark queries
  use the `*_dict` tables.
- **Address normalization matters.** AWS parquet stores addresses as 66-char
  zero-padded hex. MBAL uses standard 42-char. `prepare_tables.py` normalizes
  via `'0x' || SUBSTR(address, 27)`.
- **Entity coverage is low** (~0.3% of flows match MBAL labels). DuckDB's
  optimizer rewrites LEFT JOIN + HAVING to INNER JOIN, so actual GPU work is
  small. Higher-coverage labels would increase GPU working set.
