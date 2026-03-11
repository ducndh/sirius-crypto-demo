# sirius-crypto-demo

End-to-end benchmark and live demo harness for [Sirius](https://github.com/sirius-db/sirius)
GPU analytics on real blockchain data.

## What this is

Benchmarks and demo queries for pitching Sirius GPU analytics on Ethereum transaction data.
Compares Sirius GPU vs DuckDB CPU on queries TRM Labs actually runs: scan/filter, ASOF JOIN
for USD price matching, and heavy GROUP BY aggregation.

## Quickstart

```bash
# 1. Install AWS CLI and Python deps
sudo apt install awscli       # or: pip install awscli
pip install duckdb requests   # or use the pixi env from sirius-asof

# 2. Download real Ethereum data (7 days, ~8-10M rows, no account needed)
python scripts/download_eth_data.py --scale dev

# 3. Download price data
python scripts/download_prices.py

# 4. Validate queries (CPU only first)
python scripts/validate_queries.py --mode cpu-only

# 6. Full benchmark (CPU vs GPU)
export SIRIUS_BIN=/home/cc/sirius-asof/build/release/duckdb
python scripts/run_demo_benchmark.py

# 7. Live interactive demo
bash scripts/run_live_demo.sh
```

## Scale targets

| Scale    | Date range  | ETH txns  | Disk  | Hardware   |
|----------|-------------|-----------|-------|------------|
| dev      | 7 days      | ~8-10M    | ~3GB  | Any GPU    |
| rtx6000  | 30 days     | ~35-40M   | ~12GB | RTX 6000   |
| h100     | 180 days    | ~200M+    | ~70GB | H100 80GB  |

## Environment variables

| Variable              | Default                                          | Description           |
|-----------------------|--------------------------------------------------|-----------------------|
| `SIRIUS_BIN`          | `/home/cc/sirius-asof/build/release/duckdb`      | Sirius binary path    |
| `SIRIUS_PIXI_ENV`     | `/home/cc/sirius-asof/.pixi/envs/cuda12`         | Pixi env for LD path  |
| `GPU_CACHE_SIZE`      | `20 GB`                                          | GPU cache (RTX 6000)  |
| `GPU_PROC_SIZE`       | `15 GB`                                          | GPU proc (RTX 6000)   |
| `DATA_DIR`            | `data`                                           | Parquet files dir     |

For H100: set `GPU_CACHE_SIZE='70 GB'` and `GPU_PROC_SIZE='60 GB'`.

## Queries

| Query | Category         | Description                                      |
|-------|-----------------|--------------------------------------------------|
| Q01   | Scan/Filter      | Count transactions in date range                 |
| Q02   | Scan/Filter      | Activity summary for a specific address          |
| Q03   | Scan/Filter      | Top 100 addresses by transaction count           |
| Q04   | ASOF JOIN        | Match all transactions to ETH/USD price          |
| Q05   | ASOF JOIN        | Daily USD volume over time                       |
| Q06   | ASOF JOIN        | Top 50 senders by total USD volume               |
| Q07   | Aggregation      | Top token contracts by transfer activity         |
| Q08   | Aggregation      | Hourly gas price trends                          |
| Q09   | Aggregation      | Most active address pairs                        |
| Q10   | Aggregation      | Block-level statistics                           |
| Q11   | Graph (cuGraph)  | PageRank — most important addresses              |
| Q12   | Graph (cuGraph)  | Connected Components — entity clustering         |
| Q13   | Graph (cuGraph)  | BFS — transaction trace from flagged address     |

## Data sources

- **Transactions**: [AWS Public Blockchain Data](https://registry.opendata.aws/aws-public-blockchain/)
  (`s3://aws-public-blockchain/v1.0/eth/` — free, no auth)
- **Prices**: [CoinGecko API](https://www.coingecko.com/en/api) (free tier, hourly)
  or [CryptoDataDownload](https://www.cryptodatadownload.com/)

## Slide deck

See `slides/trm_labs_pitch.md` — Marp-compatible markdown.
Render with: `npx @marp-team/marp-cli slides/trm_labs_pitch.md --html`
