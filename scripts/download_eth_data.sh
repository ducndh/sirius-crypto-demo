#!/bin/bash
# Download real Ethereum transaction data from AWS Public Blockchain Data.
# Free, no AWS account needed (--no-sign-request).
# Data: s3://aws-public-blockchain/v1.0/eth/
#
# Usage:
#   bash scripts/download_eth_data.sh [scale] [data_dir]
#   scale: dev (7 days) | rtx6000 (30 days) | h100 (180 days) | h100-max (365 days)
#
set -euo pipefail

SCALE="${1:-dev}"
DATA_DIR="${2:-data/raw}"
S3_BASE="s3://aws-public-blockchain/v1.0/eth"

# Check aws cli
if ! command -v aws &>/dev/null; then
  echo "ERROR: aws CLI not found. Install with: sudo apt install awscli"
  echo "       or: pip install awscli"
  exit 1
fi

case "$SCALE" in
  dev)
    START="2024-01-01"; END="2024-01-07"
    echo "Scale: dev — 7 days (~8-10M transactions, ~3GB)"
    ;;
  rtx6000)
    START="2024-01-01"; END="2024-01-31"
    echo "Scale: rtx6000 — 30 days (~35-40M transactions, ~12GB)"
    ;;
  h100)
    START="2024-01-01"; END="2024-06-30"
    echo "Scale: h100 — 180 days (~200M+ transactions, ~70GB)"
    ;;
  h100-max)
    START="2024-01-01"; END="2024-12-31"
    echo "Scale: h100-max — 365 days (~400M+ transactions, ~140GB)"
    ;;
  *)
    echo "Unknown scale: $SCALE. Use: dev | rtx6000 | h100 | h100-max"
    exit 1
    ;;
esac

ETH_TX_DIR="$DATA_DIR/eth_transactions"
TOKEN_DIR="$DATA_DIR/token_transfers"
mkdir -p "$ETH_TX_DIR" "$TOKEN_DIR"

echo ""
echo "Downloading ETH transactions: $START → $END"
echo "Target: $ETH_TX_DIR"
echo ""

# Download day by day
current="$START"
tx_days=0
while [[ "$current" < "$END" ]] || [[ "$current" == "$END" ]]; do
  dest="$ETH_TX_DIR/date=${current}/"
  if [[ -d "$dest" ]] && [[ -n "$(ls -A "$dest" 2>/dev/null)" ]]; then
    echo "  [skip] date=$current (already downloaded)"
  else
    echo "  Downloading transactions/date=$current ..."
    aws s3 cp --no-sign-request --recursive \
      "${S3_BASE}/transactions/date=${current}/" \
      "$dest" \
      --quiet || echo "  WARNING: No data for date=$current (may be weekend/holiday)"
  fi
  current=$(date -d "$current + 1 day" +%Y-%m-%d)
  ((tx_days++)) || true
done

echo ""
echo "Downloading token transfers: $START → $END"
echo "Target: $TOKEN_DIR"
echo ""

current="$START"
while [[ "$current" < "$END" ]] || [[ "$current" == "$END" ]]; do
  dest="$TOKEN_DIR/date=${current}/"
  if [[ -d "$dest" ]] && [[ -n "$(ls -A "$dest" 2>/dev/null)" ]]; then
    echo "  [skip] date=$current (already downloaded)"
  else
    echo "  Downloading token_transfers/date=$current ..."
    aws s3 cp --no-sign-request --recursive \
      "${S3_BASE}/token_transfers/date=${current}/" \
      "$dest" \
      --quiet || echo "  WARNING: No data for date=$current"
  fi
  current=$(date -d "$current + 1 day" +%Y-%m-%d)
done

echo ""
echo "=== Download complete ==="
du -sh "$ETH_TX_DIR" "$TOKEN_DIR" 2>/dev/null || true
echo ""
echo "Next step: python scripts/prepare_demo_data.py"
