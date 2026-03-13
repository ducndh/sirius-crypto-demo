#!/usr/bin/env python3
"""Download MBAL 10M Crypto Address Label Dataset from Kaggle.

Requires: pip install kaggle
Configure: ~/.kaggle/kaggle.json with API credentials
Dataset: https://www.kaggle.com/datasets/yidongchaintoolai/mbal-10m-crypto-address-label-dataset
"""
import os
import sys
import subprocess
import zipfile
import glob

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "mbal")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    zip_path = os.path.join(DATA_DIR, "mbal.zip")
    csv_path = os.path.join(DATA_DIR, "dataset_10m_ads.csv")

    if os.path.exists(csv_path):
        print(f"MBAL dataset already exists at {csv_path}")
        return

    print("Downloading MBAL dataset from Kaggle...")
    print("(Requires: pip install kaggle && configure ~/.kaggle/kaggle.json)")
    try:
        subprocess.run([
            "kaggle", "datasets", "download",
            "-d", "yidongchaintoolai/mbal-10m-crypto-address-label-dataset",
            "-p", DATA_DIR
        ], check=True)
    except FileNotFoundError:
        print("ERROR: kaggle CLI not found. Install with: pip install kaggle")
        print("Then configure API key: https://www.kaggle.com/docs/api")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: kaggle download failed: {e}")
        sys.exit(1)

    # Find and extract the zip file
    zip_files = glob.glob(os.path.join(DATA_DIR, "*.zip"))
    if zip_files:
        print(f"Extracting {zip_files[0]}...")
        with zipfile.ZipFile(zip_files[0], 'r') as zf:
            zf.extractall(DATA_DIR)
        os.remove(zip_files[0])

    print(f"MBAL dataset ready at {DATA_DIR}")

    # Show basic stats
    if os.path.exists(csv_path):
        line_count = sum(1 for _ in open(csv_path))
        print(f"  Rows: {line_count - 1:,}")  # subtract header
        size_mb = os.path.getsize(csv_path) / 1024 / 1024
        print(f"  Size: {size_mb:.0f} MB")


if __name__ == "__main__":
    main()
