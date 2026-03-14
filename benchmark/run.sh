#!/bin/bash
# Crypto benchmark runner for Sirius (GPU-accelerated DuckDB extension)
#
# Runs each query from queries.sql with gpu_processing in a single session:
#   1x gpu_buffer_init → TRIES x gpu_processing(query)
# First run = cold (loads tables to GPU cache), subsequent = hot.
#
# Sizing guide — adjust for your GPU:
#   gpu_buffer_init(cache, processing, pinned_memory_size = host)
#   - cache: GPU memory for table data (rule of thumb: ~60-70% of VRAM)
#   - processing: GPU working memory for joins/aggs (~20-30% of VRAM)
#   - pinned_memory_size: host pinned memory for data transfer
#   Total cache + processing must fit in GPU VRAM.
#
# Machine-specific settings below — MODIFY FOR YOUR GPU:
#   GH200 (96GB):  GPU_CACHING_SIZE='80 GB', GPU_PROCESSING_SIZE='40 GB'
#   A100  (80GB):  GPU_CACHING_SIZE='50 GB', GPU_PROCESSING_SIZE='25 GB'
#   RTX 6000 (24GB): GPU_CACHING_SIZE='16 GB', GPU_PROCESSING_SIZE='6 GB'
#   RTX 4090 (24GB): GPU_CACHING_SIZE='16 GB', GPU_PROCESSING_SIZE='6 GB'

TRIES=3

# --- GPU memory settings (RTX 6000 24GB) ---
GPU_CACHING_SIZE='16 GB'
GPU_PROCESSING_SIZE='6 GB'
CPU_PROCESSING_SIZE='32 GB'

cat queries.sql | while read -r query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

    echo "$query";

    cli_params=()
    cli_params+=("-c")
    cli_params+=(".timer on")
    cli_params+=("-c")
    cli_params+=("call gpu_buffer_init(\"${GPU_CACHING_SIZE}\", \"${GPU_PROCESSING_SIZE}\", pinned_memory_size = \"${CPU_PROCESSING_SIZE}\");")

    for i in $(seq 1 $TRIES); do
      cli_params+=("-c")
      cli_params+=("call gpu_processing(\"${query}\");")
    done;

    echo "${cli_params[@]}"
    duckdb crypto.db "${cli_params[@]}"
done;
