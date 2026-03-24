# TPC-H SF=10 gpu_processing — Cross-Machine Reference

Calibration numbers for setting expectations on different hardware pairs.
All timings: warm best-of-3, per-query warm methodology (1 warmup + 3 bench back-to-back).

**Date:** 2026-03-24
**Sirius branch:** `dev` (commit 500b050). TPC-H queries don't use FULL OUTER JOIN.

## Hardware Tested

| | RTX A5000 | A100-SXM4-40GB | Paper (GH200 vs c8i) |
|---|---|---|---|
| **GPU** | RTX A5000 (24GB GDDR6) | A100-SXM4-40GB (40GB HBM2e) | GH200 (96GB HBM3) |
| **GPU mem BW** | 768 GB/s | 1,555 GB/s | 3,350 GB/s |
| **CPU** | Threadripper PRO 3975WX (32c/64t) | Xeon Gold 6526Y (2x16c/64t) | c8i.8xlarge (16c/32t) |
| **RAM** | DDR4 | 64GB DDR5 | DDR5 |
| **gpu_buffer_init** | `('9 GB', '9 GB')` | `('16 GB', '16 GB')` | ~80% of 96GB |
| **Scale factor** | SF=10 | SF=10 | SF=100 |

## Results: Standard TPC-H (VARCHAR/DATE columns)

**A5000 numbers are from standard TPC-H only** — mod (integer-encoded) benchmark not yet
run on A5000. A5000 Q01 shows VARCHAR GROUP BY regression (0.4x). A5000 Q11 silently
falls back to CPU due to INTERVAL type (see Known Issues). These are not bugs in the
GPU engine — they are schema/type limitations. TODO: re-run A5000 with mod queries.
Also TODO: collect RTX 6000 numbers for a third calibration point.

| Query | A5000 CPU | A5000 GPU | A5000 x | A100 CPU | A100 GPU | A100 x | Paper x (est.) |
|-------|-----------|-----------|---------|----------|----------|--------|----------------|
| Q01 | 31 | 79 | 0.4x | 39 | 62 | 0.6x | ~7x+ |
| Q02 | 36 | 13 | 2.8x | 50 | 18 | 2.8x | ~3x |
| Q03 | 66 | 19 | 3.5x | 94 | 18 | 5.2x | ~4x |
| Q04 | 100 | 22 | 4.5x | 121 | 19 | 6.4x | ~4x |
| Q05 | 86 | 23 | 3.7x | 104 | 20 | 5.2x | ~4x |
| Q06 | 16 | 4 | 4.0x | 13 | 4 | 3.2x | ~7x |
| Q07 | 66 | 24 | 2.8x | 84 | 23 | 3.7x | ~4x |
| Q08 | 84 | 26 | 3.2x | 115 | 25 | 4.6x | ~5x |
| Q09 | 202 | 51 | 4.0x | 285 | 37 | 7.7x | ~8x |
| Q10 | 127 | 27 | 4.7x | 182 | 25 | 7.3x | ~7x |
| Q11 | 21 | 35 | 0.6x* | 22 | 35 | 0.6x* | ~7x |
| Q12 | 55 | 18 | 3.1x | 55 | 14 | 3.9x | ~7x |
| Q13 | 155 | 77 | 2.0x | 211 | 38 | 5.6x | ~4x |
| Q14 | 32 | 7 | 4.6x | 58 | 5 | 11.6x | ~14x |
| Q15 | 23 | 8 | 2.9x | 37 | 7 | 5.3x | ~3x |
| Q16 | 56 | 16 | 3.5x | 77 | 14 | 5.5x | ~3x |
| Q17 | 71 | 50 | 1.4x | 68 | 44 | 1.5x | ~3x |
| Q18 | 120 | 46 | 2.6x | 150 | 35 | 4.3x | ~5x |
| Q19 | 61 | 35 | 1.7x | 83 | 20 | 4.2x | ~10x |
| Q20 | 43 | 18 | 2.4x | 72 | 16 | 4.5x | ~12x |
| Q21 | 320 | 102 | 3.1x | 365 | 73 | 5.0x | ~10x |
| Q22 | 72 | 15 | 4.8x | 77 | 14 | 5.5x | ~7x |

\* Q11 GPU times are CPU fallback + overhead (see Known Issues).

All times in milliseconds.

## Results: Integer-Encoded TPC-H (mod queries, A100 only)

The paper used integer-encoded schema — all dates as BIGINT (YYYYMMDD), all categoricals
as BIGINT. This avoids VARCHAR hashing and DATE type handling on GPU.

| Query | A100 CPU | A100 GPU | A100 x |
|-------|----------|----------|--------|
| Q01 | 50 | 50 | 1.0x |
| Q02 | 44 | 18 | 2.4x |
| Q03 | 112 | 18 | 6.2x |
| Q04 | 118 | 18 | 6.6x |
| Q05 | 108 | 24 | 4.5x |
| Q06 | 21 | 4 | 5.2x |
| Q07 | 101 | 21 | 4.8x |
| Q08 | 129 | 26 | 5.0x |
| Q09 | 277 | 39 | 7.1x |
| Q10 | 274 | 77 | 3.6x |
| Q11 | 30 | 15 | 2.0x |
| Q12 | 56 | 14 | 4.0x |
| Q13 | 204 | 40 | 5.1x |
| Q14 | 53 | 7 | 7.6x |
| Q15 | 34 | 7 | 4.9x |
| Q16 | 49 | 12 | 4.1x |
| Q17 | 102 | 46 | 2.2x |
| Q18 | 158 | 38 | 4.2x |
| Q19 | 68 | 15 | 4.5x |
| Q20 | 73 | 16 | 4.6x |
| Q21 | 385 | 75 | 5.1x |
| Q22 | 56 | 12 | 4.7x |

## Speedup Summary

| Metric | A5000 std | A100 std | A100 mod | Paper (GH200, mod, SF=100) |
|---|---|---|---|---|
| Geomean (excl Q11*) | ~2.7x | ~3.8x | ~4.1x | ~6x |
| Best query | Q22: 4.8x | Q14: 11.6x | Q14: 7.6x | Q14/Q19/Q20: ~10-14x |
| Worst query | Q01: 0.4x | Q01: 0.6x | Q01: 1.0x | Q02/Q15: ~3x |
| Queries < 1x | Q01, Q11* | Q01 | none | none |

## Key Insight: Speedup Is a Function of the Hardware Pair

```
GPU speedup ≈ (GPU memory bandwidth) / (CPU aggregate scan+join throughput)
```

| Factor | A5000+TR | A100+Xeon | GH200+c8i | Effect |
|---|---|---|---|---|
| GPU BW | 768 GB/s | 1,555 GB/s | 3,350 GB/s | More BW → faster GPU → higher speedup |
| CPU cores | 32 (strong) | 32 (moderate) | 16 (weak) | Fewer cores → slower CPU → higher speedup |
| Data size | SF=10 | SF=10 | SF=100 | More data → GPU parallelism wins |
| Avg speedup | ~3x | ~4-5x | ~6-8x | Consistent with hardware ratios |

When evaluating on a new machine pair, expect speedup proportional to the
GPU-bandwidth / CPU-strength ratio. Don't compare raw speedup numbers across
machines without accounting for the CPU baseline difference.

## Known Issues

1. **Silent CPU fallback in gpu_processing**: When GPU hits an unsupported type
   (e.g. INTERVAL = type 27), `GPUProcessingFunction` (`sirius_extension.cpp:324-329`)
   silently falls back to CPU. The `enable_duckdb_fallback` config flag only guards
   plan generation, not execution-time errors. This makes Q11 on standard TPC-H
   report misleading "GPU" times that are actually CPU + overhead.

2. **Q01 regression on standard TPC-H**: VARCHAR GROUP BY on `l_returnflag`/`l_linestatus`
   adds GPU overhead. With integer-encoded schema (mod), Q01 reaches break-even (1.0x).
   At SF=100 on GH200, Q01 is a clear GPU win (~7x) due to HBM3 bandwidth + weaker CPU.

3. **Paper uses integer-encoded schema**: The ~8x claim in the README uses modified TPC-H
   with all dates/categoricals as BIGINT, not standard TPC-H with VARCHAR/DATE columns.
   Standard TPC-H produces lower speedups due to string hashing and type conversion overhead.

## How to Reproduce

### Standard TPC-H (quick)

```bash
cd /home/dnguyen56/sirius
bash bench_tpch_gpu_processing.sh --sf 10 --runs 3
```

The script auto-detects GPU memory, generates TPC-H data if missing, moves
`~/.sirius/sirius.cfg` aside for gpu_processing, and restores it after.

### Integer-encoded TPC-H (mod)

```bash
cd /home/dnguyen56/sirius

# 1. Generate standard TPC-H SF=10 if missing
SIRIUS_CONFIG_FILE= LD_LIBRARY_PATH=".pixi/envs/default/lib:$LD_LIBRARY_PATH" \
  ./build/release/duckdb -unsigned test_datasets/tpch_sf10.duckdb -c \
  "INSTALL tpch; LOAD tpch; CALL dbgen(sf=10);"

# 2. Convert to integer-encoded schema
SIRIUS_CONFIG_FILE= LD_LIBRARY_PATH=".pixi/envs/default/lib:$LD_LIBRARY_PATH" \
  ./build/release/duckdb -unsigned test_datasets/tpch_sf10_mod.duckdb -c "
ATTACH 'test_datasets/tpch_sf10.duckdb' AS src (READ_ONLY);
CREATE TABLE region AS SELECT * FROM src.region;
CREATE TABLE nation AS SELECT * FROM src.nation;
CREATE TABLE supplier AS SELECT s_suppkey, s_name, s_address, s_nationkey, s_phone,
  CAST(s_acctbal AS DOUBLE) as s_acctbal, s_comment FROM src.supplier;
CREATE TABLE part AS SELECT p_partkey, p_name,
  CAST(CASE WHEN p_mfgr='Manufacturer#1' THEN 1 WHEN p_mfgr='Manufacturer#2' THEN 2
  WHEN p_mfgr='Manufacturer#3' THEN 3 WHEN p_mfgr='Manufacturer#4' THEN 4
  ELSE 5 END AS BIGINT) as p_mfgr,
  CAST(hash(p_brand)%100 AS BIGINT) as p_brand,
  CAST(hash(p_type)%200 AS BIGINT) as p_type,
  CAST(p_size AS BIGINT) as p_size,
  CAST(hash(p_container)%50 AS BIGINT) as p_container,
  CAST(p_retailprice AS DOUBLE) as p_retailprice, p_comment FROM src.part;
CREATE TABLE partsupp AS SELECT ps_partkey, ps_suppkey,
  CAST(ps_availqty AS DOUBLE) as ps_availqty,
  CAST(ps_supplycost AS DOUBLE) as ps_supplycost, ps_comment FROM src.partsupp;
CREATE TABLE customer AS SELECT c_custkey, c_name, c_address, c_nationkey, c_phone,
  CAST(c_acctbal AS DOUBLE) as c_acctbal,
  CAST(CASE c_mktsegment WHEN 'AUTOMOBILE' THEN 0 WHEN 'BUILDING' THEN 1
  WHEN 'FURNITURE' THEN 2 WHEN 'HOUSEHOLD' THEN 3 ELSE 4 END AS BIGINT) as c_mktsegment,
  c_comment FROM src.customer;
CREATE TABLE orders AS SELECT o_orderkey, o_custkey,
  CAST(CASE o_orderstatus WHEN 'F' THEN 0 WHEN 'O' THEN 1 ELSE 2 END AS BIGINT) as o_orderstatus,
  CAST(o_totalprice AS DOUBLE) as o_totalprice,
  CAST(year(o_orderdate)*10000+month(o_orderdate)*100+day(o_orderdate) AS BIGINT) as o_orderdate,
  CAST(CASE WHEN o_orderpriority='1-URGENT' THEN 0 WHEN o_orderpriority='2-HIGH' THEN 1
  WHEN o_orderpriority='3-MEDIUM' THEN 2 WHEN o_orderpriority='4-NOT SPECIFIED' THEN 3
  ELSE 4 END AS BIGINT) as o_orderpriority,
  CAST(hash(o_clerk)%10000 AS BIGINT) as o_clerk,
  CAST(o_shippriority AS BIGINT) as o_shippriority, o_comment FROM src.orders;
CREATE TABLE lineitem AS SELECT l_orderkey, l_partkey, l_suppkey,
  CAST(l_linenumber AS BIGINT) as l_linenumber,
  CAST(l_quantity AS DOUBLE) as l_quantity,
  CAST(l_extendedprice AS DOUBLE) as l_extendedprice,
  CAST(l_discount AS DOUBLE) as l_discount, CAST(l_tax AS DOUBLE) as l_tax,
  CAST(CASE l_returnflag WHEN 'A' THEN 0 WHEN 'N' THEN 1 ELSE 2 END AS BIGINT) as l_returnflag,
  CAST(CASE l_linestatus WHEN 'F' THEN 0 ELSE 1 END AS BIGINT) as l_linestatus,
  CAST(year(l_shipdate)*10000+month(l_shipdate)*100+day(l_shipdate) AS BIGINT) as l_shipdate,
  CAST(year(l_commitdate)*10000+month(l_commitdate)*100+day(l_commitdate) AS BIGINT) as l_commitdate,
  CAST(year(l_receiptdate)*10000+month(l_receiptdate)*100+day(l_receiptdate) AS BIGINT) as l_receiptdate,
  CAST(CASE l_shipinstruct WHEN 'DELIVER IN PERSON' THEN 0 WHEN 'COLLECT COD' THEN 1
  WHEN 'NONE' THEN 2 ELSE 3 END AS BIGINT) as l_shipinstruct,
  CAST(CASE l_shipmode WHEN 'AIR' THEN 0 WHEN 'REG AIR' THEN 1 WHEN 'MAIL' THEN 2
  WHEN 'SHIP' THEN 3 WHEN 'TRUCK' THEN 4 WHEN 'RAIL' THEN 5 ELSE 6 END AS BIGINT) as l_shipmode,
  l_comment FROM src.lineitem;
DETACH src;"

# 3. Run mod queries using bench script pointed at mod DB
#    (or run scripts/tpch-queries-mod.sql manually with gpu_buffer_init)
```
