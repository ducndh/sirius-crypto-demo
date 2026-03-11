#!/usr/bin/env python3
"""Measure KuzuDB hot times (second run after warmup)."""
import os
import shutil
import csv
import time
import duckdb
import kuzu


def main():
    data_path = '/home/cc/sirius-crypto-demo/data/eth_transactions.parquet'
    limit = 500000
    tmp_dir = '/tmp/kuzu_bench_hot'
    db_dir = '/tmp/kuzu_bench_hot_db'

    # Clean
    shutil.rmtree(tmp_dir, ignore_errors=True)
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(tmp_dir, exist_ok=True)

    # Export edges
    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE edges AS
        SELECT from_address AS src, to_address AS dst
        FROM '{data_path}' WHERE value > 0 LIMIT {limit}
    """)
    vertices = con.execute("SELECT DISTINCT v FROM (SELECT src AS v FROM edges UNION SELECT dst AS v FROM edges)").fetchall()
    edges = con.execute("SELECT src, dst FROM edges").fetchall()
    top = con.execute("SELECT src FROM edges GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1").fetchone()[0]

    vertex_csv = os.path.join(tmp_dir, 'vertices.csv')
    edge_csv = os.path.join(tmp_dir, 'edges.csv')
    with open(vertex_csv, 'w', newline='') as f:
        w = csv.writer(f); w.writerow(['id'])
        for (v,) in vertices: w.writerow([v])
    with open(edge_csv, 'w', newline='') as f:
        w = csv.writer(f); w.writerow(['from', 'to'])
        for row in edges: w.writerow(row)
    con.close()
    print(f"{len(vertices)} vertices, {len(edges)} edges, BFS source: {top[:12]}...")

    # Setup KuzuDB
    db = kuzu.Database(db_dir)
    conn = kuzu.Connection(db)
    conn.execute("INSTALL algo"); conn.execute("LOAD EXTENSION algo")
    conn.execute("CREATE NODE TABLE Address(id STRING, PRIMARY KEY(id))")
    conn.execute("CREATE REL TABLE Edge(FROM Address TO Address)")
    conn.execute(f"COPY Address FROM '{vertex_csv}' (header=true)")
    conn.execute(f"COPY Edge FROM '{edge_csv}' (header=true)")
    conn.execute("CALL project_graph('G', ['Address'], ['Edge'])")

    # Warmup + hot runs
    algos = [
        ("PageRank", "CALL page_rank('G') RETURN node.id, rank ORDER BY rank DESC LIMIT 10"),
        ("WCC", "CALL wcc('G') RETURN group_id, count(*) AS s ORDER BY s DESC LIMIT 10"),
        ("BFS depth 2", f"MATCH (src:Address)-[e:Edge* SHORTEST 1..2]->(dst:Address) WHERE src.id = '{top}' RETURN dst.id, length(e) LIMIT 50"),
    ]

    for name, query in algos:
        # Warmup
        conn.execute(query).get_as_df()
        # Hot runs
        times = []
        for _ in range(3):
            t0 = time.perf_counter()
            conn.execute(query).get_as_df()
            times.append(time.perf_counter() - t0)
        best = min(times)
        print(f"{name:20s}  best={best*1000:.1f}ms  runs={[f'{t*1000:.1f}' for t in times]}")

    shutil.rmtree(db_dir, ignore_errors=True)
    shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == '__main__':
    main()
