#!/usr/bin/env python3
"""Benchmark KuzuDB (CPU) graph algorithms as baseline for Sirius GPU comparison.

Usage:
  python scripts/bench_kuzu_graph.py [--edges 500000]
"""
import argparse
import csv
import os
import shutil
import time

import duckdb
import kuzu


def export_edges(data_path, edge_csv, limit):
    """Export edges from parquet to CSV for KuzuDB bulk load."""
    con = duckdb.connect()
    # Extract unique vertices
    con.execute(f"""
        CREATE TABLE edges AS
        SELECT from_address AS src, to_address AS dst
        FROM '{data_path}' WHERE value > 0 LIMIT {limit}
    """)
    # Write vertex list
    vertices = con.execute("""
        SELECT DISTINCT v FROM (
            SELECT src AS v FROM edges UNION SELECT dst AS v FROM edges
        )
    """).fetchall()
    vertex_csv = edge_csv.replace('edges.csv', 'vertices.csv')
    with open(vertex_csv, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['id'])
        for (v,) in vertices:
            w.writerow([v])
    # Write edge list
    edges = con.execute("SELECT src, dst FROM edges").fetchall()
    with open(edge_csv, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['from', 'to'])
        for row in edges:
            w.writerow(row)
    print(f"Exported {len(vertices)} vertices, {len(edges)} edges")
    # Return a source vertex for BFS
    top = con.execute("SELECT src, COUNT(*) AS c FROM edges GROUP BY 1 ORDER BY 2 DESC LIMIT 1").fetchone()
    con.close()
    return top[0], len(edges), len(vertices)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--edges', type=int, default=500000)
    p.add_argument('--data', default='/home/cc/sirius-crypto-demo/data/eth_transactions.parquet')
    args = p.parse_args()

    tmp_dir = '/tmp/kuzu_bench'
    db_dir = '/tmp/kuzu_bench_db'
    os.makedirs(tmp_dir, exist_ok=True)
    edge_csv = os.path.join(tmp_dir, 'edges.csv')
    vertex_csv = os.path.join(tmp_dir, 'vertices.csv')

    print(f"Preparing {args.edges} edges...")
    source_vertex, n_edges, n_vertices = export_edges(args.data, edge_csv, args.edges)
    print(f"BFS source: {source_vertex}")

    # Clean up any previous DB
    if os.path.exists(db_dir):
        shutil.rmtree(db_dir)

    print("\nSetting up KuzuDB...")
    db = kuzu.Database(db_dir)
    conn = kuzu.Connection(db)

    # Schema
    conn.execute("CREATE NODE TABLE Address(id STRING, PRIMARY KEY (id))")
    conn.execute("CREATE REL TABLE Edge(FROM Address TO Address)")

    # Bulk load
    t0 = time.perf_counter()
    conn.execute(f"COPY Address FROM '{vertex_csv}' (header=true)")
    conn.execute(f"COPY Edge FROM '{edge_csv}' (header=true)")
    load_time = time.perf_counter() - t0
    print(f"Load time: {load_time:.3f}s")

    # Project graph for algorithms
    conn.execute("LOAD EXTENSION algo")
    conn.execute("CALL project_graph('G', ['Address'], ['Edge'])")

    results = {}

    # PageRank
    print("\nRunning PageRank...")
    t0 = time.perf_counter()
    res = conn.execute("CALL page_rank('G') RETURN node.id AS vertex, rank ORDER BY rank DESC LIMIT 10")
    rows = res.get_as_df()
    results['pagerank'] = time.perf_counter() - t0
    print(f"PageRank: {results['pagerank']:.3f}s")
    print(rows)

    # WCC
    print("\nRunning WCC...")
    t0 = time.perf_counter()
    res = conn.execute("CALL wcc('G') RETURN node.id AS vertex, group_id ORDER BY group_id LIMIT 10")
    rows = res.get_as_df()
    results['wcc'] = time.perf_counter() - t0
    print(f"WCC: {results['wcc']:.3f}s")
    print(rows)

    # BFS (shortest path from source to all reachable, depth-limited)
    print(f"\nRunning BFS (shortest paths from {source_vertex[:12]}..., depth 2)...")
    t0 = time.perf_counter()
    res = conn.execute(f"""
        MATCH (src:Address)-[e:Edge* SHORTEST 1..2]->(dst:Address)
        WHERE src.id = '{source_vertex}'
        RETURN dst.id AS vertex, length(e) AS distance
        ORDER BY distance, vertex LIMIT 50
    """)
    rows = res.get_as_df()
    results['bfs_depth2'] = time.perf_counter() - t0
    print(f"BFS (depth 2): {results['bfs_depth2']:.3f}s")
    print(rows)

    # Summary
    print("\n" + "=" * 50)
    print("KuzuDB Graph Benchmark Summary")
    print(f"  Vertices: {n_vertices:,}")
    print(f"  Edges:    {n_edges:,}")
    print(f"  Load:     {load_time:.3f}s")
    for algo, elapsed in results.items():
        print(f"  {algo:20s} {elapsed:.3f}s")
    print("=" * 50)

    # Cleanup
    shutil.rmtree(db_dir, ignore_errors=True)
    shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == '__main__':
    main()
