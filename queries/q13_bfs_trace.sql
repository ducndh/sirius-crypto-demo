-- Q13: BFS transaction trace from a flagged address
-- "Find all addresses within 3 hops of this address"
-- Category: Graph Analytics (GPU)
-- Showcases: Transaction tracing — the core compliance use case
--
-- Prerequisite: tx_edges table (see q11)
-- NOTE: Replace address with a real high-activity address from your dataset.
-- The address below is the top sender found in Q02.

SELECT vertex, distance, predecessor
FROM gpu_graph_bfs('tx_edges', 'src', 'dst',
                   '0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88', 3)
ORDER BY distance, vertex
LIMIT 50;
