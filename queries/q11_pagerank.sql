-- Q11: PageRank on Ethereum transaction graph
-- "Which addresses are most important in the network?"
-- Category: Graph Analytics (GPU)
-- Showcases: cuGraph PageRank via SQL — no export, no Python round-trip
--
-- Prerequisite: CREATE TABLE tx_edges AS
--   SELECT from_address AS src, to_address AS dst
--   FROM eth_transactions WHERE value > 0;

SELECT * FROM gpu_graph_pagerank('tx_edges', 'src', 'dst')
ORDER BY pagerank DESC
LIMIT 20;
