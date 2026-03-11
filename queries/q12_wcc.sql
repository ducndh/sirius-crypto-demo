-- Q12: Weakly Connected Components on transaction graph
-- "How many independent clusters of addresses exist?"
-- Category: Graph Analytics (GPU)
-- Showcases: Entity clustering — addresses in the same component likely share an entity
--
-- Prerequisite: tx_edges table (see q11)

SELECT component, COUNT(*) AS cluster_size
FROM gpu_graph_wcc('tx_edges', 'src', 'dst')
GROUP BY component
ORDER BY cluster_size DESC
LIMIT 20;
