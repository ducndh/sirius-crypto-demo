-- Q01: Count transactions in a date range
-- "How many transactions happened in Q1 2024?"
-- Category: Scan / Filter
-- Showcases: GPU filter + COUNT on large table

SELECT COUNT(*) AS num_transactions
FROM eth_transactions
WHERE block_timestamp BETWEEN '2024-01-01' AND '2024-03-31';
