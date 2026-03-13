-- Major ERC-20 token decimals for value normalization
-- raw_value / 10^decimals = human-readable amount
CREATE TABLE IF NOT EXISTS token_decimals (
    token_address VARCHAR,
    symbol VARCHAR,
    decimals INTEGER
);
INSERT INTO token_decimals VALUES
    ('0xdac17f958d2ee523a2206206994597c13d831ec7', 'USDT', 6),
    ('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 'USDC', 6),
    ('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 'WETH', 18),
    ('0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 'WBTC', 8),
    ('0x6b175474e89094c44da98b954eedeac495271d0f', 'DAI', 18),
    ('0x514910771af9ca656af840dff83e8264ecf986ca', 'LINK', 18),
    ('0x1f9840a85d5af5bf1d1762f925bdaddc4201f984', 'UNI', 18),
    ('0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', 'AAVE', 18),
    ('0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce', 'SHIB', 18),
    ('0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0', 'MATIC', 18),
    ('0x4fabb145d64652a948d72533023f6e7a623c7c53', 'BUSD', 18),
    ('0x45804880de22913dafe09f4980848ece6ecbaf78', 'PAXG', 18),
    ('0x0000000000000000000000000000000000000000', 'ETH', 18);
