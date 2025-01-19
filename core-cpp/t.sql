--
CREATE TABLE IF NOT EXISTS default.fundings_curr_2025_01_12 (
    ticker_raw String,
    symbol String,
    fundingRate Float64,
    nextFundingTime Int64,
    ts Int64,
    ex String,
    k String
)
ENGINE = TinyLog;
-- fill coingecko_coin_id for hyperliquid
INSERT INTO default.t_fut_to_coingecko_coin_id VALUES
    ('hyperliquid', 'ARB', 'USD', 'arbitrum', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MAV', 'USD', 'maverick-protocol', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'FIL', 'USD', 'filecoin', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'PURR', 'USD', 'purr-2', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'NEAR', 'USD', 'near', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'SEI', 'USD', 'sei', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ONDO', 'USD', 'ondo', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ATOM', 'USD', 'cosmos-hub', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'TRX', 'USD', 'tron', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ZK', 'USD', 'zksync', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'CHILLGUY', 'USD', 'just-a-chill-guy', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'DYDX', 'USD', 'dydx-chain', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'APT', 'USD', 'aptos', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ILV', 'USD', 'illuvium', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'kLUNC', 'USD', 'terra-luna-classic', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'DOT', 'USD', 'polkadot', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'PYTH', 'USD', 'pyth-network', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'OGN', 'USD', 'origin-protocol', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BANANA', 'USD', 'banana-gun', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'RSR', 'USD', 'reserve-rights', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'kBONK', 'USD', 'bonk', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BTC', 'USD', 'bitcoin', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ALT', 'USD', 'altlayer', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'UNI', 'USD', 'uniswap', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'kNEIRO', 'USD', 'neiro-3', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BADGER', 'USD', 'badger', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'OP', 'USD', 'optimism', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'UNIBOT', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'KAS', 'USD', 'kaspa', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'WIF', 'USD', 'dogwifhat', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'STRK', 'USD', 'starknet', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ZEREBRO', 'USD', 'zerebro', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BLUR', 'USD', 'blur', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ZRO', 'USD', 'layerzero', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'RENDER', 'USD', 'render', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MINA', 'USD', 'mina-protocol', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'XAI', 'USD', 'xai', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'OMNI', 'USD', 'omni-network', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'DOGE', 'USD', 'dogecoin', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'PENDLE', 'USD', 'pendle', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BOME', 'USD', 'book-of-meme', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MATIC', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'RUNE', 'USD', 'thorchain', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'LISTA', 'USD', 'lista-dao', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'SPX', 'USD', 'spx6900', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'TRUMP', 'USD', 'official-trump', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'AIXBT', 'USD', 'aixbt-by-virtuals', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'kDOGS', 'USD', 'dogs', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'PIXEL', 'USD', 'pixels', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'S', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'RDNT', 'USD', 'radiant-capital', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'HBAR', 'USD', 'hedera', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ZETA', 'USD', 'zetachain', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'FET', 'USD', 'artificial-superintelligence-alliance', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ALGO', 'USD', 'algorand', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MOVE', 'USD', 'movement', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'CYBER', 'USD', 'cyberconnect', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'PENGU', 'USD', 'pudgy-penguins', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'CATI', 'USD', 'catizen', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'XRP', 'USD', 'xrp', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'SUI', 'USD', 'sui', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'CELO', 'USD', 'celo', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MANTA', 'USD', 'manta-network', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'SOL', 'USD', 'solana', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'IOTA', 'USD', 'iota', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'YGG', 'USD', 'yield-guild-games', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'NOT', 'USD', 'notcoin', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'HYPE', 'USD', 'hyperliquid', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'VIRTUAL', 'USD', 'virtual-protocol', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BNT', 'USD', 'bancor-network', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'USTC', 'USD', 'terraclassicusd', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'GRIFFAIN', 'USD', 'griffain', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ACE', 'USD', 'fusionist', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'POPCAT', 'USD', 'popcat', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'FTT', 'USD', 'ftx-token', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'LTC', 'USD', 'litecoin', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MEW', 'USD', 'mew', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'GMT', 'USD', 'stepn', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'REZ', 'USD', 'renzo', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'GOAT', 'USD', 'goatseus-maximus', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'AVAX', 'USD', 'avalanche', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'FXS', 'USD', 'frax-share', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'INJ', 'USD', 'injective', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BSV', 'USD', 'bitcoin-sv', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ETH', 'USD', 'ethereum', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'FARTCOIN', 'USD', 'fartcoin', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'W', 'USD', 'wormhole', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BLZ', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'TRB', 'USD', 'tellor-tributes', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'PEOPLE', 'USD', 'constitutiondao', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'GAS', 'USD', 'gas', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'CAKE', 'USD', 'pancakeswap', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'AAVE', 'USD', 'aave', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'LDO', 'USD', 'lido-dao', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'CRV', 'USD', 'curve-dao-token', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ARK', 'USD', 'ark', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ETHFI', 'USD', 'ether-fi', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BIGTIME', 'USD', 'big-time', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'REQ', 'USD', 'request-network', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'OX', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'IO', 'USD', 'io-net', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MNT', 'USD', 'mantle', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BRETT', 'USD', 'brett-2', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'LOOM', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'AR', 'USD', 'arweave', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MERL', 'USD', 'merlin-chain', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ETC', 'USD', 'ethereum-classic', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'HMSTR', 'USD', 'hamster-kombat', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'SNX', 'USD', 'synthetix-network-token', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'APE', 'USD', 'apecoin', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ADA', 'USD', 'cardano', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'IMX', 'USD', 'immutable-x', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ZEN', 'USD', 'horizen', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BNB', 'USD', 'bnb', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'NEIROETH', 'USD', 'neiro-on-eth', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'XLM', 'USD', 'stellar', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'SHIA', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'STX', 'USD', 'stacks', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'SUPER', 'USD', 'superverse', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MORPHO', 'USD', 'morpho', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'COMP', 'USD', 'compound', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ENS', 'USD', 'ethereum-name-service', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'EIGEN', 'USD', 'eigenlayer', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'RNDR', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'RLB', 'USD', 'rollbit-coin', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'SAGA', 'USD', 'saga', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'GALA', 'USD', 'gala', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'TURBO', 'USD', 'turbo', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'NTRN', 'USD', 'neutron', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ORBS', 'USD', 'orbs', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'TAO', 'USD', 'bittensor', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'SAND', 'USD', 'the-sandbox', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'USUAL', 'USD', 'usual', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'UMA', 'USD', 'uma', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'TON', 'USD', 'toncoin', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'AI', 'USD', 'sleepless-ai', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'NEO', 'USD', 'neo', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'JTO', 'USD', 'jito', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'TIA', 'USD', 'celestia', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'HPOS', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'WLD', 'USD', 'worldcoin', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ENA', 'USD', 'ethena', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'GRASS', 'USD', 'grass', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'LINK', 'USD', 'chainlink', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MEME', 'USD', 'meme', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BLAST', 'USD', 'blast', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'POLYX', 'USD', 'polymesh', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'SCR', 'USD', 'scroll', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MKR', 'USD', 'maker', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BIO', 'USD', 'bio-protocol', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'NFTI', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ME', 'USD', 'magic-eden', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'GMX', 'USD', 'gmx', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'STRAX', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'JUP', 'USD', 'jupiter', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'FRIEND', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MAVIA', 'USD', 'heroes-of-mavia', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'POL', 'USD', 'pol-ex-matic', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'STG', 'USD', 'stargate-finance', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'SUSHI', 'USD', 'sushi', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MOODENG', 'USD', 'moo-deng', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'CANTO', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'ORDI', 'USD', 'ordi', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'TNSR', 'USD', 'tensor', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'CFX', 'USD', 'conflux', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'BCH', 'USD', 'bitcoin-cash', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'kPEPE', 'USD', 'pepe', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'kSHIB', 'USD', 'shiba-inu', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'PNUT', 'USD', 'peanut-the-squirrel', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'FTM', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'kFLOKI', 'USD', 'floki', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'DYM', 'USD', 'dymension', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'PANDORA', 'USD', '', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'AI16Z', 'USD', 'ai16z', 'by-hands-on-2024-01-19'),
    ('hyperliquid', 'MYRO', 'USD', 'myro', 'by-hands-on-2024-01-19');
-- figure out fundings differences
WITH fundings_bybit AS (
    SELECT t1.*, t2.fundingInterval / 60 funding_hours
    FROM default.fundings_curr_2025_01_12 t1
    INNER JOIN (
        SELECT *
        FROM default.t_bybit_2025_01_02
        WHERE k = 'fut'
    ) t2
        ON t1.symbol = t2.s
    WHERE ex = 'bybit'
), fundings_gateio AS (
    SELECT t1.*, funding_hours
    FROM default.fundings_curr_2025_01_12 t1
    INNER JOIN (
        SELECT s, JSONExtract(contract_raw, 'funding_interval', 'Int64') / 60 / 60 funding_hours
        FROM default.t_gateio_2025_01_02
        WHERE k = 'fut'
    ) t2
        ON t1.symbol = t2.s
    WHERE ex = 'gateio'
), fundings_hyperliquid AS (
    SELECT *, 1 funding_hours
    FROM default.fundings_curr_2025_01_12
    WHERE ex = 'hyperliquid'
), fundings_arkm AS (
    SELECT *, 1 funding_hours
    FROM default.fundings_curr_2025_01_12
    WHERE ex = 'arkm'
), fundings AS (
    SELECT *
    FROM (
        SELECT
            t1.ex,
            replaceRegexpOne(replaceRegexpOne(replaceRegexpOne(replaceRegexpOne(symbol, '_PERP$', ''), '_USDT$', ''), '_USDC$', ''), 'USDT$', '') token_1,
            t1.fundingRate / t1.funding_hours funding_rate_1h,
            row_number() OVER (PARTITION BY ex, symbol ORDER BY ts DESC) AS rank,
            t2.coingecko_coin_id
        FROM (
            SELECT * FROM fundings_bybit
            UNION ALL
            SELECT * FROM fundings_gateio
            UNION ALL
            SELECT * FROM fundings_hyperliquid
            UNION ALL
            SELECT * FROM fundings_arkm
        ) t1
        INNER JOIN default.t_fut_to_coingecko_coin_id t2
            ON t1.ex = t2.ex AND token_1 = t2.base
        WHERE replaceAll(t2.coingecko_coin_id, ' ', '') != ''
    )
    WHERE rank = 1
), fundings_min AS (
    SELECT *
    FROM (
        SELECT *, row_number() OVER (PARTITION BY coingecko_coin_id ORDER BY funding_rate_1h ASC) t_rank
        FROM fundings
    )
    WHERE t_rank = 1
), fundings_max AS (
    SELECT *
    FROM (
        SELECT *, row_number() OVER (PARTITION BY coingecko_coin_id ORDER BY funding_rate_1h DESC) t_rank
        FROM fundings
    )
    WHERE t_rank = 1
)
SELECT
    t1.coingecko_coin_id,
    t1.token_1,
    t1.ex ex_min,
    t2.ex ex_max,
    t1.funding_rate_1h fund_1h_min,
    t2.funding_rate_1h fund_1h_max,
    truncate(fund_1h_max - fund_1h_min, 4) diff_abs_1h,
    fund_1h_min * 8 fund_8h_min,
    fund_1h_max * 8 fund_8h_max,
    truncate(fund_8h_max - fund_8h_min, 4) diff_abs_8h
FROM fundings_min t1
INNER JOIN fundings_max t2
    ON t1.coingecko_coin_id = t2.coingecko_coin_id
ORDER BY abs(diff_abs_1h) DESC
LIMIT 10
--
CREATE TABLE default.t_mexc_2025_01_02 (
    `symbol_raw` String,
    `s` String,
    `k` String,
    `orderTypes` Array(String),
    `isSpotTradingAllowed` Boolean,
    `permissions` Array(String),
    `filters` Array(String),
    `fullName` String,
    `tradeSideType` UInt8
)
ENGINE = TinyLog;
INSERT INTO default.t_mexc_2025_01_02
SELECT
    symbol_raw,
    JSONExtractString(symbol_raw, 'symbol') s,
    'spot' k,
    JSONExtract(symbol_raw, 'orderTypes', 'Array(String)') orderTypes,
    JSONExtractBool(symbol_raw, 'isSpotTradingAllowed') isSpotTradingAllowed,
    JSONExtract(symbol_raw, 'permissions', 'Array(String)') permissions,
    JSONExtract(symbol_raw, 'filters', 'Array(String)') filters,
    JSONExtractString(symbol_raw, 'fullName') fullName,
    JSONExtract(symbol_raw, 'tradeSideType', 'UInt8') tradeSideType
FROM (
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'symbols')) symbol_raw
    FROM url('https://api.mexc.com/api/v3/exchangeInfo', 'JSONAsString')
);
CREATE TABLE default.t_gateio_2025_01_02 (
    `current_pair_raw` String,
    `currency_raw` String,
    `contract_raw` String,
    `s` String,
    `k` String,
    `trade_status` String,
    `sell_start` DateTime,
    `buy_start` DateTime,
    `delisted` Boolean,
    `trade_disabled` Boolean,
    `in_delisting` Boolean
)
ENGINE = TinyLog;
INSERT INTO default.t_gateio_2025_01_02
SELECT
    '{}' current_pair_raw,
    '{}' currency_raw,
    json contract_raw,
    JSONExtractString(contract_raw, 'name') s,
    'fut' k,
    '' trade_status,
    toDateTime(0) sell_start,
    toDateTime(0) buy_start,
    false delisted,
    false trade_disabled,
    JSONExtractBool(contract_raw, 'in_delisting') in_delisting
FROM url('https://api.gateio.ws/api/v4/futures/usdt/contracts', JSONAsString);
INSERT INTO default.t_gateio_2025_01_02
SELECT
    current_pair_raw,
    currency_raw,
    '{}' contract_raw,
    JSONExtractString(current_pair_raw, 'id') s,
    'spot' k,
    JSONExtractString(current_pair_raw, 'trade_status') trade_status,
    toDateTime(JSONExtract(current_pair_raw, 'sell_start', 'UInt64')) sell_start,
    toDateTime(JSONExtract(current_pair_raw, 'buy_start', 'UInt64')) buy_start,
    JSONExtractBool(currency_raw, 'delisted') delisted,
    JSONExtractBool(currency_raw, 'trade_disabled') trade_disabled,
    false in_delisting
FROM (
    SELECT json current_pair_raw
    FROM url('https://api.gateio.ws/api/v4/spot/currency_pairs', 'JSONAsString')
) t1
LEFT JOIN (
    SELECT json currency_raw
    FROM url('https://api.gateio.ws/api/v4/spot/currencies', 'JSONAsString')
) t2
    ON JSONExtractString(current_pair_raw, 'base') =
        JSONExtractString(currency_raw, 'currency');
CREATE TABLE default.t_bybit_2025_01_02 (
    `symbol_raw` String,
    `s` String,
    `k` String,
    `baseCoin` String,
    `quoteCoin` String,
    `status` String,
    `fundingInterval` UInt64,
    `settleCoin` String,
    `upperFundingRate` Float64,
    `lowerFundingRate` Float64,
    `isPreListing` Boolean
)
ENGINE = TinyLog;
INSERT INTO default.t_bybit_2025_01_02
SELECT
    symbol_raw,
    JSONExtractString(symbol_raw, 'symbol') s,
    'fut' k,
    JSONExtractString(symbol_raw, 'baseCoin') baseCoin,
    JSONExtractString(symbol_raw, 'quoteCoin') quoteCoin,
    JSONExtractString(symbol_raw, 'status') status,
    JSONExtract(symbol_raw, 'fundingInterval', 'UInt64') fundingInterval,
    JSONExtractString(symbol_raw, 'settleCoin') settleCoin,
    JSONExtract(symbol_raw, 'upperFundingRate', 'Float64') upperFundingRate,
    JSONExtract(symbol_raw, 'lowerFundingRate', 'Float64') lowerFundingRate,
    JSONExtractBool(symbol_raw, 'isPreListing') isPreListing
FROM (
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'result'), 'list')) symbol_raw
    FROM url('https://api.bybit.com/v5/market/instruments-info?category=linear', JSONAsString)
);
INSERT INTO default.t_bybit_2025_01_02
SELECT
    symbol_raw,
    JSONExtractString(symbol_raw, 'symbol') s,
    'spot' k,
    JSONExtractString(symbol_raw, 'baseCoin') baseCoin,
    JSONExtractString(symbol_raw, 'quoteCoin') quoteCoin,
    JSONExtractString(symbol_raw, 'status') status,
    0 fundingInterval,
    '' settleCoin,
    .0 upperFundingRate,
    .0 lowerFundingRate,
    false isPreListing
FROM (
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'result'), 'list')) symbol_raw
    FROM url('https://api.bybit.com/v5/market/instruments-info?category=spot', JSONAsString)
);
---
CREATE TABLE default.t_cmc_exchange_market_pairs_2025_01_05 (
    `pair_raw` String,
    `exchangeId` UInt64,
    `exchangeSlug` String,
    `marketPair` String,
    `category` String,
    `marketUrl` String,
    `baseSymbol` String,
    `baseCurrencyId` UInt64,
    `quoteSymbol` String,
    `quoteCurrencyId` UInt64
)
ENGINE = TinyLog;
INSERT INTO default.t_cmc_exchange_market_pairs_2025_01_05
SELECT
    pair_raw,
    JSONExtract(pair_raw, 'exchangeId', 'UInt64') exchangeId,
    JSONExtractString(pair_raw, 'exchangeSlug') exchangeSlug,
    JSONExtractString(pair_raw, 'marketPair') marketPair,
    JSONExtractString(pair_raw, 'category') category,
    JSONExtractString(pair_raw, 'marketUrl') marketUrl,
    JSONExtractString(pair_raw, 'baseSymbol') baseSymbol,
    JSONExtract(pair_raw, 'baseCurrencyId', 'UInt64') baseCurrencyId,
    JSONExtractString(pair_raw, 'quoteSymbol') quoteSymbol,
    JSONExtract(pair_raw, 'quoteCurrencyId', 'UInt64') quoteCurrencyId
FROM (
    WITH 1 AS start_val, 1000 AS limit_val
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=mexc&category=spot&start=1&limit=1000', JSONAsString)
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=mexc&category=spot&start=1001&limit=1000', JSONAsString)
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=mexc&category=spot&start=2001&limit=1000', JSONAsString)
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=gate-io&category=perpetual&start=1&limit=200', JSONAsString)
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=gate-io&category=perpetual&start=201&limit=200', JSONAsString)
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=gate-io&category=perpetual&start=401&limit=200', JSONAsString)
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=gate-io&category=spot&start=1&limit=1000', JSONAsString)
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=gate-io&category=spot&start=1001&limit=1000', JSONAsString)
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=gate-io&category=spot&start=2001&limit=1000', JSONAsString)
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=gate-io&category=spot&start=3001&limit=1000', JSONAsString)
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=bybit&category=perpetual&start=1&limit=200', JSONAsString)
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=bybit&category=perpetual&start=201&limit=200', JSONAsString)
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=bybit&category=perpetual&start=401&limit=200', JSONAsString)
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'data'), 'marketPairs')) pair_raw
    FROM url('https://api.coinmarketcap.com/data-api/v3/exchange/market-pairs/latest?slug=bybit&category=spot&start=1&limit=1000', JSONAsString)
);
---
CREATE TABLE default.t_cmc_cryptocurrency_map_2025_01_05 (
    `cryptocurrency_raw` String,
    `id` UInt64,
    `symbol` String
)
ENGINE = TinyLog;
INSERT INTO default.t_cmc_cryptocurrency_map_2025_01_05
SELECT
    cryptocurrency_raw,
    JSONExtract(cryptocurrency_raw, 'id', 'UInt64') id,
    JSONExtractString(cryptocurrency_raw, 'symbol') symbol
FROM (
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'data')) cryptocurrency_raw
    FROM url('https://pro-api.coinmarketcap.com/v1/cryptocurrency/map?start=1&limit=5000', JSONAsString, headers('X-CMC_PRO_API_KEY'='be43125a-574a-46bb-8f5e-db2e8d204adf'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'data')) cryptocurrency_raw
    FROM url('https://pro-api.coinmarketcap.com/v1/cryptocurrency/map?start=5001&limit=5000', JSONAsString, headers('X-CMC_PRO_API_KEY'='be43125a-574a-46bb-8f5e-db2e8d204adf'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'data')) cryptocurrency_raw
    FROM url('https://pro-api.coinmarketcap.com/v1/cryptocurrency/map?start=10001&limit=5000', JSONAsString, headers('X-CMC_PRO_API_KEY'='be43125a-574a-46bb-8f5e-db2e8d204adf'))
);
---
CREATE TABLE default.t_cmc_exchange_map_2025_01_05 (
    `exchange_raw` String,
    `id` UInt64,
    `slug` String
)
ENGINE = TinyLog;
INSERT INTO default.t_cmc_exchange_map_2025_01_05
SELECT
    exchange_raw,
    JSONExtract(exchange_raw, 'id', 'UInt64') id,
    JSONExtractString(exchange_raw, 'slug') slug
FROM (
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'data')) exchange_raw
    FROM url('https://pro-api.coinmarketcap.com/v1/exchange/map?start=1&limit=5000', JSONAsString, headers('X-CMC_PRO_API_KEY'='be43125a-574a-46bb-8f5e-db2e8d204adf'))
);
---
CREATE TABLE default.t_coingecko_coins_list_2025_01_05 (
    `coin_raw` String,
    `id` String,
    `symbol` String,
    `name` String
)
ENGINE = TinyLog;
INSERT INTO default.t_coingecko_coins_list_2025_01_05
SELECT
    coin_raw,
    JSONExtractString(coin_raw, 'id') id,
    JSONExtractString(coin_raw, 'symbol') symbol,
    JSONExtractString(coin_raw, 'name') name
FROM (
    SELECT json coin_raw
    FROM url('https://api.coingecko.com/api/v3/coins/list', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
);
---
CREATE TABLE default.t_coingecko_exchanges_list_2025_01_05 (
    `ex_raw` String,
    `id` String,
    `name` String
)
ENGINE = TinyLog;
INSERT INTO default.t_coingecko_exchanges_list_2025_01_05
SELECT
    ex_raw,
    JSONExtractString(ex_raw, 'id') id,
    JSONExtractString(ex_raw, 'name') name
FROM (
    SELECT json ex_raw
    FROM url('https://api.coingecko.com/api/v3/exchanges/list', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
);
---
CREATE TABLE default.t_coingecko_exchanges_tickers_2025_01_05 (
    `ticker_raw` String,
    `base` String,
    `target` String,
    `trade_url` String,
    `k` String,
    `market_identifier` String
)
ENGINE = TinyLog;
-- for i in $(seq 0 36); do echo "SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=$i', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))\nUNION ALL"; done
INSERT INTO default.t_coingecko_exchanges_tickers_2025_01_05
SELECT
    ticker_raw,
    JSONExtractString(ticker_raw, 'base') base,
    JSONExtractString(ticker_raw, 'target') target,
    JSONExtractString(ticker_raw, 'trade_url') trade_url,
    'spot' k,
    JSONExtractString(JSONExtractRaw(ticker_raw, 'market'), 'identifier') market_identifier
FROM (
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=0', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=1', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=2', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=3', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=4', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=5', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=6', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=7', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=8', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=9', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=10', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=11', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=12', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=13', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=14', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=15', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=16', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=17', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=18', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=19', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=20', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=21', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=22', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=23', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=24', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=25', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=26', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=27', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=28', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=29', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=30', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=31', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=32', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=33', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=34', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=35', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/gate/tickers?page=36', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
);
INSERT INTO default.t_coingecko_exchanges_tickers_2025_01_05
SELECT
    ticker_raw,
    JSONExtractString(ticker_raw, 'base') base,
    JSONExtractString(ticker_raw, 'target') target,
    JSONExtractString(ticker_raw, 'trade_url') trade_url,
    'fut' k,
    'gate_futures' market_identifier
FROM (
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw
    FROM url(
        'https://api.coingecko.com/api/v3/derivatives/exchanges/gate_futures?include_tickers=all',
        JSONAsString,
        headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme')
    )
);
-- for i in $(seq 0 7); do echo "SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/bybit_spot/tickers?page=$i', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))\nUNION ALL"; done
INSERT INTO default.t_coingecko_exchanges_tickers_2025_01_05
SELECT
    ticker_raw,
    JSONExtractString(ticker_raw, 'base') base,
    JSONExtractString(ticker_raw, 'target') target,
    JSONExtractString(ticker_raw, 'trade_url') trade_url,
    'spot' k,
    JSONExtractString(JSONExtractRaw(ticker_raw, 'market'), 'identifier') market_identifier
FROM (
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/bybit_spot/tickers?page=0', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/bybit_spot/tickers?page=1', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/bybit_spot/tickers?page=2', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/bybit_spot/tickers?page=3', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/bybit_spot/tickers?page=4', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/bybit_spot/tickers?page=5', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/bybit_spot/tickers?page=6', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/bybit_spot/tickers?page=7', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
);
INSERT INTO default.t_coingecko_exchanges_tickers_2025_01_05
SELECT
    ticker_raw,
    JSONExtractString(ticker_raw, 'base') base,
    JSONExtractString(ticker_raw, 'target') target,
    JSONExtractString(ticker_raw, 'trade_url') trade_url,
    'fut' k,
    'bybit' market_identifier
FROM (
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw
    FROM url(
        'https://api.coingecko.com/api/v3/derivatives/exchanges/bybit?include_tickers=all',
        JSONAsString,
        headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme')
    )
);
-- for i in $(seq 0 29); do echo "SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=$i', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))\nUNION ALL"; done
INSERT INTO default.t_coingecko_exchanges_tickers_2025_01_05
SELECT
    ticker_raw,
    JSONExtractString(ticker_raw, 'base') base,
    JSONExtractString(ticker_raw, 'target') target,
    JSONExtractString(ticker_raw, 'trade_url') trade_url,
    'spot' k,
    JSONExtractString(JSONExtractRaw(ticker_raw, 'market'), 'identifier') market_identifier
FROM (
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=0', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=1', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=2', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=3', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=4', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=5', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=6', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=7', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=8', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=9', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=10', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=11', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=12', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=13', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=14', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=15', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=16', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=17', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=18', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=19', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=20', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=21', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=22', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=23', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=24', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=25', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=26', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=27', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=28', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
    UNION ALL
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'tickers')) ticker_raw FROM url('https://api.coingecko.com/api/v3/exchanges/mxc/tickers?page=29', JSONAsString, headers('x-cg-demo-api-key'='CG-SW1M45WZhgX1R29iEfWJYCme'))
);
--- select tokens for workout
WITH spot AS (
    SELECT baseSymbol, groupArray(exchangeSlug) exchanges
    FROM default.t_cmc_exchange_market_pairs_2025_01_05
    WHERE category = 'spot'
    GROUP BY baseSymbol
), fut AS (
    SELECT baseSymbol, groupArray(exchangeSlug) exchanges
    FROM default.t_cmc_exchange_market_pairs_2025_01_05
    WHERE category = 'perpetual'
    GROUP BY baseSymbol
)
SELECT
    baseSymbol,
    arrayCompact(arraySort(arrayConcat(spot.exchanges, fut.exchanges))) AS exchanges
FROM spot
INNER JOIN fut
    ON spot.baseSymbol = fut.baseSymbol;
--- select spot map to coingecko_coin_id
SELECT
    *,
    JSONExtractString(ticker_raw, 'coin_id') coingecko_coin_id,
    concat('https://www.coingecko.com/en/coins/', coingecko_coin_id) coingecko_url
FROM default.t_coingecko_exchanges_tickers_2025_01_05
WHERE k = 'fut' AND market_identifier = 'bybit'
LIMIT 1
\G;
---
CREATE TABLE default.t_fut_to_coingecko_coin_id (
    `ex` String,
    `base` String,
    `target` String,
    `coingecko_coin_id` String,
    `how_appeared` String
)
ENGINE = TinyLog;
---
INSERT INTO default.t_fut_to_coingecko_coin_id
-- join bybit fut+spot and figure out diff-rel
WITH fut_prices AS (
    SELECT
        base,
        target,
        JSONExtract(ticker_raw, 'last', 'Float64') last_price
    FROM default.t_coingecko_exchanges_tickers_2025_01_05
    WHERE (k = 'fut') AND (market_identifier = 'bybit') AND (target = 'USDT')
        AND startsWith(base, '10') = 0
), spot_prices AS (
    SELECT
        base,
        target,
        last_price,
        coingecko_coin_id
    FROM (
        SELECT
            *,
            JSONExtract(ticker_raw, 'last', 'Float64') last_price,
            JSONExtractString(ticker_raw, 'coin_id') coingecko_coin_id,
            ROW_NUMBER() OVER (
                PARTITION BY base, target
                ORDER BY toDateTime(replace(JSONExtractString(ticker_raw, 'timestamp'), '+00:00', '')) DESC
            ) AS rank
        FROM default.t_coingecko_exchanges_tickers_2025_01_05
        WHERE (k = 'spot') AND (market_identifier = 'bybit_spot') AND (target = 'USDT')
    )
    WHERE rank = 1
)
SELECT 'bybit' ex, base, target, coingecko_coin_id, 'matched-with-spot-on-spread-2025-01-06' how_appeared
FROM (
SELECT
    tf.base, tf.target, ts.base, ts.target, tf.last_price AS fut_last_price, ts.last_price AS spot_last_price,
    truncate(abs((tf.last_price - ts.last_price) / ts.last_price * 100.0), 2) diff_rel_abs,
    ts.coingecko_coin_id
FROM fut_prices tf
FULL OUTER JOIN spot_prices ts
    ON tf.base = ts.base AND tf.target = ts.target
-- WHERE ts.base = '' OR ts.target = '' -- only fut
-- WHERE tf.base = '' OR tf.target = '' -- only spot
WHERE tf.base != '' AND ts.target != '' AND diff_rel_abs < 2
ORDER BY diff_rel_abs
);
---
INSERT INTO default.t_fut_to_coingecko_coin_id VALUES
('bybit', 'TOMI', 'USDT', '', 'by-hands-on-2024-01-06'),
('bybit', 'FITFI', 'USDT', '', 'by-hands-on-2024-01-06'),
('bybit', 'PIXFI', 'USDT', '', 'by-hands-on-2024-01-06'),
('bybit', 'ZKF', 'USDT', '', 'by-hands-on-2024-01-06'),
('bybit', 'LFT', 'USDT', '', 'by-hands-on-2024-01-06'),
('bybit', 'FB', 'USDT', 'fractal-bitcoin', 'by-hands-on-2024-01-06'),
('bybit', 'LIT', 'USDT', 'litentry', 'by-hands-on-2024-01-06'),
('bybit', 'REEF', 'USDT', 'reef', 'by-hands-on-2024-01-06'),
('bybit', 'CVC', 'USDT', 'civic', 'by-hands-on-2024-01-06'),
('bybit', 'CVX', 'USDT', 'convex-finance', 'by-hands-on-2024-01-06'),
('bybit', 'AERGO', 'USDT', 'aergo', 'by-hands-on-2024-01-06'),
('bybit', 'AI', 'USDT', 'sleepless-ai', 'by-hands-on-2024-01-06'),
('bybit', 'AI16Z', 'USDT', 'ai16z', 'by-hands-on-2024-01-06'),
('bybit', 'AIXBT', 'USDT', 'aixbt-by-virtuals', 'by-hands-on-2024-01-06'),
('bybit', 'AKRO', 'USDT', '', 'by-hands-on-2024-01-06'),
('bybit', 'AKT', 'USDT', 'akash-network', 'by-hands-on-2024-01-06'),
('bybit', 'ALEO', 'USDT', 'aleo', 'by-hands-on-2024-01-06'),
('bybit', 'ALICE', 'USDT', 'my-neighbor-alice', 'by-hands-on-2024-01-06'),
('bybit', 'ALPACA', 'USDT', 'alpaca-finance', 'by-hands-on-2024-01-06'),
('bybit', 'ALPHA', 'USDT', 'stella', 'by-hands-on-2024-01-06'),
('bybit', 'AMB', 'USDT', 'airdao', 'by-hands-on-2024-01-06'),
('bybit', 'ANT', 'USDT', '', 'by-hands-on-2024-01-06'),
('bybit', 'API3', 'USDT', 'api3', 'by-hands-on-2024-01-06'),
('bybit', 'ARK', 'USDT', 'ark', 'by-hands-on-2024-01-06'),
('bybit', 'ARPA', 'USDT', 'arpa', 'by-hands-on-2024-01-06'),
('bybit', 'ASTR', 'USDT', 'astar', 'by-hands-on-2024-01-06'),
('bybit', 'ATA', 'USDT', 'automata', 'by-hands-on-2025-01-06'),
('bybit', 'AUCTION', 'USDT', 'bounce', 'by-hands-on-2025-01-06'),
('bybit', 'AUDIO', 'USDT', 'audius', 'by-hands-on-2025-01-06'),
('bybit', 'BADGER', 'USDT', 'badger', 'by-hands-on-2025-01-06'),
('bybit', 'BAKE', 'USDT', 'bakeryswap', 'by-hands-on-2025-01-06'),
('bybit', 'BAL', 'USDT', 'balancer', 'by-hands-on-2025-01-06'),
('bybit', 'BANANA', 'USDT', 'banana-gun', 'by-hands-on-2025-01-06'),
('bybit', 'BAND', 'USDT', 'band-protocol', 'by-hands-on-2025-01-06'),
('bybit', 'BENDOG', 'USDT', 'ben-the-dog', 'by-hands-on-2025-01-06'),
('bybit', 'BIGTIME', 'USDT', 'big-time', 'by-hands-on-2025-01-06'),
('bybit', 'BILLY', 'USDT', 'billy', 'by-hands-on-2025-01-06'),
('bybit', 'BIO', 'USDT', 'bio-protocol', 'by-hands-on-2025-01-06'),
('bybit', 'BLUE', 'USDT', 'bluefin', 'by-hands-on-2025-01-06'),
('bybit', 'BLZ', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'BNX', 'USDT', 'binaryx', 'by-hands-on-2025-01-06'),
('bybit', 'BOND', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'BSV', 'USDT', 'bitcoin-sv', 'by-hands-on-2025-01-06'),
('bybit', 'BSW', 'USDT', 'biswap', 'by-hands-on-2025-01-06'),
('bybit', 'CANTO', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'CEEK', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'CELR', 'USDT', 'celer-network', 'by-hands-on-2025-01-06'),
('bybit', 'CETUS', 'USDT', 'cetus-protocol', 'by-hands-on-2025-01-06'),
('bybit', 'CFX', 'USDT', 'conflux', 'by-hands-on-2025-01-06'),
('bybit', 'CHESS', 'USDT', 'tranchess', 'by-hands-on-2025-01-06'),
('bybit', 'CHR', 'USDT', 'chromia', 'by-hands-on-2025-01-06'),
('bybit', 'CKB', 'USDT', 'nervos-network', 'by-hands-on-2025-01-06'),
('bybit', 'COMBO', 'USDT', 'combo', 'by-hands-on-2025-01-06'),
('bybit', 'COS', 'USDT', 'contentos', 'by-hands-on-2025-01-06'),
('bybit', 'COTI', 'USDT', 'coti', 'by-hands-on-2025-01-06'),
('bybit', 'COVAL', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'COW', 'USDT', 'cow-protocol', 'by-hands-on-2025-01-06'),
('bybit', 'CRO', 'USDT', 'cronos', 'by-hands-on-2025-01-06'),
('bybit', 'CTK', 'USDT', 'shentu', 'by-hands-on-2025-01-06'),
('bybit', 'ACE', 'USDT', 'fusionist', 'by-hands-on-2025-01-06'),
('bybit', 'ACT', 'USDT', 'act-i-the-ai-prophecy', 'by-hands-on-2025-01-06'),
('bybit', 'ACX', 'USDT', 'across-protocol', 'by-hands-on-2025-01-06'),
('bybit', 'DAO', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'DAR', 'USDT', 'mines-of-dalarnia', 'by-hands-on-2025-01-06'),
('bybit', 'DASH', 'USDT', 'dash', 'by-hands-on-2025-01-06'),
('bybit', 'DATA', 'USDT', 'streamr', 'by-hands-on-2025-01-06'),
('bybit', 'DENT', 'USDT', 'dent', 'by-hands-on-2025-01-06'),
('bybit', 'DEXE', 'USDT', 'dexe', 'by-hands-on-2025-01-06'),
('bybit', 'DODO', 'USDT', 'dodo', 'by-hands-on-2025-01-06'),
('bybit', 'DOG', 'USDT', 'dog-go-to-the-moon-runes-2', 'by-hands-on-2025-01-06'),
('bybit', 'DUSK', 'USDT', 'dusk', 'by-hands-on-2025-01-06'),
('bybit', 'EDU', 'USDT', 'open-campus', 'by-hands-on-2025-01-06'),
('bybit', 'FARTCOIN', 'USDT', 'fartcoin', 'by-hands-on-2025-01-06'),
('bybit', 'FDUSD', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'FIO', 'USDT', 'fio-protocol', 'by-hands-on-2025-01-06'),
('bybit', 'FLM', 'USDT', 'flamingo-finance', 'by-hands-on-2025-01-06'),
('bybit', 'FLUX', 'USDT', 'flux-zelcash', 'by-hands-on-2025-01-06'),
('bybit', 'FORTH', 'USDT', 'ampleforth-governance-token', 'by-hands-on-2025-01-06'),
('bybit', 'FRONT', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'FTN', 'USDT', 'fasttoken', 'by-hands-on-2025-01-06'),
('bybit', 'FUN', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'FWOG', 'USDT', 'fwog', 'by-hands-on-2025-01-06'),
('bybit', 'GAL', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'GAS', 'USDT', 'gas', 'by-hands-on-2025-01-06'),
('bybit', 'GEMS', 'USDT', 'gems-vip', 'by-hands-on-2025-01-06'),
('bybit', 'GFT', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'GIGA', 'USDT', 'gigachad-2', 'by-hands-on-2025-01-06'),
('bybit', 'GLM', 'USDT', 'golem', 'by-hands-on-2025-01-06'),
('bybit', 'GME', 'USDT', 'gme', 'by-hands-on-2025-01-06'),
('bybit', 'GNO', 'USDT', 'gnosis', 'by-hands-on-2025-01-06'),
('bybit', 'GOMINING', 'USDT', 'gomining-token', 'by-hands-on-2025-01-06'),
('bybit', 'GRIFFAIN', 'USDT', 'griffain', 'by-hands-on-2025-01-06'),
('bybit', 'GTC', 'USDT', 'gitcoin', 'by-hands-on-2025-01-06'),
('bybit', 'HIFI', 'USDT', 'hifi-finance', 'by-hands-on-2025-01-06'),
('bybit', 'HIGH', 'USDT', 'highstreet', 'by-hands-on-2025-01-06'),
('bybit', 'HIPPO', 'USDT', 'sudeng', 'by-hands-on-2025-01-06'),
('bybit', 'HIVE', 'USDT', 'hive', 'by-hands-on-2025-01-06'),
('bybit', 'HYPE', 'USDT', 'hyperliquid', 'by-hands-on-2025-01-06'),
('bybit', 'IDEX', 'USDT', 'idex', 'by-hands-on-2025-01-06'),
('bybit', 'ILV', 'USDT', 'illuvium', 'by-hands-on-2025-01-06'),
('bybit', 'IOST', 'USDT', 'iost', 'by-hands-on-2025-01-06'),
('bybit', 'IOTA', 'USDT', 'iota', 'by-hands-on-2025-01-06'),
('bybit', 'IOTX', 'USDT', 'iotex', 'by-hands-on-2025-01-06'),
('bybit', 'JOE', 'USDT', 'joe', 'by-hands-on-2025-01-06'),
('bybit', 'KEY', 'USDT', 'selfkey', 'by-hands-on-2025-01-06'),
('bybit', 'KLAY', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'KNC', 'USDT', 'kyber-network-crystal', 'by-hands-on-2025-01-06'),
('bybit', 'KOMA', 'USDT', 'koma-inu', 'by-hands-on-2025-01-06'),
('bybit', 'LINA', 'USDT', 'linear', 'by-hands-on-2025-01-06'),
('bybit', 'LISTA', 'USDT', 'lista-dao', 'by-hands-on-2025-01-06'),
('bybit', '1CAT', 'USDT', 'bitcoin-cats', 'by-hands-on-2025-01-06'),
('bybit', 'CTSI', 'USDT', 'cartesi', 'by-hands-on-2025-01-06'),
('bybit', 'LPT', 'USDT', 'livepeer', 'by-hands-on-2025-01-06'),
('bybit', 'LQTY', 'USDT', 'liquity', 'by-hands-on-2025-01-06'),
('bybit', 'LSK', 'USDT', 'lisk', 'by-hands-on-2025-01-06'),
('bybit', 'LTO', 'USDT', 'lto-network', 'by-hands-on-2025-01-06'),
('bybit', 'LUMIA', 'USDT', 'lumia', 'by-hands-on-2025-01-06'),
('bybit', 'LUNA2', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'MANEKI', 'USDT', 'maneki', 'by-hands-on-2025-01-06'),
('bybit', 'MAPO', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'MATIC', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'MAV', 'USDT', 'maverick-protocol', 'by-hands-on-2025-01-06'),
('bybit', 'MAX', 'USDT', 'matr1x', 'by-hands-on-2025-01-06'),
('bybit', 'MBL', 'USDT', 'moviebloc', 'by-hands-on-2025-01-06'),
('bybit', 'MDT', 'USDT', 'measurable-data-token', 'by-hands-on-2025-01-06'),
('bybit', 'METIS', 'USDT', 'metis-token', 'by-hands-on-2025-01-06'),
('bybit', 'MOBILE', 'USDT', 'helium-mobile', 'by-hands-on-2025-01-06'),
('bybit', 'MOODENG', 'USDT', 'moo-deng', 'by-hands-on-2025-01-06'),
('bybit', 'MOTHER', 'USDT', 'mother-iggy', 'by-hands-on-2025-01-06'),
('bybit', 'MTL', 'USDT', 'metal-dao', 'by-hands-on-2025-01-06'),
('bybit', 'NEIROETH', 'USDT', 'neiro-on-eth', 'by-hands-on-2025-01-06'),
('bybit', 'NEO', 'USDT', 'neo', 'by-hands-on-2025-01-06'),
('bybit', 'NFP', 'USDT', 'nfprompt', 'by-hands-on-2025-01-06'),
('bybit', 'NKN', 'USDT', 'nkn', 'by-hands-on-2025-01-06'),
('bybit', 'NMR', 'USDT', 'numeraire', 'by-hands-on-2025-01-06'),
('bybit', 'NTRN', 'USDT', 'neutron', 'by-hands-on-2025-01-06'),
('bybit', 'NULS', 'USDT', 'nuls', 'by-hands-on-2025-01-06'),
('bybit', 'OG', 'USDT', 'og-fan-token', 'by-hands-on-2025-01-06'),
('bybit', 'OGN', 'USDT', 'origin-protocol', 'by-hands-on-2025-01-06'),
('bybit', 'OM', 'USDT', 'mantra', 'by-hands-on-2025-01-06'),
('bybit', 'ONG', 'USDT', 'ontology-gas', 'by-hands-on-2025-01-06'),
('bybit', 'ONT', 'USDT', 'ontology', 'by-hands-on-2025-01-06'),
('bybit', 'ORBS', 'USDT', 'orbs', 'by-hands-on-2025-01-06'),
('bybit', 'ORCA', 'USDT', 'orca', 'by-hands-on-2025-01-06'),
('bybit', 'ORN', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'OSMO', 'USDT', 'osmosis', 'by-hands-on-2025-01-06'),
('bybit', 'OXT', 'USDT', 'orchid-protocol', 'by-hands-on-2025-01-06'),
('bybit', 'PEAQ', 'USDT', 'peaq', 'by-hands-on-2025-01-06'),
('bybit', 'PENG', 'USDT', 'peng', 'by-hands-on-2025-01-06'),
('bybit', 'PHA', 'USDT', 'phala-network', 'by-hands-on-2025-01-06'),
('bybit', 'PHB', 'USDT', 'phoenix', 'by-hands-on-2025-01-06'),
('bybit', 'PIXEL', 'USDT', 'pixels', 'by-hands-on-2025-01-06'),
('bybit', 'POLYX', 'USDT', 'polymesh', 'by-hands-on-2025-01-06'),
('bybit', 'POWR', 'USDT', 'power-ledger', 'by-hands-on-2025-01-06'),
('bybit', 'PROM', 'USDT', 'prom', 'by-hands-on-2025-01-06'),
('bybit', 'PROS', 'USDT', 'prosper', 'by-hands-on-2025-01-06'),
('bybit', 'PUNDU', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'PYR', 'USDT', 'vulcan-forged', 'by-hands-on-2025-01-06'),
('bybit', 'QI', 'USDT', 'benqi', 'by-hands-on-2025-01-06'),
('bybit', 'QUICK', 'USDT', 'quickswap', 'by-hands-on-2025-01-06'),
('bybit', 'RAD', 'USDT', 'radworks', 'by-hands-on-2025-01-06'),
('bybit', 'ZCX', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'ZEC', 'USDT', 'zcash', 'by-hands-on-2025-01-06'),
('bybit', 'ZEUS', 'USDT', 'zeus-network', 'by-hands-on-2025-01-06'),
('bybit', 'REQ', 'USDT', 'request-network', 'by-hands-on-2025-01-06'),
('bybit', 'REZ', 'USDT', 'renzo', 'by-hands-on-2025-01-06'),
('bybit', 'RIF', 'USDT', 'rsk-infrastructure-framework', 'by-hands-on-2025-01-06'),
('bybit', 'RIFSOL', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'RLC', 'USDT', 'iexec-rlc', 'by-hands-on-2025-01-06'),
('bybit', 'RNDR', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'RON', 'USDT', 'ronin', 'by-hands-on-2025-01-06'),
('bybit', 'RSR', 'USDT', 'reserve-rights', 'by-hands-on-2025-01-06'),
('bybit', 'SAGA', 'USDT', 'saga', 'by-hands-on-2025-01-06'),
('bybit', 'SFP', 'USDT', 'safepal', 'by-hands-on-2025-01-06'),
('bybit', 'SHIB1000', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'SILLY', 'USDT', 'silly-dragon', 'by-hands-on-2025-01-06'),
('bybit', 'SKL', 'USDT', 'skale', 'by-hands-on-2025-01-06'),
('bybit', 'SLERF', 'USDT', 'slerf', 'by-hands-on-2025-01-06'),
('bybit', 'SLF', 'USDT', 'self-chain', 'by-hands-on-2025-01-06'),
('bybit', 'SNT', 'USDT', 'status', 'by-hands-on-2025-01-06'),
('bybit', 'STEEM', 'USDT', 'steem', 'by-hands-on-2025-01-06'),
('bybit', 'STMX', 'USDT', 'stormx', 'by-hands-on-2025-01-06'),
('bybit', 'STORJ', 'USDT', 'storj', 'by-hands-on-2025-01-06'),
('bybit', 'STPT', 'USDT', 'stp-network', 'by-hands-on-2025-01-06'),
('bybit', 'SUPER', 'USDT', 'superverse', 'by-hands-on-2025-01-06'),
('bybit', 'SXP', 'USDT', 'solar-2', 'by-hands-on-2025-01-06'),
('bybit', 'SYN', 'USDT', 'synapse', 'by-hands-on-2025-01-06'),
('bybit', 'SYS', 'USDT', 'syscoin', 'by-hands-on-2025-01-06'),
('bybit', 'TAO', 'USDT', 'bittensor', 'by-hands-on-2025-01-06'),
('bybit', 'THE', 'USDT', 'thena', 'by-hands-on-2025-01-06'),
('bybit', 'TLM', 'USDT', 'alien-worlds', 'by-hands-on-2025-01-06'),
('bybit', 'TRB', 'USDT', 'tellor-tributes', 'by-hands-on-2025-01-06'),
('bybit', 'TROY', 'USDT', 'troy', 'by-hands-on-2025-01-06'),
('bybit', 'TRU', 'USDT', 'truefi', 'by-hands-on-2025-01-06'),
('bybit', 'UNFI', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'URO', 'USDT', 'urolithin-a', 'by-hands-on-2025-01-06'),
('bybit', 'USUAL', 'USDT', 'usual', 'by-hands-on-2025-01-06'),
('bybit', 'VELODROME', 'USDT', 'velodrome-finance', 'by-hands-on-2025-01-06'),
('bybit', 'VET', 'USDT', 'vechain', 'by-hands-on-2025-01-06'),
('bybit', 'VGX', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'VIDT', 'USDT', 'vidt-dao', 'by-hands-on-2025-01-06'),
('bybit', 'VOXEL', 'USDT', 'voxies', 'by-hands-on-2025-01-06'),
('bybit', 'VTHO', 'USDT', 'vethor-token', 'by-hands-on-2025-01-06'),
('bybit', 'XCH', 'USDT', 'chia', 'by-hands-on-2025-01-06'),
('bybit', 'XCN', 'USDT', 'onyxcoin', 'by-hands-on-2025-01-06'),
('bybit', 'XMR', 'USDT', 'monero', 'by-hands-on-2025-01-06'),
('bybit', 'XNO', 'USDT', 'nano', 'by-hands-on-2025-01-06'),
('bybit', 'XRD', 'USDT', 'radix', 'by-hands-on-2025-01-06'),
('bybit', 'XVG', 'USDT', 'verge', 'by-hands-on-2025-01-06'),
('bybit', 'XVS', 'USDT', 'venus', 'by-hands-on-2025-01-06'),
('bybit', 'YGG', 'USDT', 'yield-guild-games', 'by-hands-on-2025-01-06'),
('bybit', 'ZBCN', 'USDT', 'zebec-network', 'by-hands-on-2025-01-06'),
('bybit', 'RARE', 'USDT', 'superrare', 'by-hands-on-2025-01-06'),
('bybit', 'RAYDIUM', 'USDT', 'raydium', 'by-hands-on-2025-01-06'),
('bybit', 'LOOM', 'USDT', '', 'by-hands-on-2025-01-06'),
('bybit', 'BLAST', 'USDT', 'blast', 'by-hands-on-2025-01-01'),
('bybit', 'DGB', 'USDT', 'digibyte', 'by-hands-on-2025-01-01'),
('bybit', 'DOGS', 'USDT', 'dogs', 'by-hands-on-2025-01-01'),
('bybit', 'DOP1', 'USDT', '', 'by-hands-on-2025-01-01'),
('bybit', 'HMSTR', 'USDT', 'hamster-kombat', 'by-hands-on-2025-01-01'),
('bybit', 'HOT', 'USDT', 'holo', 'by-hands-on-2025-01-01'),
('bybit', 'LEVER', 'USDT', 'leverfi', 'by-hands-on-2025-01-01'),
('bybit', 'MEME', 'USDT', 'meme', 'by-hands-on-2025-01-01'),
('bybit', 'MEMEFI', 'USDT', 'memefi', 'by-hands-on-2025-01-01'),
('bybit', 'MEW', 'USDT', 'mew', 'by-hands-on-2025-01-01'),
('bybit', 'MVL', 'USDT', 'mass-vehicle-ledger', 'by-hands-on-2025-01-01'),
('bybit', 'MYRIA', 'USDT', 'myria', 'by-hands-on-2025-01-01'),
('bybit', 'NOT', 'USDT', 'notcoin', 'by-hands-on-2025-01-01'),
('bybit', 'SC', 'USDT', 'siacoin', 'by-hands-on-2025-01-01'),
('bybit', 'SLP', 'USDT', 'smooth-love-potion', 'by-hands-on-2025-01-01'),
('bybit', 'SPELL', 'USDT', 'spell-token', 'by-hands-on-2025-01-01'),
('bybit', 'SWEAT', 'USDT', 'sweat-economy', 'by-hands-on-2025-01-01'),
('bybit', 'VRA', 'USDT', 'verasity', 'by-hands-on-2025-01-01'),
('bybit', 'XRP', 'USDT', 'xrp', 'by-hands-on-2025-01-01'),
('bybit', 'XTZ', 'USDT', 'tezos', 'by-hands-on-2025-01-01'),
('bybit', 'YFI', 'USDT', 'yearn-finance', 'by-hands-on-2025-01-01'),
('bybit', 'ZEN', 'USDT', 'horizen', 'by-hands-on-2025-01-01'),
('bybit', 'ZETA', 'USDT', 'zetachain', 'by-hands-on-2025-01-01'),
('bybit', 'ZIL', 'USDT', 'zilliqa', 'by-hands-on-2025-01-01'),
('bybit', 'ZK', 'USDT', 'zksync', 'by-hands-on-2025-01-01'),
('bybit', 'ZRC', 'USDT', 'zircuit', 'by-hands-on-2025-01-01'),
('bybit', 'ZRO', 'USDT', 'layerzero', 'by-hands-on-2025-01-01'),
('bybit', 'ZRX', 'USDT', '0x', 'by-hands-on-2025-01-01');
---
INSERT INTO default.t_fut_to_coingecko_coin_id
WITH fut_prices AS (
    SELECT
        base,
        target,
        JSONExtract(ticker_raw, 'last', 'Float64') last_price
    FROM default.t_coingecko_exchanges_tickers_2025_01_05
    WHERE (k = 'fut') AND (market_identifier = 'gate_futures') AND (target = 'USDT')
        AND startsWith(base, '10') = 0
), spot_prices AS (
    SELECT
        base,
        target,
        last_price,
        coingecko_coin_id
    FROM (
        SELECT
            *,
            JSONExtract(ticker_raw, 'last', 'Float64') last_price,
            JSONExtractString(ticker_raw, 'coin_id') coingecko_coin_id,
            ROW_NUMBER() OVER (
                PARTITION BY base, target
                ORDER BY toDateTime(replace(JSONExtractString(ticker_raw, 'timestamp'), '+00:00', '')) DESC
            ) AS rank
        FROM default.t_coingecko_exchanges_tickers_2025_01_05
        WHERE (k = 'spot') AND (market_identifier = 'gate') AND (target = 'USDT')
    )
    WHERE rank = 1
)
SELECT 'gateio' ex, base, target, coingecko_coin_id, 'matched-with-spot-on-spread-2025-01-07' how_appeared
FROM (
SELECT
    tf.base, tf.target, ts.base, ts.target, tf.last_price AS fut_last_price, ts.last_price AS spot_last_price,
    truncate(abs((tf.last_price - ts.last_price) / ts.last_price * 100.0), 2) diff_rel_abs,
    ts.coingecko_coin_id
FROM fut_prices tf
FULL OUTER JOIN spot_prices ts
    ON tf.base = ts.base AND tf.target = ts.target
-- WHERE ts.base = '' OR ts.target = '' -- only fut
-- WHERE tf.base = '' OR tf.target = '' -- only spot
-- WHERE tf.base != '' AND ts.target != '' AND diff_rel_abs >= 2
WHERE tf.base != '' AND ts.target != '' AND diff_rel_abs < 2
ORDER BY diff_rel_abs
);
---
INSERT INTO default.t_fut_to_coingecko_coin_id VALUES
('gateio', 'DOGEGOV', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'AIOZ', 'USDT', 'aioz-network', 'by-hands-on-2025-01-07'),
('gateio', 'DEAI', 'USDT', 'zero1-labs', 'by-hands-on-2025-01-07'),
('gateio', 'L3', 'USDT', 'layer3', 'by-hands-on-2025-01-07'),
('gateio', 'BOME', 'USDT', 'book-of-meme', 'by-hands-on-2025-01-07'),
('gateio', 'FLIP', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'FET', 'USDT', 'artificial-superintelligence-alliance', 'by-hands-on-2025-01-07'),
('gateio', 'ALCX', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'NFP', 'USDT', 'nfprompt', 'by-hands-on-2025-01-07'),
('gateio', 'HIPPO', 'USDT', 'sudeng', 'by-hands-on-2025-01-07'),
('gateio', 'MOVE', 'USDT', 'movement', 'by-hands-on-2025-01-07'),
('gateio', 'KEY', 'USDT', 'selfkey', 'by-hands-on-2025-01-07'),
('gateio', 'CPOOL', 'USDT', 'clearpool', 'by-hands-on-2025-01-07'),
('gateio', 'DEGEN', 'USDT', 'degen-base', 'by-hands-on-2025-01-07'),
('gateio', 'DGB', 'USDT', 'digibyte', 'by-hands-on-2025-01-07'),
('gateio', 'BENDOG', 'USDT', 'ben-the-dog', 'by-hands-on-2025-01-07'),
('gateio', 'SPA', 'USDT', 'sperax', 'by-hands-on-2025-01-07'),
('gateio', 'MBOX', 'USDT', 'mobox', 'by-hands-on-2025-01-07'),
('gateio', 'OAX', 'USDT', 'oax', 'by-hands-on-2025-01-07'),
('gateio', 'MANEKI', 'USDT', 'maneki', 'by-hands-on-2025-01-07'),
('gateio', 'MGT', 'USDT', 'moongate', 'by-hands-on-2025-01-07'),
('gateio', 'GEAR', 'USDT', 'gearbox', 'by-hands-on-2025-01-07'),
('gateio', 'COOKIE', 'USDT', 'cookie', 'by-hands-on-2025-01-07'),
('gateio', 'MOBILE', 'USDT', 'helium-mobile', 'by-hands-on-2025-01-07'),
('gateio', 'AMB', 'USDT', 'airdao', 'by-hands-on-2025-01-07'),
('gateio', 'MEME', 'USDT', 'meme', 'by-hands-on-2025-01-07'),
('gateio', 'FWOG', 'USDT', 'fwog', 'by-hands-on-2025-01-07'),
('gateio', 'TLM', 'USDT', 'alien-worlds', 'by-hands-on-2025-01-07'),
('gateio', 'ULTI', 'USDT', 'ultiverse', 'by-hands-on-2025-01-07'),
('gateio', 'MOTHER', 'USDT', 'mother-iggy', 'by-hands-on-2025-01-07'),
('gateio', 'BENQI', 'USDT', 'benqi', 'by-hands-on-2025-01-07'),
('gateio', 'DOG', 'USDT', 'dog-go-to-the-moon-runes-2', 'by-hands-on-2025-01-07'),
('gateio', 'IOST', 'USDT', 'iost', 'by-hands-on-2025-01-07'),
('gateio', 'FTM', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'MEW', 'USDT', 'mew', 'by-hands-on-2025-01-07'),
('gateio', 'AIFUN', 'USDT', 'ai-agent-layer', 'by-hands-on-2025-01-07'),
('gateio', 'DHX', 'USDT', 'datahighway', 'by-hands-on-2025-01-07'),
('gateio', 'MOZ', 'USDT', 'lumoz', 'by-hands-on-2025-01-07'),
('gateio', 'MPLX', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'BARSIK', 'USDT', 'hasbulla-s-cat', 'by-hands-on-2025-01-07'),
('gateio', 'STMX', 'USDT', 'stormx', 'by-hands-on-2025-01-07'),
('gateio', 'CKB', 'USDT', 'nervos-network', 'by-hands-on-2025-01-07'),
('gateio', 'SC', 'USDT', 'siacoin', 'by-hands-on-2025-01-07'),
('gateio', 'GNS', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'BGSC', 'USDT', 'bugscoin', 'by-hands-on-2025-01-07'),
('gateio', 'SWARMS', 'USDT', 'swarms', 'by-hands-on-2025-01-07'),
('gateio', 'GROK', 'USDT', 'grok-2', 'by-hands-on-2025-01-07'),
('gateio', 'FUN', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'OKB', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'NOT', 'USDT', 'notcoin', 'by-hands-on-2025-01-07'),
('gateio', 'BLAST', 'USDT', 'blast', 'by-hands-on-2025-01-07'),
('gateio', 'LEVER', 'USDT', 'leverfi', 'by-hands-on-2025-01-07'),
('gateio', 'ORDER', 'USDT', 'orderly-network', 'by-hands-on-2025-01-07'),
('gateio', 'RWA', 'USDT', 'rwa-inc', 'by-hands-on-2025-01-07'),
('gateio', 'SCIHUB', 'USDT', 'sci-hub', 'by-hands-on-2025-01-07'),
('gateio', 'SLP', 'USDT', 'smooth-love-potion', 'by-hands-on-2025-01-07'),
('gateio', 'U2U', 'USDT', 'u2u-network', 'by-hands-on-2025-01-07'),
('gateio', 'FIS', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'ZBCN', 'USDT', 'zebec-network', 'by-hands-on-2025-01-07'),
('gateio', 'SWEAT', 'USDT', 'sweat-economy', 'by-hands-on-2025-01-07'),
('gateio', 'IQ', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'VTHO', 'USDT', 'vethor-token', 'by-hands-on-2025-01-07'),
('gateio', 'FARM', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'FITFI', 'USDT', 'step-app', 'by-hands-on-2025-01-07'),
('gateio', 'TROY', 'USDT', 'troy', 'by-hands-on-2025-01-07'),
('gateio', 'BBL', 'USDT', 'beoble', 'by-hands-on-2025-01-07'),
('gateio', 'FLT', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'NEIROCTO', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'MEMEFI', 'USDT', 'memefi', 'by-hands-on-2025-01-07'),
('gateio', 'FORT', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'PIXFI', 'USDT', 'pixelverse-xyz', 'by-hands-on-2025-01-07'),
('gateio', 'XCN', 'USDT', 'onyxcoin', 'by-hands-on-2025-01-07'),
('gateio', 'MYRIA', 'USDT', 'myria', 'by-hands-on-2025-01-07'),
('gateio', 'ACA', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'HMSTR', 'USDT', 'hamster-kombat', 'by-hands-on-2025-01-07'),
('gateio', 'VENOM', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'IRIS', 'USDT', 'irisnet', 'by-hands-on-2025-01-07'),
('gateio', 'VRA', 'USDT', 'verasity', 'by-hands-on-2025-01-07'),
('gateio', 'MBL', 'USDT', 'moviebloc', 'by-hands-on-2025-01-07'),
('gateio', 'MPC', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'REEF', 'USDT', 'reef', 'by-hands-on-2025-01-07'),
('gateio', 'HOT', 'USDT', 'holo', 'by-hands-on-2025-01-07'),
('gateio', 'POND', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'MSN', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'PATEX', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'BLOCK', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'NIBI', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'CLV', 'USDT', 'clover-finance', 'by-hands-on-2025-01-07'),
('gateio', 'SPELL', 'USDT', 'spell-token', 'by-hands-on-2025-01-07'),
('gateio', 'ZEN', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'GOATS', 'USDT', 'goats', 'by-hands-on-2025-01-07'),
('gateio', 'SLN', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'APU', 'USDT', 'apu-apustaja', 'by-hands-on-2025-01-07'),
('gateio', 'PBUX', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'DOP', 'USDT', 'data-ownership-protocol', 'by-hands-on-2025-01-07'),
('gateio', '1CAT', 'USDT', 'bitcoin-cats', 'by-hands-on-2025-01-07'),
('gateio', 'DENT', 'USDT', 'dent', 'by-hands-on-2025-01-07'),
('gateio', 'KARRAT', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'DAR', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'DUKO', 'USDT', 'duko', 'by-hands-on-2025-01-07'),
('gateio', 'ZKF', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'DOGS', 'USDT', 'dogs', 'by-hands-on-2025-01-07'),
('gateio', 'MAGA', 'USDT', 'maga-hat', 'by-hands-on-2025-01-07'),
('gateio', 'ZEROLEND', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'MAD', 'USDT', 'mad-2', 'by-hands-on-2025-01-07'),
('gateio', 'LUNC', 'USDT', 'terra-luna-classic', 'by-hands-on-2025-01-07'),
('gateio', 'QUBIC', 'USDT', 'qubic', 'by-hands-on-2025-01-07'),
('gateio', 'RACA', 'USDT', 'radio-caca', 'by-hands-on-2025-01-07'),
('gateio', 'RATS', 'USDT', 'rats', 'by-hands-on-2025-01-07'),
('gateio', 'LADYS', 'USDT', 'milady-meme-coin', 'by-hands-on-2025-01-07'),
('gateio', 'REKTCOIN', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'RIZZMAS', 'USDT', 'rizzmas', 'by-hands-on-2025-01-07'),
('gateio', 'PEIPEI', 'USDT', 'peipeicoin-vip', 'by-hands-on-2025-01-07'),
('gateio', 'SATS', 'USDT', 'sats-ordinals', 'by-hands-on-2025-01-07'),
('gateio', 'PEPE2', 'USDT', 'pepe-2-0', 'by-hands-on-2025-01-07'),
('gateio', 'GOLDENCAT', 'USDT', 'goldencat', 'by-hands-on-2025-01-07'),
('gateio', 'SHIB', 'USDT', 'shiba-inu', 'by-hands-on-2025-01-07'),
('gateio', 'MOODENGETH', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'FLOKI', 'USDT', 'floki', 'by-hands-on-2025-01-07'),
('gateio', 'SOON', 'USDT', 'soon', 'by-hands-on-2025-01-07'),
('gateio', 'ELON', 'USDT', 'dogelon-mars', 'by-hands-on-2025-01-07'),
('gateio', 'MEMETOON', 'USDT', 'memetoon', 'by-hands-on-2025-01-07'),
('gateio', 'STARDOGE', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'STARL', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'PEPE', 'USDT', 'pepe', 'by-hands-on-2025-01-07'),
('gateio', 'MOG', 'USDT', 'mog-coin', 'by-hands-on-2025-01-07'),
('gateio', 'MONKY', 'USDT', 'wise-monkey', 'by-hands-on-2025-01-07'),
('gateio', 'CWIF', 'USDT', 'catwifhat-2', 'by-hands-on-2025-01-07'),
('gateio', 'TOSHI', 'USDT', 'toshi', 'by-hands-on-2025-01-07'),
('gateio', 'CHEEMS', 'USDT', 'cheems-token', 'by-hands-on-2025-01-07'),
('gateio', 'CATS', 'USDT', 'cats-2', 'by-hands-on-2025-01-07'),
('gateio', 'CATDOG', 'USDT', 'cat-dog', 'by-hands-on-2025-01-07'),
('gateio', 'CAT', 'USDT', 'simons-cat', 'by-hands-on-2025-01-07'),
('gateio', 'BTT', 'USDT', 'bittorrent', 'by-hands-on-2025-01-07'),
('gateio', 'BONK', 'USDT', 'bonk', 'by-hands-on-2025-01-07'),
('gateio', 'WEN', 'USDT', 'wen-solana', 'by-hands-on-2025-01-07'),
('gateio', 'WHY', 'USDT', 'why', 'by-hands-on-2025-01-07'),
('gateio', 'WIN', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'WOLF', 'USDT', 'landwolf-0x67', 'by-hands-on-2025-01-07'),
('gateio', 'X', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'BITBOARD', 'USDT', 'bitboard', 'by-hands-on-2025-01-07'),
('gateio', 'XEC', 'USDT', 'ecash', 'by-hands-on-2025-01-07'),
('gateio', 'XEN', 'USDT', 'xen-crypto', 'by-hands-on-2025-01-07'),
('gateio', 'BEER', 'USDT', 'beercoin-2', 'by-hands-on-2025-01-07'),
('gateio', 'APEPE', 'USDT', 'ape-and-pepe', 'by-hands-on-2025-01-07'),
('gateio', 'IQ50', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'GFT', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'AGIX', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'XVG', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'ANT', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'BAIDOGE', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'CEL', 'USDT', 'celsius-network-token', 'by-hands-on-2025-01-07'),
('gateio', 'DASH', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'ESE', 'USDT', 'eesee', 'by-hands-on-2025-01-07'),
('gateio', 'FRONT', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'AKRO', 'USDT', 'kaon', 'by-hands-on-2025-01-07'),
('gateio', 'HOLD', 'USDT', 'holdcoin', 'by-hands-on-2025-01-07'),
('gateio', 'HYPE', 'USDT', 'hyperliquid', 'by-hands-on-2025-01-07'),
('gateio', 'KLAY', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'LINA', 'USDT', 'linear', 'by-hands-on-2025-01-07'),
('gateio', 'LL', 'USDT', 'lightlink', 'by-hands-on-2025-01-07'),
('gateio', 'MATIC', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'MBABYDOGE', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'RVN', 'USDT', 'ravencoin', 'by-hands-on-2025-01-07'),
('gateio', 'MPL', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'MUBI', 'USDT', 'multibit', 'by-hands-on-2025-01-07'),
('gateio', 'OCEAN', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'ORN', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'RNDR', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'ZEC', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'SNEK', 'USDT', 'snek', 'by-hands-on-2025-01-07'),
('gateio', 'TAOCAT', 'USDT', 'taocat-by-virtuals', 'by-hands-on-2025-01-07'),
('gateio', 'XMR', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'XNO', 'USDT', 'nano', 'by-hands-on-2025-01-07'),
('gateio', 'GAL', 'USDT', '', 'by-hands-on-2025-01-07'),
('gateio', 'MBABYNEIRO', 'USDT', '', 'by-hands-on-2025-01-07');
---
INSERT INTO default.t_fut_to_coingecko_coin_id VALUES
('arkm', 'BONK', 'USD', 'bonk', 'by-hands-on-2025-01-19'),
('arkm', 'RENDER', 'USD', 'render', 'by-hands-on-2025-01-19'),
('arkm', 'BTC', 'USD', 'bitcoin', 'by-hands-on-2025-01-19'),
('arkm', 'ETH', 'USD', 'ethereum', 'by-hands-on-2025-01-19'),
('arkm', 'TON', 'USD', 'toncoin', 'by-hands-on-2025-01-19'),
('arkm', 'SUI', 'USD', 'sui', 'by-hands-on-2025-01-19'),
('arkm', 'AVAX', 'USD', 'avalanche', 'by-hands-on-2025-01-19'),
('arkm', 'PEPE', 'USD', 'pepe', 'by-hands-on-2025-01-19'),
('arkm', 'ARKM', 'USD', 'arkham', 'by-hands-on-2025-01-19'),
('arkm', 'XRP', 'USD', 'xrp', 'by-hands-on-2025-01-19'),
('arkm', 'FLOKI', 'USD', 'floki', 'by-hands-on-2025-01-19'),
('arkm', 'DOGE', 'USD', 'dogecoin', 'by-hands-on-2025-01-19'),
('arkm', 'WIF', 'USD', 'dogwifhat', 'by-hands-on-2025-01-19'),
('arkm', 'FET', 'USD', 'artificial-superintelligence-alliance', 'by-hands-on-2025-01-19'),
('arkm', 'SOL', 'USD', 'solana', 'by-hands-on-2025-01-19');
