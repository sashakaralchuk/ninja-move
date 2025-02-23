CREATE TABLE default.fundings_curr_2025_01_12 (
    ticker_raw String,
    symbol String,
    funding_rate Float64,
    next_funding_time UInt64,
    ts UInt64,
    ex String,
    k String,
    ts_write DateTime
)
PARTITION BY toDate(ts_write)
ORDER BY (ex, k, ts);
-- figure out fundings differences
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
--
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
    FROM url('https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000', JSONAsString)
)
UNION ALL
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
    FROM url('https://api.bybit.com/v5/market/instruments-info?category=spot&limit=1000', JSONAsString)
);
--
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
--
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
--
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
--
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
--
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
--
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
-- select tokens for workout
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
-- select spot map to coingecko_coin_id
SELECT
    *,
    JSONExtractString(ticker_raw, 'coin_id') coingecko_coin_id,
    concat('https://www.coingecko.com/en/coins/', coingecko_coin_id) coingecko_url
FROM default.t_coingecko_exchanges_tickers_2025_01_05
WHERE k = 'fut' AND market_identifier = 'bybit'
LIMIT 1
\G;
--
CREATE TABLE default.t_fut_to_coingecko_coin_id (
    `ex` String,
    `base` String,
    `target` String,
    `coingecko_coin_id` String,
    `notes` String
)
ENGINE = TinyLog;
--
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
--
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
--
CREATE TABLE default.t_paradex_2025_01_21
(
    `market_raw` String,
    `s` String,
    `k` String,
    `baseCoin` String,
    `quoteCoin` String,
    `fundingInterval` UInt64
)
ENGINE = TinyLog;
--
INSERT INTO default.t_paradex_2025_01_21
SELECT
    market_raw,
    JSONExtractString(market_raw, 'symbol') s,
    'fut' k,
    JSONExtractString(market_raw, 'base_currency') baseCoin,
    JSONExtractString(market_raw, 'quote_currency') quoteCoin,
    JSONExtractString(market_raw, 'funding_period_hours') fundingInterval
FROM (
    SELECT arrayJoin(JSONExtractArrayRaw(json, 'results')) market_raw
    FROM url('https://api.prod.paradex.trade/v1/markets', 'JSONAsString')
);
--
CREATE FUNCTION conv_symbol_to_token_dumb AS (s) -> replaceRegexpOne(
    replaceRegexpOne(
        replaceRegexpOne(
            replaceRegexpOne(
                replaceRegexpOne(
                    replaceRegexpOne(
                        replaceRegexpOne(
                            replaceRegexpOne(
                                replaceRegexpOne(
                                    replaceRegexpOne(
                                        replaceRegexpOne(
                                            replaceRegexpOne(
                                                replaceRegexpOne(
                                                    replaceRegexpOne(s, '-USDC$', ''),
                                                    '_USDC$', ''),
                                                'USDC', ''),
                                            '-USD-PERP', ''),
                                        '_PERP$', ''),
                                    '-USDT', ''),
                                '_USDT$', ''),
                            '_USDC$', ''),
                        'USDT$', '')
                    , '-USD$', ''),
                '^(100*)', ''),
            '_USD$', ''),
        'USD$', ''),
    'PERP$', ''
);
--
CREATE FUNCTION conv_symbol_to_token_v2 AS (ex, s) -> replaceRegexpOne(
  multiIf(
    ex = 'bingx', replaceRegexpOne(s, '-(USDT|USDC)$', ''),
    ex = 'gateio', replaceRegexpOne(s, '_USDT$', ''),
    ex = 'bitunix', replaceRegexpOne(s, 'USDT$', ''),
    ex = 'apex-pro', replaceRegexpOne(s, 'USDC$', ''),
    ex = 'paradex', replaceRegexpOne(s, '-USD-PERP$', ''),
    ex = 'bybit', replaceRegexpOne(s, '(USDT|PERP|(USDT)?-[0-9]{2}[A-Z]{3}[0-9]{2})$', ''),
    ex = 'mexc', replaceRegexpOne(s, '_(USDT|USD)$', ''),
    ex = 'arkm', replaceRegexpOne(s, '_USDT_PERP$', ''),
    ex = 'aevo', replaceRegexpOne(s, '-USD$', ''),
    ex = 'coinex', replaceRegexpOne(s, '(USDT|USDC|USD)$', ''),
    ex = 'hyperliquid', replaceRegexpOne(s, '_USDC$', ''),
    ex = 'apex-omni', replaceRegexpOne(s, 'USDT$', ''),
    ex = 'polynomial-fi', s,
    ''
  ),
  '^(100*)', ''
);
--
SELECT *
FROM default.t_fut_to_coingecko_coin_id
INTO OUTFILE '/tmp/dump_default_t_fut_to_coingecko_coin_id_on_2025_02_09.sql'
FORMAT SQLInsert;
--
CREATE TABLE default.spreads_curr_rasul_hasanov_2025_02_13 (
    obj_kind String,
    obj_raw String,
    s String,
    p_bid Float64,
    p_ask Float64,
    ex String,
    k String,
    ts_write DateTime
)
PARTITION BY toDate(ts_write)
ORDER BY (ex, k, ts_write);
--
INSERT INTO default.spreads_curr_rasul_hasanov_2025_02_13
SELECT
    'ticker' obj_kind,
    ticker_raw obj_raw,
    JSON_VALUE(ticker_raw, '$.symbol') s,
    toFloat64(JSON_VALUE(ticker_raw, '$.bid1Price')) p_bid,
    toFloat64(JSON_VALUE(ticker_raw, '$.ask1Price')) p_ask,
    'bybit' ex,
    'fut' k,
    NOW() ts_write
FROM (
    SELECT
        arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'result'), 'list')) ticker_raw
    FROM url('https://api.bybit.com/v5/market/tickers?category=linear', 'JSONAsString')
)
UNION ALL
SELECT
    'ticker' obj_kind,
    ticker_raw obj_raw,
    JSON_VALUE(ticker_raw, '$.symbol') s,
    toFloat64(JSON_VALUE(ticker_raw, '$.bid1Price')) p_bid,
    toFloat64(JSON_VALUE(ticker_raw, '$.ask1Price')) p_ask,
    'bybit' ex,
    'spot' k,
    NOW() ts_write
FROM (
    SELECT
        arrayJoin(JSONExtractArrayRaw(JSONExtractRaw(json, 'result'), 'list')) ticker_raw
    FROM url('https://api.bybit.com/v5/market/tickers?category=spot', 'JSONAsString')
)
UNION ALL
SELECT
    'book-ticker' obj_kind,
    json obj_raw,
    JSON_VALUE(json, '$.symbol') s,
    toFloat64(JSON_VALUE(json, '$.bidPrice')) last_bid,
    toFloat64(JSON_VALUE(json, '$.askPrice')) last_ask,
    'binance' ex,
    'fut' k,
    NOW() ts_write
FROM url('https://fapi.binance.com/fapi/v1/ticker/bookTicker', 'JSONAsString')
UNION ALL
SELECT
    'book-ticker' obj_kind,
    json obj_raw,
    JSON_VALUE(json, '$.symbol') s,
    toFloat64(JSON_VALUE(json, '$.bidPrice')) last_bid,
    toFloat64(JSON_VALUE(json, '$.askPrice')) last_ask,
    'binance' ex,
    'spot' k,
    NOW() ts_write
FROM url('https://api.binance.com/api/v1/ticker/bookTicker', 'JSONAsString');
--
CREATE TABLE default.t_ex_k_to_coingecko_coin_id (
    `ex` String,
    `k` String,
    `base` String,
    `target` String,
    `coingecko_coin_id` String,
    `notes` String
)
ENGINE = TinyLog;
--
INSERT INTO default.t_ex_k_to_coingecko_coin_id
SELECT ex, 'fut' k, base, target, coingecko_coin_id, notes
FROM default.t_fut_to_coingecko_coin_id
WHERE ex = 'bybit';
--
INSERT INTO default.t_ex_k_to_coingecko_coin_id
SELECT
    'bybit' ex,
    'spot' k,
    JSON_VALUE(ticker_raw, '$.base') base,
    JSON_VALUE(ticker_raw, '$.target') target,
    JSON_VALUE(ticker_raw, '$.coin_id') coingecko_coin_id,
    'added-from-coingecko-tickers-api-on-2025-02-14' notes
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
)
GROUP BY ex, k, base, target, coingecko_coin_id, notes;
--
INSERT INTO default.t_ex_k_to_coingecko_coin_id VALUES
('bybit', 'fut', 'B3', 'USD', 'b3', 'added-by-hands-on-2025-02-14'),
('bybit', 'fut', 'IP', 'USD', 'story', 'added-by-hands-on-2025-02-14'),
('bybit', 'fut', 'RONIN', 'USD', 'ronin', 'added-by-hands-on-2025-02-14');
--
INSERT INTO default.t_ex_k_to_coingecko_coin_id VALUES
('bybit', 'spot', 'OM', 'USD', 'mantra', 'added-by-hands-on-2025-02-14'),
('bybit', 'spot', 'MCG', 'USD', 'metalcore', 'added-by-hands-on-2025-02-14');