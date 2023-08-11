import os
import logging
import datetime as dt
import dataclasses as dc

import requests
import pandas as pd
from pybit.unified_trading import HTTP


@dc.dataclass
class Symbol:
    alias: str
    base_coin: str
    quote_coin: str


@dc.dataclass
class Ticker:
    symbol_alias: str
    last_price: float


@dc.dataclass
class Fee:
    symbol_alias: str
    taker_fee_rate: float
    maker_fee_rate: float


class PortError(Exception):
    pass


class BybitPublicPort:
    def fetch_symbols(self) -> list[Symbol]:
        url = (
            'https://api.bybit.com'
            '/spot/v3/public/symbols'
        )
        response = requests.get(url)
        self._valdiate_status(response=response)
        return [
            Symbol(
                alias=x['alias'],
                base_coin=x['baseCoin'],
                quote_coin=x['quoteCoin'],
            )
            for x in response.json()['result']['list']
            if x['showStatus'] == '1'
        ]

    def fetch_tickers(self) -> list[Ticker]:
        url = (
            'https://api.bybit.com'
            '/spot/v3/public/quote/ticker/24hr'
        )
        response = requests.get(url)
        self._valdiate_status(response=response)
        return [
            Ticker(symbol_alias=x['s'], last_price=float(x['lp']))
            for x in response.json()['result']['list']
        ]

    def fetch_zero_fees(self) -> set[str]:
        url = (
            'https://api.bybit.com'
            '/spot/api/basic/symbol_list'
        )
        response = requests.get(url)
        self._valdiate_status(response=response)
        zero_fees = set()
        for result_item in response.json()['result']:
            for symbol_item in result_item['quoteTokenSymbols']:
                if symbol_item['zeroFeeType'] != 1:
                    continue
                zero_fees.add(symbol_item['symbolId'])
        return zero_fees

    def _valdiate_status(self, response: requests.Response) -> None:
        status = response.status_code
        if status != 200:
            message = 'request to %s failed, status_code %s, body: %s'
            url, text = response.url, response.text
            raise PortError(message.format(url, status, text))


BybitPrivatePort = HTTP


class BybitFeesAdapter:
    def __init__(
        self,
        private_port: BybitPrivatePort,
        public_port: BybitPublicPort,
    ) -> None:
        self._private_port = private_port
        self._public_port = public_port

    def fetch(self) -> dict[str, Fee]:
        user_fees = self._private_port.get_fee_rates()
        zero_fees = self._public_port.fetch_zero_fees()
        fees = {}
        for fee_item in user_fees['result']['list']:
            symbol_alias = fee_item['symbol']
            taker_fee_rate = float(fee_item['takerFeeRate'])
            maker_fee_rate = float(fee_item['makerFeeRate'])
            fee = Fee(
                symbol_alias=symbol_alias,
                taker_fee_rate=taker_fee_rate,
                maker_fee_rate=maker_fee_rate,
            )
            fees[symbol_alias] = fee
        for symbol_alias in zero_fees:
            fee = Fee(
                symbol_alias=symbol_alias,
                taker_fee_rate=0,
                maker_fee_rate=0,
            )
            fees[symbol_alias] = fee
        return fees


logger = logging.getLogger()


def gen_ways(symbols: list[Symbol], max_depth: int = 5) -> list[str]:
    """Generates all possible tokens exchange ways"""
    date_start = dt.datetime.utcnow()
    # generate ways
    ways = []
    def step(prev_token: str, passed_tokens: list) -> None:
        ways.append(passed_tokens)
        if len(passed_tokens) == max_depth:
            return
        for symbol in symbols:
            next_token = None
            if symbol.base_coin == prev_token and symbol.quote_coin not in passed_tokens:
                next_token = symbol.quote_coin
            if symbol.quote_coin == prev_token and symbol.base_coin not in passed_tokens:
                next_token = symbol.base_coin
            if not next_token:
                continue
            passed = passed_tokens + [next_token]
            step(prev_token=next_token, passed_tokens=passed)
    step(prev_token='USDT', passed_tokens=['USDT'])
    date_end = dt.datetime.utcnow()
    logger.info('calculation time: %s', date_end - date_start)
    # close ways with USDT
    to_usdt_map = [
        symbol.quote_coin if symbol.base_coin == 'USDT' else symbol.base_coin
        for symbol in symbols
        if symbol.base_coin == 'USDT' or symbol.quote_coin == 'USDT'
    ]
    for way in ways:
        if way[-1] in to_usdt_map:
            way.append('USDT')
    return ways


def test_gen_ways() -> None:
    symbols = [
        Symbol(alias='USDTUSDC', base_coin='USDT', quote_coin='USDC'),
        Symbol(alias='MNTUSDC', base_coin='MNT', quote_coin='USDC'),
        Symbol(alias='BTCMNT', base_coin='BTC', quote_coin='MNT'),
        Symbol(alias='BTCUSDT', base_coin='BTC', quote_coin='USDT'),
    ]
    ways = gen_ways(symbols=symbols)
    expected_ways = [
        ['USDT'],
        ['USDT', 'USDC', 'USDT'],
        ['USDT', 'USDC', 'MNT'],
        ['USDT', 'USDC', 'MNT', 'BTC', 'USDT'],
        ['USDT', 'BTC', 'USDT'],
        ['USDT', 'BTC', 'MNT'],
        ['USDT', 'BTC', 'MNT', 'USDC', 'USDT'],
    ]
    assert ways == expected_ways


def test_gen_ways_2() -> None:
    symbols = [
        Symbol(alias='USDTUSDC', base_coin='USDT', quote_coin='USDC'),
        Symbol(alias='MNTUSDC', base_coin='MNT', quote_coin='USDC'),
        Symbol(alias='BTCMNT', base_coin='BTC', quote_coin='MNT'),
        Symbol(alias='BTCUSDT', base_coin='BTC', quote_coin='USDT'),
    ]
    ways = gen_ways(symbols=symbols, max_depth=3)
    expected_ways = [
        ['USDT'],
        ['USDT', 'USDC', 'USDT'],
        ['USDT', 'USDC', 'MNT'],
        ['USDT', 'BTC', 'USDT'],
        ['USDT', 'BTC', 'MNT'],
    ]
    assert ways == expected_ways


def gen_prices(
    symbols: list[Symbol],
    tickers: list[Ticker],
    zero_fees: set[str],
) -> dict[str, float]:
    maker_fee_rate = 0.00055
    symbols_map = {symbol.alias: symbol for symbol in symbols}
    prices = {}
    for ticker in tickers:
        alias = ticker.symbol_alias
        if not (symbol := symbols_map.get(alias)):
            logger.warning('alias %s doesnt exists', alias)
            continue
        key_base_quote = '{}{}'.format(symbol.base_coin, symbol.quote_coin)
        key_quote_base = '{}{}'.format(symbol.quote_coin, symbol.base_coin)
        fee_rate = 0 if alias in zero_fees else maker_fee_rate
        prices[key_base_quote] = ticker.last_price * (1 - fee_rate)
        prices[key_quote_base] = 1 / ticker.last_price * (1 - fee_rate)
    return prices


def test_gen_prices() -> None:
    symbols = [Symbol(alias='ETHUSDT', base_coin='ETH', quote_coin='USDT')]
    tickers = [Ticker(symbol_alias='ETHUSDT', last_price=1954.12)]
    zero_fees = set([])
    prices = gen_prices(symbols=symbols, tickers=tickers, zero_fees=zero_fees)
    assert prices == {'ETHUSDT': 1953.0452339999997, 'USDTETH': 0.0005114578429165046}


def test_gen_prices_zeros() -> None:
    symbols = [Symbol(alias='ETHUSDT', base_coin='ETH', quote_coin='USDT')]
    tickers = [Ticker(symbol_alias='ETHUSDT', last_price=1954.12)]
    zero_fees = set(['ETHUSDT'])
    prices = gen_prices(symbols=symbols, tickers=tickers, zero_fees=zero_fees)
    assert prices == {'ETHUSDT': 1954.12, 'USDTETH': 0.0005117392995312468}


def fetch_for_calc() -> None:
    public_port = BybitPublicPort()
    _ = public_port.fetch_symbols()
    _ = public_port.fetch_tickers()
    _ = public_port.fetch_zero_fees()


def fetch_fees() -> None:
    api_key = os.environ['API_KEY_BYBIT']
    api_secret = os.environ['API_SECRET_BYBIT']
    private_port = BybitPrivatePort(
        api_key=api_key,
        api_secret=api_secret,
    )
    public_port = BybitPublicPort()
    fees_adapter = BybitFeesAdapter(
        private_port=private_port,
        public_port=public_port,
    )
    _ = fees_adapter.fetch()


def calc_ways() -> None:
    logging.basicConfig(level=logging.INFO)

    public_port = BybitPublicPort()
    symbols = public_port.fetch_symbols()
    tickers = public_port.fetch_tickers()
    zero_fees = public_port.fetch_zero_fees()
    prices = gen_prices(symbols=symbols, tickers=tickers, zero_fees=zero_fees)

    max_depth = 6
    columns=['step{}'.format(i+1) for i in range(max_depth+1)]
    ways = gen_ways(symbols=symbols, max_depth=max_depth)
    ways_for_df = [
        way
        for way in ways
        if len(way) > 3 and way[-1] == 'USDT'
    ]

    def apply_price(row: pd.Series) -> float:
        tokens = [x for x in row.tolist() if x]
        amount = 100  # at the beginning amoutn is in USDT
        for i in range(len(tokens)-1):
            key = '{}{}'.format(tokens[i], tokens[i+1])
            amount *= prices[key]
        return amount

    ways_df = pd.DataFrame(ways_for_df, columns=columns).assign(
        final_price=lambda x: x.apply(apply_price, axis=1),
    )

    # ways_df.sort_values(by=['final_price'])
    breakpoint()
    pass


def main() -> None:
    test_gen_ways()
    test_gen_ways_2()
    test_gen_prices()
    test_gen_prices_zeros()
    # fetch_for_calc()
    # fetch_fees()
    calc_ways()


if __name__ == '__main__':
    main()
