import json
import os
import functools
import typing
import dataclasses as dc
import datetime as dt

import requests
import websocket
import pandas as pd
from tabulate import tabulate

from run import configure_logger, logger


@dc.dataclass
class BinancePair:
    l1: str
    l2: str
    l3: str
    d1: str
    d2: str
    d3: str
    lv1: str
    lv2: str
    lv3: str
    value: float
    tpath: str
    tg_path: str


@dc.dataclass
class OrderBook:
    ask_price: float
    bid_price: float


class BinancePort:
    def fetch_symbols(self) -> dict:
        response = requests.get('https://api.binance.com/api/v3/exchangeInfo')
        if response.status_code != 200:
            raise Exception('failed to fetch binance exchangeInfo')
        return response.json()['symbols']

    def listen_for_tickers(self, handler: typing.Callable[[dict], None]) -> None:
        ws = websocket.WebSocket()
        ws.connect('wss://stream.binance.com:9443/ws')
        ws.send(json.dumps({
            'method': 'SUBSCRIBE',
            'params': ['!ticker@arr'],
            'id': 1,
        }))
        while True:
            handler(json.loads(ws.recv()))


class Binance:
    def __init__(
        self,
        binance_port: BinancePort,
        rows_to_print: typing.Optional[int] = None,
    ) -> None:
        self._binance_port = binance_port
        self._order_books: typing.Optional[dict[str, OrderBook]] = None
        self._pairs: typing.Optional[list[BinancePair]] = None
        self._rows_to_print = rows_to_print

    def look_for_opportunities(self) -> None:
        self._fill_order_books()
        self._fill_pairs()
        self._binance_port.listen_for_tickers(self._handler)

    def _fill_order_books(self) -> None:
        """Fills dict with ask/bid prices depending on symbols"""
        self._order_books = {
            symbol['symbol']: OrderBook(ask_price=0, bid_price=0)
            for symbol in self._trading_symbols
        }
        logger.info('order books have filled')

    def _fill_pairs(self) -> None:
        """Generates pairs depending on symbols and order_books"""
        start_time = dt.datetime.utcnow()
        tokens_assets = [
            (x['baseAsset'], x['quoteAsset'])
            for x in self._trading_symbols
        ]
        tokens: set[str] = set([x for l in tokens_assets for x in l])
        pairs = []

        for d1 in tokens:
            for d2 in tokens:
                for d3 in tokens:
                    if d1 == d2 or d2 == d3 or d3 == d1:
                        continue

                    lv1 = []
                    lv2 = []
                    lv3 = []
                    l1 = ''
                    l2 = ''
                    l3 = ''

                    if d1+d2 in self._order_books:
                        lv1.append(d1+d2)
                        l1 = 'num'
                    if d2+d1 in self._order_books:
                        lv1.append(d2+d1)
                        l1 = 'den'
                    if d2+d3 in self._order_books:
                        lv2.append(d2+d3)
                        l2 = 'num'
                    if d3+d2 in self._order_books:
                        lv2.append(d3+d2)
                        l2 = 'den'
                    if d3+d1 in self._order_books:
                        lv3.append(d3+d1)
                        l3 = 'num'
                    if d1+d3 in self._order_books:
                        lv3.append(d1+d3)
                        l3 = 'den'

                    if not (lv1 and lv2 and lv3):
                        continue

                    pair = {
                        'l1': l1,
                        'l2': l2,
                        'l3': l3,
                        'd1': d1,
                        'd2': d2,
                        'd3': d3,
                        'lv1': lv1[0],
                        'lv2': lv2[0],
                        'lv3': lv3[0],
                        'value': -100,
                        'tpath': '',
                    }
                    pair = BinancePair(
                        l1=l1,
                        l2=l2,
                        l3=l3,
                        d1=d1,
                        d2=d2,
                        d3=d3,
                        lv1=lv1[0],
                        lv2=lv2[0],
                        lv3=lv3[0],
                        value=-100,
                        tpath='',
                        tg_path='',
                    )

                    pairs.append(pair)

        logger.info(
            '%s pair found in %s seconds',
            len(pairs),
            dt.datetime.utcnow() - start_time,
        )
        self._pairs = pairs

    def _handler(self, message: typing.Union[list[dict], dict]) -> None:
        """Handles tickers ws messages"""
        if isinstance(message, dict):
            return
        for ticker in message:
            if not (order_book := self._order_books.get(ticker['s'])):
                continue
            bid_price = float(ticker['b'])
            ask_price = float(ticker['a'])
            order_book.bid_price = bid_price
            order_book.ask_price = ask_price
        for pair in self._pairs:
            pair1 = self._order_books[pair.lv1]
            pair2 = self._order_books[pair.lv2]
            pair3 = self._order_books[pair.lv3]
            if not (pair1.bid_price and pair2.bid_price and pair3.bid_price):
                continue
            lv_calc = 0
            lv_str = ''
            if pair.l1 == 'num':
                lv_calc = pair1.bid_price
                lv_str = f'{pair.d1}->{pair.lv1}[bid][{pair1.bid_price}]->{pair.d2}'
            else:
                lv_calc = 1 / pair1.ask_price
                lv_str += f'{pair.d1}->{pair.lv1}[ask][{pair1.ask_price}]->{pair.d2}'
            if pair.l2 == 'num':
                lv_calc *= pair2.bid_price
                lv_str += f'{pair.d2}->{pair.lv2}[bid][{pair2.bid_price}]->{pair.d3}'
            else:
                lv_calc *= 1 / pair2.ask_price
                lv_str += f'{pair.d2}->{pair.lv2}[ask][{pair2.ask_price}]->{pair.d3}'
            if pair.l3 == 'num':
                lv_calc *= pair3.bid_price
                lv_str += f'{pair.d3}->{pair.lv3}[bid][{pair3.bid_price}]->{pair.d1}'
            else:
                lv_calc *= 1 / pair3.ask_price
                lv_str += f'{pair.d3}->{pair.lv3}[ask][{pair3.ask_price}]->{pair.d1}'
            pair.tpath = lv_str
            pair.value = (lv_calc - 1) * 100
            pair.tg_path = f'{pair.d1}->{pair.d2}->{pair.d3}->{pair.d1}'
        logger.info('message processed, len: %s', len(message))
        if self._rows_to_print:
            pairs_df = pd.DataFrame([dc.asdict(pair) for pair in self._pairs])
            pairs_df = pairs_df.sort_values(by=['value'], ascending=False)
            pairs_df = pairs_df.head(self._rows_to_print)
            os.system('clear')
            print(tabulate(pairs_df, headers = 'keys', tablefmt = 'psql'))

    @functools.cached_property
    def _trading_symbols(self) -> dict:
        symbols = self._binance_port.fetch_symbols()
        return [x for x in symbols if x['status'] == 'TRADING']


def main() -> None:
    binance_port = BinancePort()
    rows_to_print = int(os.environ.get('ROWS_TO_PRINT', '0'))
    binance = Binance(
        binance_port=binance_port,
        rows_to_print=rows_to_print,
    )
    binance.look_for_opportunities()


if __name__ == '__main__':
    configure_logger()
    main()
