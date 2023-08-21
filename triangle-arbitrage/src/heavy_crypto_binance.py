import json
import os
import functools
import typing
import argparse
import abc
import unittest.mock
import dataclasses as dc
import datetime as dt

import requests
import websocket
import pandas as pd
from tabulate import tabulate

from src.common import configure_logger, logger, TelegramPort
from src.domain import Binance, HttpPort, TradeSumUp


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


@dc.dataclass
class OrderBook:
    ask_price: float
    bid_price: float


class LotSizeError(Exception):
    pass


class SymbolLotSize:
    def __init__(
        self,
        max_qty: float,
        min_qty: float,
        step_size: float,
    ) -> None:
        self.max_qty = max_qty
        self.min_qty = min_qty
        self.step_size = step_size

    def apply(self, qty: float) -> float:
        if qty < self.min_qty:
            raise LotSizeError('qty less than lot_size.min_qty')
        precision = len('{:.16f}'.format(self.step_size).rstrip('0').split('.')[1])
        qty_int = int(qty * 10**precision)
        min_qty = int(self.min_qty * 10**precision)
        step_size = int(self.step_size * 10**precision)
        out = min_qty + (qty_int - min_qty) // step_size * step_size
        return out / 10**precision


class Symbol:
    def __init__(
        self,
        alias: str,
        base_asset: str,
        quote_asset: str,
        lot_size: SymbolLotSize,
    ) -> None:
        self.alias = alias
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.lot_size = lot_size

    @classmethod
    def new_from_raw(cls, content: dict) -> 'Symbol':
        lot_size = [
            SymbolLotSize(
                max_qty=float(filter_['maxQty']),
                min_qty=float(filter_['minQty']),
                step_size=float(filter_['stepSize']),
            )
            for filter_ in content['filters']
            if filter_['filterType'] == 'LOT_SIZE'
        ].pop()
        symbol = cls(
            alias=content['symbol'],
            base_asset=content['baseAsset'],
            quote_asset=content['quoteAsset'],
            lot_size=lot_size,
        )
        return symbol


def test_symbol_new_from_raw() -> None:
    symbol_raw = {
        'baseAsset': 'ETH',
        'filters': [{'filterType': 'LOT_SIZE',
                    'maxQty': '100000.00000000',
                    'minQty': '0.00010000',
                    'stepSize': '0.00010000'},
                    {'filterType': 'MAX_NUM_ORDERS', 'maxNumOrders': 200},
                    {'filterType': 'MAX_NUM_ALGO_ORDERS', 'maxNumAlgoOrders': 5}],
        'quoteAsset': 'BTC',
        'status': 'TRADING',
        'symbol': 'ETHBTC'}
    symbol = Symbol.new_from_raw(content=symbol_raw)

    assert symbol.alias == 'ETHBTC'
    assert symbol.base_asset == 'ETH'
    assert symbol.quote_asset == 'BTC'
    assert symbol.lot_size.max_qty == 100000.0
    assert symbol.lot_size.min_qty == 0.0001
    assert symbol.lot_size.step_size == 0.0001


def test_lot_size_apply() -> None:
    def example_1() -> None:
        symbol_raw = {
            'baseAsset': 'ETH',
            'filters': [{'filterType': 'LOT_SIZE',
                        'maxQty': '100000.00000000',
                        'minQty': '0.00010000',
                        'stepSize': '0.00010000'},
                        {'filterType': 'MAX_NUM_ORDERS', 'maxNumOrders': 200},
                        {'filterType': 'MAX_NUM_ALGO_ORDERS', 'maxNumAlgoOrders': 5}],
            'quoteAsset': 'BTC',
            'status': 'TRADING',
            'symbol': 'ETHBTC'}
        symbol = Symbol.new_from_raw(content=symbol_raw)

        assert symbol.lot_size.apply(qty=0.0001) == 0.0001
        assert symbol.lot_size.apply(qty=0.00015) == 0.0001
        assert symbol.lot_size.apply(qty=0.00025) == 0.0002
        assert symbol.lot_size.apply(qty=1.12355) == 1.1235
        assert symbol.lot_size.apply(qty=25) == 25.0

    def example_2() -> None:
        symbol_raw = {
            'baseAsset': 'BTC',
            'baseAssetPrecision': 8,
            'baseCommissionPrecision': 8,
            'filters': [
                        {'filterType': 'LOT_SIZE',
                        'maxQty': '9000.00000000',
                        'minQty': '0.00001000',
                        'stepSize': '0.00001000'}],
            'quoteAsset': 'USDT',
            'quoteAssetPrecision': 8,
            'quoteCommissionPrecision': 8,
            'quoteOrderQtyMarketAllowed': True,
            'quotePrecision': 8,
            'status': 'TRADING',
            'symbol': 'BTCUSDT'}
        symbol = Symbol.new_from_raw(content=symbol_raw)

        assert symbol.lot_size.apply(qty=0.00076303) == 0.00076

    def example_3() -> None:
        lot_size = SymbolLotSize(
            min_qty=10**-5,
            max_qty=9000.0,
            step_size=10**-5,
        )
        symbol = Symbol(
            alias='BTCUSDT',
            base_asset='BTC',
            quote_asset='USDT',
            lot_size=lot_size,
        )

        assert symbol.lot_size.apply(qty=0.00076303) == 0.00076

    example_1()
    example_2()
    example_3()


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


class IHandler(abc.ABC):

    @abc.abstractmethod
    def handle_diff(
        self,
        pairs_df: pd.DataFrame,
        symbols_info: dict[str, Symbol],
    ) -> None:
        pass


class PrintHandler(IHandler):

    def __init__(self, rows_to_print: int) -> None:
        self._rows_to_print = rows_to_print

    def handle_diff(self, pairs_df: pd.DataFrame, _: dict[str, Symbol]) -> None:
        if not self._rows_to_print:
            return
        pairs_to_print_df = pairs_df.head(self._rows_to_print)
        os.system('clear')
        print(tabulate(pairs_to_print_df, headers='keys', tablefmt='psql'))


class NotifyHandler(IHandler):

    def __init__(
        self,
        threshold: float,
        telegram_port: TelegramPort,
    ) -> None:
        """Parameters
        ----------
        x : float
            Notify threshold in percents.
        """
        self._threshold = threshold
        self._telegram_port = telegram_port

    def handle_diff(self, pairs_df: pd.DataFrame, _: dict[str, Symbol]) -> None:
        pair_to_notify_df = pairs_df[lambda x: x.value > self._threshold]
        if pair_to_notify_df.empty:
            return
        rows = []
        for (_, row) in pair_to_notify_df.iterrows():
            steps = '{}->{}->{}->{}'.format(row.d1, row.d2, row.d3, row.d1)
            message = '{} - {:.2f}'.format(steps, row.value)
            rows.append(message)
        message = '\n'.join(rows)
        self._telegram_port.notify_markdown(message=message)


class TradeError(Exception):
    pass


def binance_trade_step_v1(
    trading: Binance.TradingDomain,
    symbol: Symbol,
    token_from: str,
    qty: float,
) -> float:
    # TODO: put to TradeV1Handler
    # TODO: add tests, i guess this way def test_trade_v1_trade_step
    #       will be redunant(completely useless)
    """Performs exchange and returns next token amount"""
    logger.info(
        '[binance_trade_step_v1] symbol %s, token_from %s, qty %s',
        symbol.alias,
        token_from,
        qty,
    )
    match token_from:
        case symbol.base_asset:
            trade_kwargs = {'symbol': symbol.alias, 'side': 'SELL', 'quantity': qty}
        case symbol.quote_asset:
            trade_kwargs = {'symbol': symbol.alias, 'side': 'BUY', 'quote_order_qty': qty}
        case _:
            raise TradeError('havent found pair match')
    trade = trading.market(**trade_kwargs)
    if not trade.success:
        message = 'trade_args: {}, body: {}'.format(
            trade_kwargs, trade.body,
        )
        raise TradeError(message)
    match token_from:
        case symbol.base_asset:
            out_qty = float(trade.body['cummulativeQuoteQty'])
        case symbol.quote_asset:
            out_qty = float(trade.body['executedQty'])
        case _:
            pass
    logger.info('out_qty: %s', out_qty)
    return out_qty


class TradeV1Handler(IHandler):

    def __init__(
        self,
        threshold: float,
        telegram_port: TelegramPort,
        trading_binance: Binance.TradingDomain,
        commit_hash: str,
    ) -> None:
        """Parameters
        ----------
        x : float
            Trade threshold in percents.
        """
        self._threshold = threshold
        self._telegram_port = telegram_port
        self._trading_binance = trading_binance
        self._commit_hash = commit_hash
        self._made = False

    def handle_diff(
        self,
        pairs_df: pd.DataFrame,
        symbols_info: dict[str, Symbol],
    ) -> None:
        """Specifics:
        1. trade happens only one time
        2. happens when USDT is in first pair
        3. happens only when value > threshold
        """
        if self._made:
            return
        start_time = dt.datetime.utcnow()
        # NOTE: debug-purpose
        # to_trade_df = pairs_df[lambda x: x.value > self._threshold]
        to_trade_df = pairs_df[lambda x: x.value > 0.01]
        to_trade_df = to_trade_df[lambda x: x.d1 == 'USDT']
        if to_trade_df.empty:
            return
        self._made = True
        path = to_trade_df.sort_values(by=['value'], ascending=False).iloc[0]
        try:
            logger.info('trade start %s->%s->%s', path.d1, path.d2, path.d3)
            self._trade(path=path, qty_usdt=25, symbols_info=symbols_info)
            duration = dt.datetime.utcnow() - start_time
            message = json.dumps({
                'type': 'heavy-crypto-binance--trade',
                'message': 'trade made',
                'now': dt.datetime.utcnow().isoformat(),
                'commin_hash': self._commit_hash,
                'duration': str(duration),
                'path': '{}->{}->{}'.format(path.d1, path.d2, path.d3),
            }, indent=2)
            logger.info(message)
            self._telegram_port.notify_markdown(message=message)
        except TradeError as error:
            self._telegram_port.notify_error(error)

    def _trade(
        self,
        path: pd.Series,
        qty_usdt: float,
        symbols_info: dict[str, Symbol],
    ) -> None:
        token1 = 'USDT'
        token2 = path.d2
        token3 = path.d3
        symbol12 = path.lv1
        symbol23 = path.lv2
        symbol31 = path.lv3

        token2_qty = binance_trade_step_v1(
            trading=self._trading_binance,
            symbol=symbols_info[symbol12],
            token_from=token1,
            qty=qty_usdt,
        )
        token3_qty = binance_trade_step_v1(
            trading=self._trading_binance,
            symbol=symbols_info[symbol23],
            token_from=token2,
            qty=token2_qty,
        )
        token1_qty = binance_trade_step_v1(
            trading=self._trading_binance,
            symbol=symbols_info[symbol31],
            token_from=token3,
            qty=token3_qty,
        )
        logger.info('token1_qty: %s', token1_qty)

        return
        trade_l1 = self._trade_step(l=path.l1, symbol=path.lv1, qty=qty_usdt)
        qty_l2 = float(trade_l1.body['executedQty'])
        trade_l2 = self._trade_step(l=path.l2, symbol=path.lv2, qty=qty_l2)
        qty_l3 = float(trade_l2.body['cummulativeQuoteQty'])
        self._trade_step(l=path.l3, symbol=path.lv3, qty=qty_l3)

    def _trade_step(self, l: str, symbol: str, qty: float) -> TradeSumUp:
        trading_binance = self._trading_binance
        match l:
            case 'num':
                side = 'SELL'
                quantity_arg = {'quantity': qty}
            case 'den':
                side = 'BUY'
                quantity_arg = {'quote_order_qty': qty}
            case _:
                raise TradeError(f'unknown l {l!r}')
        logger.info(
            'trade-step: symbol %s, side %s, quantity_arg %s',
            symbol,
            side,
            quantity_arg,
        )
        trade_response = trading_binance.market(
            symbol=symbol,
            side=side,
            **quantity_arg,
        )
        if not trade_response.success:
            raise TradeError('error: {}'.format(trade_response.body))
        return trade_response


def test_trade_v1_trade_step() -> None:
    telegram_port = unittest.mock.MagicMock()
    trading_binance = unittest.mock.MagicMock()
    handler = TradeV1Handler(
        threshold=0.0,
        telegram_port=telegram_port,
        trading_binance=trading_binance,
        commit_hash='',
    )

    handler._trade_step(l='num', symbol='ETHUSDT', qty=44.77)
    _, kwargs_1 = trading_binance.market.call_args
    assert kwargs_1['symbol'] == 'ETHUSDT'
    assert kwargs_1['side'] == 'SELL'
    assert kwargs_1['quantity'] == 44.77

    trading_binance.reset_mock()
    handler._trade_step(l='den', symbol='ETHUSDT', qty=44.77)
    _, kwargs_2 = trading_binance.market.call_args
    assert kwargs_2['symbol'] == 'ETHUSDT'
    assert kwargs_2['side'] == 'BUY'
    assert kwargs_2['quote_order_qty'] == 44.77


def debug_binance_trade() -> None:
    '''
    (Pdb) path
    l1            den
    l2            num
    l3            num
    d1           USDT
    d2            ILV
    d3            BTC
    lv1       ILVUSDT
    lv2        ILVBTC
    lv3       BTCUSDT
    value    0.040828
    Name: 10960, dtype: object


    den     num     num
    USDT -> IDEX -> BTC
    IDEXUSDT -> IDEXBTC -> BTCUSDT


    symbol=IDEXUSDT    | BUY                      | SELL
    quantity=0.25      | buy 0.25 IDEX for n USDT | sell 0.25 IDEX for n USDT
    quote_order_qty=11 | buy n IDEX for 11 USDT   | sell n IDEX for 11 USDT

    (Pdb) t_buy_1 = self._trading_binance.market(symbol='ILVUSDT', side='BUY', quantity=0.25)
    (Pdb) t_buy_1
    TradeSumUp(success=True, status_code=200, body={'symbol': 'ILVUSDT', 'orderId': 137042024, 'orderListId': -1, 'clientOrderId': 'xB4ui0CSqs6j4vlygMOtJC', 'transactTime': 1692616228016, 'price': '0.00000000', 'origQty': '0.25000000', 'executedQty': '0.25000000', 'cummulativeQuoteQty': '11.20000000', 'status': 'FILLED', 'timeInForce': 'GTC', 'type': 'MARKET', 'side': 'BUY', 'workingTime': 1692616228016, 'fills': [{'price': '44.80000000', 'qty': '0.25000000', 'commission': '0.00004002', 'commissionAsset': 'BNB', 'tradeId': 10689891}], 'selfTradePreventionMode': 'NONE'})
    (Pdb) t_buy_2 = self._trading_binance.market(symbol='ILVUSDT', side='BUY', quote_order_qty=11)
    (Pdb) t_buy_2
    TradeSumUp(success=True, status_code=200, body={'symbol': 'ILVUSDT', 'orderId': 137042714, 'orderListId': -1, 'clientOrderId': 'IKgTqT9U39mpRpdATy6W8b', 'transactTime': 1692616424588, 'price': '0.00000000', 'origQty': '0.24500000', 'executedQty': '0.24500000', 'cummulativeQuoteQty': '10.97355000', 'status': 'FILLED', 'timeInForce': 'GTC', 'type': 'MARKET', 'side': 'BUY', 'workingTime': 1692616424588, 'fills': [{'price': '44.79000000', 'qty': '0.24500000', 'commission': '0.00003921', 'commissionAsset': 'BNB', 'tradeId': 10689900}], 'selfTradePreventionMode': 'NONE'})
    (Pdb) t_sell_1 = self._trading_binance.market(symbol='ILVUSDT', side='SELL', quantity=0.23)
    (Pdb) t_sell_1
    TradeSumUp(success=True, status_code=200, body={'symbol': 'ILVUSDT', 'orderId': 137043361, 'orderListId': -1, 'clientOrderId': 'tzoEC9soosM5MxoNdxNSca', 'transactTime': 1692616583526, 'price': '0.00000000', 'origQty': '0.23000000', 'executedQty': '0.23000000', 'cummulativeQuoteQty': '10.29710000', 'status': 'FILLED', 'timeInForce': 'GTC', 'type': 'MARKET', 'side': 'SELL', 'workingTime': 1692616583526, 'fills': [{'price': '44.77000000', 'qty': '0.23000000', 'commission': '0.00003682', 'commissionAsset': 'BNB', 'tradeId': 10689907}], 'selfTradePreventionMode': 'NONE'})
    (Pdb) t_sell_2 = self._trading_binance.market(symbol='ILVUSDT', side='SELL', quote_order_qty=10)
    (Pdb) t_sell_2
    TradeSumUp(success=True, status_code=200, body={'symbol': 'ILVUSDT', 'orderId': 137043680, 'orderListId': -1, 'clientOrderId': 'bDH9jIsjHNpleYu823Bh9F', 'transactTime': 1692616648537, 'price': '0.00000000', 'origQty': '0.22300000', 'executedQty': '0.22300000', 'cummulativeQuoteQty': '9.97925000', 'status': 'FILLED', 'timeInForce': 'GTC', 'type': 'MARKET', 'side': 'SELL', 'workingTime': 1692616648537, 'fills': [{'price': '44.75000000', 'qty': '0.22300000', 'commission': '0.00003560', 'commissionAsset': 'BNB', 'tradeId': 10689917}], 'selfTradePreventionMode': 'NONE'})


    symbol=USDTBIDR      | BUY                        | SELL
    quantity=11          | sell n BIRD for 10 USDT    | sell 11 USDT for 169k BIDR
    quote_order_qty=170k | sell ~184k BIRD for n USDT | sell n USDT for 170k BIRD

    (Pdb) t_buy_1 = trading_binance.market(symbol='USDTBIDR', side='BUY', quantity=10)
    (Pdb) t_buy_1
    TradeSumUp(success=True, status_code=200, body={'symbol': 'USDTBIDR', 'orderId': 47323869, 'orderListId': -1, 'clientOrderId': 'zn8TnXYMuUJ7vg5a4DQQjT', 'transactTime': 1692619300930, 'price': '0.00', 'origQty': '10.00000000', 'executedQty': '10.00000000', 'cummulativeQuoteQty': '154060.00', 'status': 'FILLED', 'timeInForce': 'GTC', 'type': 'MARKET', 'side': 'BUY', 'workingTime': 1692619300930, 'fills': [{'price': '15406.00', 'qty': '10.00000000', 'commission': '0.00003561', 'commissionAsset': 'BNB', 'tradeId': 17712036}], 'selfTradePreventionMode': 'NONE'})
    (Pdb) t_buy_2 = trading_binance.market(symbol='USDTBIDR', side='BUY', quote_order_qty=184850)
    (Pdb) t_buy_2
    TradeSumUp(success=True, status_code=200, body={'symbol': 'USDTBIDR', 'orderId': 47323875, 'orderListId': -1, 'clientOrderId': '988E7h7NfU9ghUWO95x3h8', 'transactTime': 1692619399659, 'price': '0.00', 'origQty': '11.90000000', 'executedQty': '11.90000000', 'cummulativeQuoteQty': '183331.40', 'status': 'FILLED', 'timeInForce': 'GTC', 'type': 'MARKET', 'side': 'BUY', 'workingTime': 1692619399659, 'fills': [{'price': '15406.00', 'qty': '11.90000000', 'commission': '0.01190000', 'commissionAsset': 'USDT', 'tradeId': 17712041}], 'selfTradePreventionMode': 'NONE'})
    (Pdb) t_sell_1 = trading_binance.market(symbol='USDTBIDR', side='SELL', quantity=11)
    (Pdb) t_sell_1
    TradeSumUp(success=True, status_code=200, body={'symbol': 'USDTBIDR', 'orderId': 47323839, 'orderListId': -1, 'clientOrderId': 'N6GE6wb9vXnzjFZaXNHn7q', 'transactTime': 1692619003846, 'price': '0.00', 'origQty': '11.00000000', 'executedQty': '11.00000000', 'cummulativeQuoteQty': '169455.00', 'status': 'FILLED', 'timeInForce': 'GTC', 'type': 'MARKET', 'side': 'SELL', 'workingTime': 1692619003846, 'fills': [{'price': '15405.00', 'qty': '11.00000000', 'commission': '0.00003939', 'commissionAsset': 'BNB', 'tradeId': 17712026}], 'selfTradePreventionMode': 'NONE'})
    (Pdb) t_sell_2 = trading_binance.market(symbol='USDTBIDR', side='SELL', quote_order_qty=170000)
    (Pdb) t_sell_2
    TradeSumUp(success=True, status_code=200, body={'symbol': 'USDTBIDR', 'orderId': 47323865, 'orderListId': -1, 'clientOrderId': 'eNtjOggddUthp6FkTjcUjy', 'transactTime': 1692619206411, 'price': '0.00', 'origQty': '11.00000000', 'executedQty': '11.00000000', 'cummulativeQuoteQty': '169455.00', 'status': 'FILLED', 'timeInForce': 'GTC', 'type': 'MARKET', 'side': 'SELL', 'workingTime': 1692619206411, 'fills': [{'price': '15405.00', 'qty': '11.00000000', 'commission': '0.00003939', 'commissionAsset': 'BNB', 'tradeId': 17712033}], 'selfTradePreventionMode': 'NONE'})


    ### buy base_asset way explanation
    symbol=IDEXUSDT      | BUY                       | SELL
    quantity=0.25        | buy 0.25 IDEX for n USDT  | buy n USDT for 0.25 IDEX
    quote_order_qty=11   | buy n IDEX for 11 USDT    | buy 11 USDT for n IDEX
    -----
    symbol=USDTBIDR      | BUY                       | SELL
    quantity=11          | buy 11 USDT for n BIRD    | buy n BIRD for 11 USDT
    quote_order_qty=170k | buy n USDT for ~184k BIRD | buy 107k BIRD for n USDT
    '''
    pass


def debug_binance_USDT_AVA_BTC() -> None:
    # FIXME: for AVAUSDT commision pays in AVA
    #        this causes 
    api_key_binance = os.environ['API_KEY_BINANCE']
    api_secret_binance = os.environ['API_SECRET_BINANCE']
    signer_binance = Binance.HmacSigner(secret_key=api_secret_binance)
    http_port_binance = HttpPort(url_base='https://api.binance.com')
    trading_binance = Binance.TradingDomain(
        api_key=api_key_binance,
        signer=signer_binance,
        http_port=http_port_binance,
    )
    binance_port = BinancePort()
    symbols = binance_port.fetch_symbols()
    trading_symbols = [x for x in symbols if x['status'] == 'TRADING']
    symbols_info = {
        symbol['symbol']: Symbol.new_from_raw(content=symbol)
        for symbol in trading_symbols
    }

    token1 = 'USDT'
    token2 = 'AVA'
    token3 = 'BTC'
    symbol12 = 'AVAUSDT'
    symbol23 = 'AVABTC'
    symbol31 = 'BTCUSDT'

    qty_usdt = 20
    token1_qty_f = symbols_info[symbol12].lot_size.apply(qty=qty_usdt)
    token2_qty = binance_trade_step_v1(
        trading=trading_binance,
        symbol=symbols_info[symbol12],
        token_from=token1,
        qty=token1_qty_f,
    )

    token2_qty_f = symbols_info[symbol23].lot_size.apply(qty=token2_qty)
    token3_qty = binance_trade_step_v1(
        trading=trading_binance,
        symbol=symbols_info[symbol23],
        token_from=token2,
        qty=token2_qty_f,
    )

    token3_qty_f = symbols_info[symbol31].lot_size.apply(qty=token3_qty)
    token_qty_end = binance_trade_step_v1(
        trading=trading_binance,
        symbol=symbols_info[symbol31],
        token_from=token3,
        qty=token3_qty_f,
    )
    logger.info('token_qty_end(USDT): %s', token_qty_end)


class BinanceDomain:

    def __init__(
        self,
        binance_port: BinancePort,
        price_threshold: float,  # in percents
        handlers: list[IHandler],
    ) -> None:
        self._binance_port = binance_port
        self._symbols_info: typing.Optional[dict[str, Symbol]] = None
        self._order_books: typing.Optional[dict[str, OrderBook]] = None
        self._pairs: typing.Optional[list[BinancePair]] = None
        self._price_threshold = price_threshold
        self._handlers = handlers

    def look_for_opportunities(self) -> None:
        self._fill_symbols_info()
        self._fill_order_books()
        self._fill_pairs()
        self._binance_port.listen_for_tickers(self._handler)

    def _fill_symbols_info(self) -> None:
        """Fills hash map with symbols informations"""
        self._symbols_info = {
            symbol['symbol']: Symbol(
                alias=symbol['symbol'],
                base_asset=symbol['baseAsset'],
                quote_asset=symbol['quoteAsset'],
            )
            for symbol in self._trading_symbols
        }
        logger.info('symbols information have filled')

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
        logger.info('pairs filling started')
        tokens_assets = [
            (x['baseAsset'], x['quoteAsset'])
            for x in self._trading_symbols
        ]
        tokens: set[str] = set([x for l in tokens_assets for x in l])
        pairs = []

        logger.info('pairs generating started')
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
            if pair.l1 == 'num':
                lv_calc = pair1.bid_price
            else:
                lv_calc = 1 / pair1.ask_price
            if pair.l2 == 'num':
                lv_calc *= pair2.bid_price
            else:
                lv_calc *= 1 / pair2.ask_price
            if pair.l3 == 'num':
                lv_calc *= pair3.bid_price
            else:
                lv_calc *= 1 / pair3.ask_price
            pair.value = (lv_calc - 1) * 100
        logger.info('message processed, len: %s', len(message))
        pairs_df = pd.DataFrame([dc.asdict(pair) for pair in self._pairs])
        pairs_df = pairs_df[lambda x: x.value < self._price_threshold]
        pairs_df = pairs_df.sort_values(by=['value'], ascending=False)
        for handler in self._handlers:
            handler.handle_diff(pairs_df=pairs_df, symbols_info=self._symbols_info)

    @functools.cached_property
    def _trading_symbols(self) -> dict:
        symbols = self._binance_port.fetch_symbols()
        return [x for x in symbols if x['status'] == 'TRADING']


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--actions', required=True, type=str, nargs='+')
    args = parser.parse_args()

    commit_hash = os.environ['COMMIT_HASH']
    token = os.environ['TOKEN_TELEGRAM_BOT']
    chat_id_info = os.environ['CHAT_ID_INFO_TELEGRAM_BOT']
    telegram_port_info = TelegramPort(token=token, chat_id=chat_id_info)

    handlers = []
    for action in set(args.actions):
        match action:
            case 'print':
                rows_to_print = int(os.getenv('ROWS_TO_PRINT', '0'))
                handler = PrintHandler(rows_to_print=rows_to_print)
                handlers.append(handler)
            case 'notify':
                notify_threshold = float(os.environ['THRESHOLD_NOTIFY'])
                handler = NotifyHandler(
                    threshold=notify_threshold,
                    telegram_port=telegram_port_info,
                )
                handlers.append(handler)
            case 'trade-v1':
                api_key_binance = os.environ['API_KEY_BINANCE']
                api_secret_binance = os.environ['API_SECRET_BINANCE']
                chat_id = os.environ['CHAT_ID_TRADES_TELEGRAM_BOT']
                trade_threshold = float(os.environ['THRESHOLD_TRADE'])
                signer_binance = Binance.HmacSigner(secret_key=api_secret_binance)
                http_port_binance = HttpPort(url_base='https://api.binance.com')
                trading_binance = Binance.TradingDomain(
                    api_key=api_key_binance,
                    signer=signer_binance,
                    http_port=http_port_binance,
                )
                telegram_port_trades = TelegramPort(token=token, chat_id=chat_id)
                handler = TradeV1Handler(
                    threshold=trade_threshold,
                    telegram_port=telegram_port_trades,
                    trading_binance=trading_binance,
                    commit_hash=commit_hash,
                )
                handlers.append(handler)
            case _:
                logger.warning('incorrect action %s', action)
                return

    price_threshold = float(os.environ['THRESHOLD_PRICE'])
    binance_port = BinancePort()
    binance_domain = BinanceDomain(
        binance_port=binance_port,
        price_threshold=price_threshold,
        handlers=handlers,
    )

    try:
        binance_domain.look_for_opportunities()
    except Exception as error:
        logger.error(error)
        message = json.dumps({
            'type': 'heavy-crypto-binance',
            'commit_hash': commit_hash,
            'action': 'finished',
            'now': dt.datetime.utcnow().isoformat(),
        }, indent=2)
        telegram_port_info.notify_markdown(message=message)
        telegram_port_info.notify_error(error=error)


if __name__ == '__main__':
    test_symbol_new_from_raw()
    test_lot_size_apply()
    test_trade_v1_trade_step()
    configure_logger()
    debug_binance_trade()
    debug_binance_USDT_AVA_BTC()
    # main()
