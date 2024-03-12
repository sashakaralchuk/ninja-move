import typing
import abc
import hmac
import hashlib
import time

import requests
import pydantic as pc


class HttpPort:

    def __init__(self, url_base: str):
        self._url_base = url_base

    def get(self, path: str, *args, **kwargs) -> requests.Response:
        return requests.get(self._url(path), *args, **kwargs)

    def post(self, path: str, *args, **kwargs) -> requests.Response:
        return requests.post(self._url(path), *args, **kwargs)

    def _url(self, path: str) -> str:
        return self._url_base + path


class TradeSumUp(pc.BaseModel):
    success: pc.StrictBool
    status_code: pc.StrictInt
    body: typing.Dict

    class Config:
        allow_mutation = False


class IBinanceSignature(abc.ABC):

    @abc.abstractmethod
    def initialize(self) -> None:
        pass

    @abc.abstractmethod
    def signature(self, content: typing.Union[str, typing.Dict]) -> str:
        pass


class Binance:

    class SignerContent:

        def __init__(self, content: typing.Union[str, typing.Dict]) -> None:
            self._content = content

        @property
        def payload(self) -> str:
            return '&'.join([
                f'{param}={value}'
                for param, value in self._content.items()
            ]) if isinstance(self._content, dict) else self._content

    class HmacSigner(IBinanceSignature):

        def __init__(self, secret_key: str):
            self._secret_key = secret_key

        def initialize(self) -> None:
            pass

        def signature(self, content: typing.Union[str, typing.Dict]) -> str:
            payload = Binance.SignerContent(content=content).payload
            # https://github.com/binance/binance-signature-examples/blob/master/python/hmac_sha256_signature.py
            return hmac.new(
                self._secret_key.encode('utf-8'),
                payload.encode('utf-8'),
                hashlib.sha256,
            ).hexdigest()

    class TradingDomain:

        def __init__(
            self,
            api_key: str,
            signer: IBinanceSignature,
            http_port: HttpPort,
        ) -> None:
            self._api_key = api_key
            self._signer = signer
            self._http_port = http_port
            self._signer.initialize()

        def limit(self) -> typing.Dict:
            params = dict(
                symbol='ETHUSDT',
                side='SELL',
                type='LIMIT',
                timeInForce='GTC',
                quantity='0.05',
                price='1850',
            )
            return self._http_port.post(
                '/api/v3/order',
                params=self._wrap_params(params),
                headers=self._headers,
            ).json()

        def market(
            self,
            symbol: str,
            side: str,
            quantity: float = 0.0,
            quote_order_qty: float = 0.0,
        ) -> TradeSumUp:
            """ETHUSDT
            for side[BUY] quantity in ETH
            for side[SELL] quantity in ETH
            """
            if quantity:
                req_quantity = {'quantity': str(quantity)}
            elif quote_order_qty:
                req_quantity = {'quoteOrderQty': str(quote_order_qty)}
            else:
                raise Exception('valid quantitist havent passed')
            params = dict(
                type='MARKET',
                symbol=symbol,
                side=side,
                **req_quantity,
            )
            response = self._http_port.post(
                '/api/v3/order',
                params=self._wrap_params(params),
                headers=self._headers,
            )
            status_code, body = response.status_code, response.json()
            success = status_code == 200 and body.get('status') == 'FILLED'
            return TradeSumUp(
                success=success,
                status_code=status_code,
                body=body,
            )

        def orders(self) -> typing.Dict:
            params = dict(symbol='ETHUSDT')
            return self._http_port.get(
                '/api/v3/allOrders',
                params=self._wrap_params(params),
                headers=self._headers,
            ).json()

        def account(self) -> typing.Dict:
            return self._http_port.get(
                '/api/v3/account',
                params=self._wrap_params(dict()),
                headers=self._headers,
            ).json()

        def ticker_price(self, symbol: str) -> typing.Dict:
            return self._http_port.get(
                '/api/v3/ticker/price',
                params=dict(symbol=symbol),
            ).json()

        def assert_balance(self, eth: float = 0.0, usdt: float = 0.0) -> None:
            account = self.account()
            balances = {
                x['asset']: float(x['free'])
                for x in account['balances']
                if float(x['free']) > 0
            }
            if eth:
                assert balances.get('ETH', 0) > eth
            if usdt:
                assert balances.get('USDT', 0) > usdt

        def _wrap_params(self, params: typing.Dict) -> typing.Dict:
            timestamp = int(time.time() * 1000)
            params_ = dict(**params, timestamp=timestamp)
            signature = self._signer.signature(params_)
            return dict(**params_, signature=signature)

        @property
        def _headers(self) -> typing.Dict:
            return {'X-MBX-APIKEY': self._api_key}
