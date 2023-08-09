import dataclasses as dc

import requests


@dc.dataclass
class Symbol:
    alias: str
    base_coin: str
    quote_coin: str


@dc.dataclass
class Ticker:
    symbol_alias: str
    last_price: float


class PortError(Exception):
    pass


class BybitPort:
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

    def _valdiate_status(self, response: requests.Response) -> None:
        status = response.status_code
        if status != 200:
            message = 'request to %s failed, status_code %s, body: %s'
            url, text = response.url, response.text
            raise PortError(message.format(url, status, text))


def main() -> None:
    bybit_port = BybitPort()
    symbols = bybit_port.fetch_symbols()
    tickers = bybit_port.fetch_tickers()
    breakpoint()
    pass


if __name__ == '__main__':
    main()
