import os
import logging
import importlib
import datetime as dt

import pandas as pd
from binance_historical_data import BinanceDataDumper


def get_logger() -> logging.RootLogger:
    importlib.reload(logging)
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    return logger


def read_matic_k_lines() -> pd.DataFrame:
    columns = [
        'open_time', 'open', 'high', 'low', 'close',
        'volume', 'close_time', 'quote_volume',
        'count', 'taker_buy_volume',
        'taker_buy_quote_volume', 'ignore',
    ]
    dfs = [
        pd.read_csv('/home/jovyan/.var/' + p, header=None)
        for p in os.listdir('/home/jovyan/.var')
        if 'MATICUSDT-1m' in p
    ]
    matic_df = pd.concat(dfs)
    matic_df.columns = columns
    return matic_df


def dump_binance_klines(tickers: list[str]) -> None:
    """Downloads binance klines into files.
    
    Examples:
    dump_binance_klines(tickers=['BTCUSDT'])
    """
    data_dumper = BinanceDataDumper(
        path_dir_where_to_dump="/home/jovyan/.var/binance",
        asset_class="spot",
        data_type="klines",
        data_frequency="1m",
    )
    data_dumper.dump_data(
        tickers=tickers,
        date_start=dt.date(year=2024, month=1, day=1),
        date_end=dt.date(year=2024, month=2, day=1),
        is_to_update_existing=True,
    )


def read_binance_ticker_data() -> pd.DataFrame:
    columns = ['open_time', 'open', 'high', 'low', 'close', 'volume',
               'kline_close_time', 'quote_asset_volume', 'number_of_trades',
               'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
               't']
    file_path = '/home/jovyan/.var/binance/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.csv'
    btc_df = (pd.read_csv(file_path, header=None, names=columns)
        .assign(_open_time=lambda x: x.open_time.apply(lambda x: dt.datetime.fromtimestamp(x/1000)))
    )
    return btc_df
