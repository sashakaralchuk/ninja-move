"""
!pip install pandas mplfinance pandera nb-black-only
"""

import os
import typing
import logging
import importlib
import datetime as dt

import sqlalchemy.orm
import sqlalchemy as sa
import pandas as pd
import mplfinance as mpf


interval_secs_map = {
    '1m': 60,
    '5m': 5*60,
    '15m': 15*60,
    '1h': 60*60,
    '4h': 4*60*60,
    '1d': 24*60*60,
    '1w': 7*24*60*60,
}
s  = mpf.make_mpf_style(
    marketcolors=mpf.make_marketcolors(
        up='#459782',
        down='#df484c',
        edge='inherit',
        wick='inherit',
    ),
    facecolor='#181b25',
)


def get_logger() -> logging.RootLogger:
    importlib.reload(logging)
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    return logger


def get_sa_engine() -> sa.Engine:
    # XXX: cache it
    return sa.create_engine(
        "postgresql://default:default@database:5432/exchanges-arbitrage",
        echo=False,
    )


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


def read_binance_klines() -> pd.DataFrame:
    """Examples:
    btc_df = shared.read_binance_ticker_data()
    """
    dir_path = '/home/jovyan/.var/binance-local/spot/klines/1m/BTCUSDT'
    file_names = os.listdir(dir_path)
    dfs = [pd.read_csv(f'{dir_path}/{file_name}') for file_name in file_names]
    return (pd.concat(dfs, ignore_index=True)
        .assign(date=lambda x: x.open_time.apply(lambda x: dt.datetime.fromtimestamp(x/1000)))
        .set_index('date')
        .sort_index()
    )


def read_binance_trades(date_start: dt.date, date_end: dt.date) -> pd.DataFrame:
    dir_path = "/home/jovyan/.var/binance-local/spot/trades/1m/BTCUSDT"
    date = date_start
    dfs = []
    while date <= date_end:
        file_name = f'BTCUSDT-trades-{date.isoformat()}.csv'
        df = pd.read_csv(f"{dir_path}/{file_name}")
        dfs.append(df)
        date += dt.timedelta(days=1)
    return pd.concat(dfs, ignore_index=True).assign(
        timestamp=lambda x: (x.time / 1000).apply(dt.datetime.fromtimestamp)
    ).sort_values(by=['time'])


def split_df(df: pd.DataFrame, interval_str: str) -> pd.DataFrame:
    """Examples:
    btc_df = shared.read_binance_klines()
    btc_15m_df = shared.split_df(df=btc_df, interval_str='15m')
    """
    interval_millis = interval_secs_map[interval_str] * 1000
    logger = get_logger()
    prev_millis, prev_i = df.open_time.min(), 0
    out_dfs = []
    for i in range(len(df)):
        if df.iloc[i].open_time < prev_millis+interval_millis:
            continue
        slice_df = df.iloc[prev_i:i].assign(open_time=lambda _: prev_millis)
        out_dfs.append(slice_df)
        prev_millis, prev_i = df.iloc[i].open_time, i
    logger.info('len(out_dfs): %s', len(out_dfs))
    out_df = (pd.concat(out_dfs)
        .groupby(by=['open_time'], as_index=False)
        .agg(
            open=('open', 'first'),
            close=('open', 'last'),
            low=('low', 'min'),
            high=('high', 'max'),
            volume=('volume', 'sum'),
        )
        .assign(date=lambda x: x.open_time.apply(lambda x: dt.datetime.fromtimestamp(x/1000)))
        .set_index('date')
    )
    return out_df
