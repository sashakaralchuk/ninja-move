import os

import pandas as pd


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
