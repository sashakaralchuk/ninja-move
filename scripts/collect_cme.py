import asyncio
import json
import logging
import time

import dotenv
import pydantic as pc
import sqlalchemy as sa
from playwright.async_api import async_playwright

import shared


logger = logging.getLogger()


class Candle(pc.BaseModel):
    timestamp_secs: pc.StrictFloat
    open: pc.StrictFloat
    close: pc.StrictFloat
    high: pc.StrictFloat
    low: pc.StrictFloat
    volume: pc.StrictFloat


class CandlesPort:
    def __init__(self, engine: sa.Engine) -> None:
        self._engine = engine

    def create_table(self) -> None:
        q = """
            CREATE TABLE IF NOT EXISTS public.cme (
                created_at TIMESTAMP NOT NULL,
                open_time TIMESTAMP NOT NULL,
                open real NOT NULL,
                close real NOT NULL,
                high real NOT NULL,
                low real NOT NULL,
                volume real NOT NULL
            );
        """
        logger.info('creata table if exists')
        with self._engine.connect() as con:
            con.execute(sa.text(q))
            con.commit()

    def insert_candles(self, candles: list[Candle]) -> None:
        candles_strs = [
            (
                f'(now(),to_timestamp({candle.timestamp_secs})::timestamp,{candle.open},'
                f'{candle.close},{candle.high},{candle.low},{candle.volume})'
            )
            for candle in candles
        ]
        q = f"""
            insert into public.cme (created_at, open_time, open, close, high, low, volume)
            values {','.join(candles_strs)};
        """
        logger.info('insert candles')
        with self._engine.connect() as con:
            # XXX: re-make with params
            con.execute(sa.text(q))
            con.commit()

    def drop_duplicates(self) -> None:
        q = """
            create table public._cme (LIKE public.cme);
            insert into public._cme
            select distinct on (open_time) *
            from (select * from public.cme order by created_at desc) as t;
            DROP TABLE public.cme;
            ALTER TABLE public._cme 
            rename to cme;
        """
        logger.info('drop duplicates')
        with self._engine.connect() as con:
            con.execute(sa.text(q))
            con.commit()


async def fetch_and_save_candles(candles_port: CandlesPort) -> None:
    received_frames = []
    def on_web_socket(ws) -> None:
        ws.on("framereceived", lambda payload: received_frames.append(payload))
    logger.info('open browser and get candles')
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, channel="chrome")
        page = await browser.new_page()
        page.on("websocket", on_web_socket)
        await page.goto("https://www.tradingview.com/chart/?symbol=CME%3ABTC1!")
        await page.locator('#header-toolbar-intervals > button').click()
        await page.locator('#overlap-manager-root > div > span > div.menuWrap-Kq3ruQo8 > div > div > div > div:nth-child(21) > div').click()
        await asyncio.sleep(5)
        await browser.close()
    candles = []
    for frame_str in received_frames:
        frame_dicts = []
        for s in frame_str.split('~m~'):
            try:
                int(s)
                parsed_to_int = True
            except ValueError:
                parsed_to_int = False
            if s == '' or parsed_to_int:
                continue
            frame_dicts.append(json.loads(s))
        for d in frame_dicts:
            try:
                d_candles = d['p'][1]['sds_1']['s']
                interval = d_candles[5]['v'][0] - d_candles[4]['v'][0]
                if int(interval) != 3600:
                    break
                candles_raw = [
                    Candle(
                        timestamp_secs=l['v'][0],
                        open=l['v'][1],
                        high=l['v'][2],
                        low=l['v'][3],
                        close=l['v'][4],
                        volume=l['v'][5],
                    )
                    for l in d_candles
                ]
                candles.append(candles_raw)
            except:
                pass
        pass
    assert len(candles) == 1
    candles_port.insert_candles(candles=candles[0])


def main():
    dotenv.load_dotenv()
    shared.configure_logger()
    candles_port = CandlesPort(engine=shared.get_db_engine())
    candles_port.create_table()
    while True:
        asyncio.run(fetch_and_save_candles(candles_port=candles_port))
        secs = 60 * 30
        logger.info(f'wait {secs} secs')
        time.sleep(secs)
        candles_port.drop_duplicates()


if __name__ == '__main__':
    main()
