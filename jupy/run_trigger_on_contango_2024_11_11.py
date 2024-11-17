import asyncio
import datetime as dt
import json
import logging
import os
import time
import typing

import asynch
import pydantic as pc
import websockets.asyncio.client as ws_client

import shared

logger = logging.getLogger()


async def main() -> None:
    shared.configure_logger()
    clickhouse_conn = await asynch.connect(dsn="clickhouse://127.0.0.1:9000/default")
    async with ws_client.connect("wss://api.gateio.ws:443/ws/v4/") as ws_gateio:
        async with ws_client.connect("wss://contract.mexc.com:443/edge") as ws_mexc:
            ping_task = asyncio.create_task(fire_ping(ws_mexc=ws_mexc))
            while True:
                await asyncio.sleep(10)
                if opp_row := await is_opportunity_exists(
                    clickhouse_conn=clickhouse_conn
                ):
                    logger.info("opportunity appeared: %s", opp_row)
                    break
                else:
                    logger.info("opportunity doesn't exists")
            await sub_and_watch(ws_gateio=ws_gateio, ws_mexc=ws_mexc, opp_row=opp_row)
            ping_task.cancel()


async def fire_ping(
    ws_gateio: typing.Optional[ws_client.ClientConnection] = None,
    ws_mexc: typing.Optional[ws_client.ClientConnection] = None,
) -> None:
    while True:
        await asyncio.sleep(30)
        if ws_gateio:
            raise NotImplementedError
        if ws_mexc:
            logger.info("mexc ping")
            await ws_mexc.send(json.dumps({"method": "ping"}))


async def is_opportunity_exists(
    clickhouse_conn: asynch.Connection,
) -> typing.Optional["OpportunityRow"]:
    THRESHOLD_REL = float(os.environ["THRESHOLD_REL"])
    QUERY_OPPORTUNITIES = """
        WITH t AS (
            SELECT
                exchange,
                kind,
                symbol,
                UPPER(replaceRegexpAll(symbol, '[10*_-]?', '')) symbol_int_1,
                price / COALESCE(toFloat64OrNull(regexpExtract(symbol, '10*', 0)), 1) price_int_1
            FROM default.trade_contango_arbitrage_v1
            FINAL
            WHERE timestamp >= (now() - toIntervalSecond(60))
                AND length(replaceRegexpOne(symbol, '(-[0-9][0-9][A-Z][A-Z][A-Z][0-9][0-9])', '')) = length(symbol)
                AND volume > 0
                AND status = 'TRADING'
                AND symbol_int_1 != 'DEFIUSDT'
        )
        SELECT
            now() ts,
            t1.symbol_int_1,
            t1.price_int_1 AS fut_price,
            t2.price_int_1 AS spot_price,
            t1.exchange AS fut_ex,
            t2.exchange AS spot_ex,
            t1.symbol AS fut_symbol,
            t2.symbol AS spot_symbol,
            round(fut_price - spot_price, 4) AS diff_abs,
            round((fut_price - spot_price) / spot_price * 100, 2) AS diff_rel
        FROM (
            SELECT *
            FROM (
                SELECT
                    symbol,
                    symbol_int_1,
                    price_int_1,
                    exchange,
                    ROW_NUMBER() OVER(
                        PARTITION BY symbol_int_1, kind
                        ORDER BY price_int_1 DESC
                    ) _rownum
                FROM t
                WHERE kind = 'futures'
            )
            WHERE _rownum = 1
        ) t1
        INNER JOIN (
            SELECT *
            FROM (
                SELECT
                    symbol,
                    symbol_int_1,
                    price_int_1,
                    exchange,
                    ROW_NUMBER() OVER(
                        PARTITION BY symbol_int_1, kind
                        ORDER BY price_int_1 DESC
                    ) _rownum
                FROM t
                WHERE kind = 'spot'
            )
            WHERE _rownum = 1
        ) t2
            ON t1.symbol_int_1 = t2.symbol_int_1
        WHERE diff_rel > %(threshold_rel)s
            AND spot_ex = 'gateio'
            AND fut_ex = 'mexc'
        ORDER BY diff_rel DESC
    """
    async with clickhouse_conn.cursor(cursor=asynch.DictCursor) as curr:
        amount = await curr.execute(
            QUERY_OPPORTUNITIES, args={"threshold_rel": THRESHOLD_REL}
        )
        if amount != 0:
            return OpportunityRow.model_validate(await curr.fetchone())


async def sub_and_watch(
    ws_gateio: ws_client.ClientConnection,
    ws_mexc: ws_client.ClientConnection,
    opp_row: "OpportunityRow",
) -> None:
    assert opp_row.spot_ex == "gateio"
    assert opp_row.fut_ex == "mexc"
    await ws_gateio.send(
        json.dumps(
            {
                "time": int(time.time()),
                "channel": "spot.trades",
                "event": "subscribe",
                "payload": [opp_row.spot_symbol],
            }
        )
    )
    await ws_mexc.send(
        json.dumps({"method": "sub.deal", "param": {"symbol": opp_row.fut_symbol}})
    )
    prices = {}
    while True:
        try:
            t = await asyncio.wait_for(ws_gateio.recv(), timeout=0.1)
            m_gateio = json.loads(t)
        except asyncio.exceptions.TimeoutError:
            m_gateio = None
        try:
            t = await asyncio.wait_for(ws_mexc.recv(), timeout=0.1)
            m_mexc = json.loads(t)
        except asyncio.exceptions.TimeoutError:
            m_mexc = None
        event_name_gateio = (m_gateio or {}).get("event")
        event_name_mexc = (m_mexc or {}).get("channel")
        match event_name_gateio:
            case None:
                pass
            case "update":
                symbol, price = (
                    m_gateio["result"]["currency_pair"],
                    m_gateio["result"]["price"],
                )
                logger.info("gateio: %s %s", symbol, price)
                prices["gateio"] = price
            case _:
                logger.info("gateio unhandled-event=%s", event_name_gateio)
        match event_name_mexc:
            case "pong" | None:
                pass
            case "push.deal":
                symbol, price = (m_mexc["symbol"], m_mexc["data"]["p"])
                logger.info("mexc: %s %s", symbol, price)
                prices["mexc"] = price
            case _:
                logger.info(
                    "mexc unhandled-event=%s m_mexc=%s", event_name_mexc, m_mexc
                )
        if len(prices) == 2:
            # TODO: print here received prices (with event from another process) and make sum-up
            logger.info("push-prices=(%s, %s)", prices["gateio"], prices["mexc"])
            logger.info(" opp-prices=(%s, %s)", opp_row.spot_price, opp_row.fut_price)
            break


class OpportunityRow(pc.BaseModel):
    ts: dt.datetime = pc.Field(strict=True)
    symbol_int_1: str = pc.Field(strict=True, min_length=1)
    fut_price: float = pc.Field(strict=True, gt=0)
    spot_price: float = pc.Field(strict=True, gt=0)
    fut_ex: str = pc.Field(strict=True, min_length=1)
    spot_ex: str = pc.Field(strict=True, min_length=1)
    fut_symbol: str = pc.Field(strict=True, min_length=1)
    spot_symbol: str = pc.Field(strict=True, min_length=1)
    diff_abs: float = pc.Field(strict=True, gt=0)
    diff_rel: float = pc.Field(strict=True, gt=0)


if __name__ == "__main__":
    asyncio.run(main())
