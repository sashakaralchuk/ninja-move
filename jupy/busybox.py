import argparse
import asyncio
import datetime as dt
import json
import logging
import os
import re
import time
import typing

import asynch
import clickhouse_connect
import playwright._impl._errors
import pydantic as pc
import requests
from playwright import sync_api as p_sync_api
from telethon.sync import TelegramClient
from telethon.tl.types import UpdateNewChannelMessage

logger = logging.getLogger()


def main() -> typing.NoReturn:
    log_level = os.environ.get("LOG_LEVEL", "info").upper()
    logging.basicConfig(level=logging.getLevelNamesMapping()[log_level])
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, help="Input file name", required=True)
    parser.add_argument("--ex", type=str, required=False)
    parser.add_argument("--k", type=str, required=False)
    args_ = parser.parse_args()
    match args_.mode:
        case "parse-coinglass-funding-rates-2025-01-12":
            parse_coinglass_funding_rates_2025_01_12()
        case "parse-coingecko-token-2025-01-06":
            parse_coingecko_token_2025_01_06()
        case "match-ex-tokens-with-ccid-throught-search":
            match_ex_tokens_with_ccid_throught_search(ex=args_.ex, k=args_.k)
        case "match-ex-tokens-with-spot-coingecko-api":
            match_ex_tokens_with_spot_coingecko_api(ex=args_.ex)
        case "fetch-insert-coingecko-tickers":
            fetch_insert_coingecko_tickers(ex=args_.ex)
        case "fetch-telegram-messages":
            fetch_telegram_messages()
        case "print-telegram-updates":
            print_telegram_updates()
        case "listen_and_print_appartments_rent_warsaw":
            listen_and_print_appartments_rent_warsaw()
        case _:
            raise NotImplementedError(f"Unknown {args_.mode=}")


def parse_coinglass_funding_rates_2025_01_12() -> typing.NoReturn:
    clickhouse_client = clickhouse_connect.get_client(
        dsn="clickhousedb://127.0.0.1:18123/default"
    )
    clickhouse_client.command(
        """
        CREATE TABLE IF NOT EXISTS default.funding_rates_2025_01_12 (
            `token_text` String,
            `ex_text` String,
            `funding_rate_text` String,
            `ts_millis` Int64
        )
        ENGINE = TinyLog;
        """,
    )
    selector_button_show_all = (
        "#__next > div > div.cg-content.MuiBox-root.cg-style-vsqgwu > div.plr20 "
        "> div > div.MuiBox-root.cg-style-5qclv7 > button"
    )
    selector_rows_fundings_rates = (
        "#__next > div > div.cg-content.MuiBox-root.cg-style-vsqgwu > div.plr20 > "
        "div > div.ant-table-wrapper.cg-fr-table > div > div > div > div > "
        "div.ant-table-body > table > tbody tr[data-row-key]"
    )
    selector_head_exs = (
        "#__next > div > div.cg-content.MuiBox-root.cg-style-vsqgwu > div.plr20 > "
        "div > div.ant-table-wrapper.cg-fr-table > div > div > div > div > "
        "div.ant-table-header.ant-table-sticky-holder > table > thead > tr > th"
    )
    selector_consent_button = (
        "body > div.fc-consent-root > div.fc-dialog-container > "
        "div.fc-dialog.fc-choice-dialog > div.fc-footer-buttons-container > "
        "div.fc-footer-buttons > button.fc-button.fc-cta-consent.fc-primary-button > p"
    )
    with p_sync_api.sync_playwright() as p:
        browser = p.chromium.launch(headless=False, channel="chrome")
        page = browser.new_page()
        page.goto("https://www.coinglass.com/FundingRate")
        page.locator(selector_consent_button).click()
        page.locator(selector_button_show_all).click()
        exs_names = []
        for head_th in page.locator(selector_head_exs).all():
            symbol_names = head_th.locator(".symbol-name").all()
            if len(symbol_names) > 0:
                assert len(symbol_names) == 1
                exs_names.append(symbol_names[0].text_content())
        for table_row in page.locator(selector_rows_fundings_rates).all():
            fundings_rates = []
            token_name = ""
            for i, row_td in enumerate(table_row.locator("> td").all()):
                if i == 0:
                    continue
                if i == 1:
                    token_name = row_td.locator("div.symbol-name").text_content()
                    print(f"{token_name=}")
                    continue
                if row_td.get_attribute("style") == "text-align: left;":
                    break
                if token_name == "BTC":
                    row_td_a = row_td.locator("a.shou").all()[0]
                else:
                    row_td_a = row_td.locator("a.shou")
                ex_text = exs_names[i - 2]
                funding_rate_text = row_td_a.text_content()
                o = TokenFundingRate(
                    token_text=token_name,
                    ex_text=ex_text,
                    funding_rate_text=funding_rate_text,
                    ts_millis=int(time.time() * 1000),
                )
                fundings_rates.append(o)
            logger.info(
                "processed token_name=%s len(fundings_rates)=%s",
                token_name,
                len(fundings_rates),
            )
            clickhouse_client.insert(
                "default.funding_rates_2025_01_12",
                [
                    (x.token_text, x.ex_text, x.funding_rate_text, x.ts_millis)
                    for x in fundings_rates
                ],
            )
        browser.close()


async def conn_exec_clickhouse_query(*args, **kwargs) -> typing.NoReturn:
    clickhouse_conn = await asynch.connect(dsn="clickhouse://127.0.0.1:9000/default")
    async with clickhouse_conn.cursor(cursor=asynch.DictCursor) as curr:
        await curr.execute(*args, **kwargs)
        # XXX: ret == 1


class TokenFundingRate(pc.BaseModel):
    token_text: pc.StrictStr
    ex_text: pc.StrictStr
    funding_rate_text: pc.StrictStr
    ts_millis: pc.StrictInt


def parse_coingecko_token_2025_01_06() -> typing.NoReturn:
    coingecko_coin_id = "fractal-bitcoin"
    with p_sync_api.sync_playwright() as p:
        browser = p.chromium.launch(headless=False, channel="chrome")
        page = browser.new_page()
        page.goto(f"https://www.coingecko.com/en/coins/{coingecko_coin_id}")
        markets_choose_k(page=page, k="spot")
        markets_choose_100_rows(page=page)
        _spot_exchanges = markets_select_curr_exchanges(
            page=page, token=coingecko_coin_id, k="spot"
        )
        page.goto(f"https://www.coingecko.com/en/coins/{coingecko_coin_id}")
        markets_choose_k(page=page, k="fut")
        markets_choose_100_rows(page=page)
        _fut_exchanges = markets_select_curr_exchanges(
            page=page, token=coingecko_coin_id, k="fut"
        )
        browser.close()


class TokenExchange(pc.BaseModel):
    token: pc.StrictStr
    k: pc.StrictStr
    ex_href: pc.StrictStr
    data_analytics_event_properties_str: pc.StrictStr
    ex_name: pc.StrictStr
    trade_url: pc.StrictStr


def markets_choose_k(page: p_sync_api.Page, k: str) -> typing.NoReturn:
    match k:
        case "spot":
            page.locator("#spot").click()
        case "fut":
            page.locator("#perpetuals").click()
        case _:
            raise NotImplementedError


def markets_choose_100_rows(page: p_sync_api.Page) -> typing.NoReturn:
    selector_button_pag = (
        "body > div > main > div > div > div > div > div > "
        "div.gecko-pagination-selector > div > div > button"
    )
    selector_100_rows_pag = (
        "body > div > main > div > div > div > div > div > "
        "div.gecko-pagination-selector > div > div > div > div:nth-child(3) > span"
    )
    page.locator(selector_button_pag).click()
    page.locator(selector_100_rows_pag).click()
    selector_page_pag = "body > div > main > div > div > div > div > div > nav > span"
    page.wait_for_function(f"""
        () => document.querySelectorAll("{selector_page_pag}").length == 3
    """)


def markets_select_curr_exchanges(
    page: p_sync_api.Page, token: str, k: str
) -> list[TokenExchange]:
    selector_section = (
        "body > div > main > div > div > div > div table > tbody:nth-child(4) > tr"
    )
    selector_link_to_ex = "td:nth-child(2) > div > a"
    selector_link_to_trade = "td > div > a[data-analytics-event-properties]"
    out = []
    for row in page.locator(selector_section).all():
        ex_href_str = row.locator(selector_link_to_ex).get_attribute("href")
        try:
            data_analytics_event_properties_str = row.locator(
                selector_link_to_trade
            ).get_attribute("data-analytics-event-properties", timeout=100)
            ex_name = json.loads(data_analytics_event_properties_str)["exchange_name"]
        except playwright._impl._errors.TimeoutError:
            data_analytics_event_properties_str = "{}"
            ex_name = ""
        try:
            trade_url = row.locator(selector_link_to_trade).get_attribute(
                "href", timeout=100
            )
        except playwright._impl._errors.TimeoutError:
            trade_url = ""
        out.append(
            TokenExchange(
                token=token,
                k=k,
                ex_href=ex_href_str,
                data_analytics_event_properties_str=data_analytics_event_properties_str,
                ex_name=ex_name,
                trade_url=trade_url,
            )
        )
    return out


def fetch_insert_coingecko_tickers(ex: str) -> typing.NoReturn:
    if ex is None:
        raise ValueError("ex is mandatory")
    clickhouse_dsn = "clickhousedb://127.0.0.1:18123/default"
    clickhouse_client = clickhouse_connect.get_client(dsn=clickhouse_dsn)
    clickhouse_client.query(
        """
        CREATE TABLE IF NOT EXISTS default.tickers_coingecko_2025_04_09 (
            obj_raw String,
            ex String,
            k String,
            coin_id String,
            trade_url String,
            base String,
            target String,
            last String,
            ts_write DateTime
        )
        ENGINE = TinyLog;
        """
    )
    ex_coingecko = None
    match ex:
        case "mexc":
            ex_coingecko = "mxc"
        case "gateio":
            ex_coingecko = "gate"
        case "kucoin":
            ex_coingecko = "kucoin"
        case _:
            raise Exception(f"unknown ex={ex}")
    i = 0
    while True:
        logger.info("load coingecko tickers i=%s", i)
        res = requests.get(
            f"https://api.coingecko.com/api/v3/exchanges/{ex_coingecko}/tickers?page={i}",
            headers={
                "x-cg-demo-api-key": "CG-SW1M45WZhgX1R29iEfWJYCme",
            },
        )
        res.raise_for_status()
        if len(res.json()["tickers"]) == 0:
            logger.info("tickers are empty => break")
            break
        rows = [
            [
                json.dumps(obj),
                ex,
                "spot",
                obj["coin_id"],
                obj["trade_url"],
                obj["base"],
                obj["target"],
                str(obj["last"]),
                int(time.time()),
            ]
            for obj in res.json()["tickers"]
        ]
        clickhouse_client.insert("default.tickers_coingecko_2025_04_09", rows)
        i += 1


def match_ex_tokens_with_ccid_throught_search(  # noqa: PLR0912, PLR0915
    ex: typing.Optional[str],
    k: typing.Optional[str],
) -> typing.NoReturn:
    if ex is None:
        raise ValueError("ex is mandatory")
    clickhouse_dsn = "clickhousedb://127.0.0.1:18123/default"
    clickhouse_client = clickhouse_connect.get_client(dsn=clickhouse_dsn)
    if ex in ("bybit", "binance", "mexc", "gateio", "bitget", "kucoin"):
        if k is None:
            raise ValueError("k is mandatory")
        last_price_path = None
        match ex:
            case "bybit":
                last_price_path = "$.bid1Price"
            case "binance":
                last_price_path = "$.bidPrice"
            case "mexc":
                last_price_path = "$.lastPrice"
            case "gateio":
                last_price_path = "$.last"
            case "bitget":
                last_price_path = "$.lastPr"
            case "kucoin":
                if k == "fut":
                    last_price_path = "$.price"
                elif k == "spot":
                    last_price_path = "$.buy"
                else:
                    raise Exception(f"Unknown {k=}")
            case _:
                raise Exception(f"Unknown {ex=}")
        symbols_in_tickers = clickhouse_client.query(
            f"""
            SELECT s, last_price
            FROM (
                SELECT *,
                    toFloat64(JSON_VALUE(obj_raw, {last_price_path!r})) last_price,
                    RANK() OVER (ORDER BY ts_write DESC) AS rank_out
                FROM default.tickers
                WHERE ex = {ex!r} AND k = {k!r}
                    AND ts_write >= NOW() - INTERVAL 30 MINUTE
            )
            WHERE rank_out = 1
            ORDER BY s
            """,
        ).result_rows
    else:
        match ex:
            case "bitunix":
                last_price_path = "$.lastPrice"
            case "coinex":
                last_price_path = "$.mark_price"
            case "bingx":
                last_price_path = "$.indexPrice"
            case _:
                raise Exception(f"Unknown {ex=}")
        symbols_in_tickers = clickhouse_client.query(
            f"""
            SELECT * EXCEPT(rank_out, ts_write)
            FROM (
                SELECT *, RANK() OVER (ORDER BY ts_write DESC) AS rank_out
                FROM (
                    SELECT
                        s,
                        toFloat64(JSON_VALUE(obj_raw, {last_price_path!r})) last_price,
                        ts_write
                    FROM default.fundings_curr_2025_01_12
                    WHERE ex = {ex!r} AND k = {k!r}
                )
            )
            WHERE rank_out = 1
            ORDER BY s
            """,
        ).result_rows
    logger.info("len(symbols_in_tickers)=%s", len(symbols_in_tickers))
    non_matched = []
    for symbol_in_ticker, price_in_ticker in symbols_in_tickers:
        coin_in_ticker = (
            symbol_in_ticker.replace("USDTM", "")
            .replace("_USDT", "")
            .replace("-USDT", "")
            .replace("USDT", "")
        )
        url_search = (
            f"https://www.coingecko.com/en/search_v2?"
            f"query={coin_in_ticker}&vs_currency=usd"
        )
        for _ in range(5):
            res_search = requests.get(url_search)
            if res_search.status_code == 429:  # noqa: PLR2004
                secs = 10
                print(f"Rate limit exceeded, sleeping for {secs} seconds...")
                time.sleep(secs)
                continue
            res_search.raise_for_status()
            break
        match_list = []
        for coin_obj in res_search.json()["coins"]:
            if coin_obj["symbol"] == coin_in_ticker:
                try:
                    price_cg = float(coin_obj["data"]["price"].replace("$", ""))
                    if abs(price_cg - price_in_ticker) / price_in_ticker * 100 < 1:
                        match_list.append((coin_in_ticker, coin_obj["id"]))
                except (ValueError, ZeroDivisionError):
                    pass
        if len(match_list) == 1:
            date_str = dt.date.today().isoformat()
            print(
                f"({ex!r}, {k!r}, {symbol_in_ticker!r}, {match_list[0][0]!r}, "
                f"'USDT', {match_list[0][1]!r}, 0, "
                f"'added-by-jupy-busybox-on-{date_str}'),"
            )
        else:
            non_matched.append((symbol_in_ticker, match_list))
    for symbol_in_ticker, match_list in non_matched:
        print(f"{symbol_in_ticker=} {ex=} {k=} match_list !=1 {match_list=}")


def match_ex_tokens_with_spot_coingecko_api(
    ex: typing.Optional[str],
) -> typing.NoReturn:
    if ex is None:
        raise ValueError("ex is mandatory")
    clickhouse_dsn = "clickhousedb://127.0.0.1:18123/default"
    clickhouse_client = clickhouse_connect.get_client(dsn=clickhouse_dsn)
    q_tickers_price, q_s_concat = None, None
    match ex:
        case "mexc":
            q_tickers_price = "$.lastPrice"
            q_s_concat = "CONCAT(base, target)"
        case "gateio":
            q_tickers_price = "$.last"
            q_s_concat = "CONCAT(base, '_', target)"
        case "kucoin":
            pass
            q_tickers_price = "$.last"
            q_s_concat = "CONCAT(base, '-', target)"
        case _:
            raise Exception(f"unknown ex={ex}")
    query_str = f"""
        SELECT
            t1.s,
            t2.coin_id,
            t2.base,
            t2.target,
            truncate(t1.last, 4) t1_last,
            truncate(t2.last, 4) t2_last,
            truncate(abs((t1.last - t2.last) / t1.last) * 100, 4) diff_rel
        FROM (
            SELECT * EXCEPT(rank_last_write)
            FROM (
                SELECT
                    s,
                    toFloat64(JSON_VALUE(obj_raw, {q_tickers_price!r})) last,
                    row_number() OVER (PARTITION BY ex, k, s ORDER BY ts_write DESC) as rank_last_write
                FROM default.tickers t1
                LEFT JOIN default.ex_k_to_ccid_v2 t2
                    USING (ex, k, s)
                WHERE t1.ex = {ex!r}
                    AND k = 'spot'
                    AND JSON_VALUE(obj_raw, {q_tickers_price!r}) != 'null' -- NOTE: kucoin specific
                    AND ts_write >= NOW() - INTERVAL 7 DAY
                    AND t2.ccid = ''
                    AND startsWith(t2.notes, 'index-fut-') = 0
                    AND startsWith(t2.notes, 'non-fut-perp-') = 0
                    AND startsWith(t2.notes, 'non-on-ui-') = 0
                    AND startsWith(t2.notes, 'non-usd-based-') = 0
            )
            WHERE rank_last_write = 1
        ) t1
        LEFT JOIN (
            SELECT * EXCEPT(rank_last_write)
            FROM (
                SELECT
                    {q_s_concat} s,
                    toFloat64(last) last,
                    coin_id,
                    base,
                    target,
                    row_number() OVER (PARTITION BY ex, k, s ORDER BY ts_write DESC) as rank_last_write
                FROM default.tickers_coingecko_2025_04_09
                WHERE ex = {ex!r} AND k = 'spot'
            )
            WHERE rank_last_write = 1
        ) t2
            ON t1.s = t2.s
        WHERE t2.coin_id != ''
        ORDER BY diff_rel DESC
    """
    s_coingecko_match_list = clickhouse_client.query(query_str).result_rows
    date_str = dt.date.today().isoformat()
    notes = f"added-by-jupy-busybox-coingecko-api-on-{date_str}"
    logger.info("iterate through tickers notes=%s", notes)
    for obj in s_coingecko_match_list:
        (t1_s, t2_coin_id, t2_base, t2_target, _, _, diff_rel) = obj
        if diff_rel > 3.0:
            print(f"workout: {t1_s} {t2_coin_id}")
            continue
        print(
            f"({ex!r}, 'spot', {t1_s!r}, {t2_base!r}, {t2_target!r}, "
            f"{t2_coin_id!r}, 0, {notes!r}),"
        )


def fetch_telegram_messages() -> typing.NoReturn:
    api_id, api_hash, chat_id = (
        os.environ["API_ID"],
        os.environ["API_HASH"],
        int(os.environ["CHAT_ID"]),
    )
    logger.info("download message for chat_id=%s", chat_id)
    with TelegramClient("", api_id, api_hash) as client:
        dialogs = client.get_dialogs(archived=False)
        dialog_dest = [x for x in dialogs if x.id == chat_id][0]
        d0 = dt.date.today()
        d1 = d0 - dt.timedelta(days=1)
        messages_d1 = []
        for message in client.iter_messages(dialog_dest, offset_date=d0):
            if message.date.date() != d1:
                break
            messages_d1.append(message)
        messages_d1.sort(key=lambda x: x.date)
    filename = f".var/messages-{chat_id}-{d1}.jsonl"
    logger.info("write len(messages_d1)=%s into filename=%s", len(messages_d1), filename)
    with open(filename, "w") as f:
        for m in messages_d1:
            o = {
                "date": m.date.isoformat(),
                "username": m.sender.username,
                "text": m.text,
            }
            f.write(json.dumps(o, ensure_ascii=False))
            f.write("\n")


def listen_and_print_appartments_rent_warsaw() -> typing.NoReturn:
    """
    Channel "Аренда жилья Варшава" on 2025-06-07 has handle @home_Warszawa
    and channel_id=1726457020.

    Run: `LOG_LEVEL=debug API_ID= API_HASH= CHANNEL_ID=1726457020 \
        uv run busybox.py --mode=listen_and_print_appartments_rent_warsaw`
    """
    api_id, api_hash, channel_id = (
        os.environ["API_ID"],
        os.environ["API_HASH"],
        os.environ["CHANNEL_ID"],
    )

    async def handler(update: UpdateNewChannelMessage) -> typing.NoReturn:
        if not isinstance(update, UpdateNewChannelMessage):
            logger.debug("not UpdateNewChannelMessage => skip update=%s", update)
            return
        if update.message.peer_id.channel_id != channel_id:
            logger.debug("not channel_id=%s => skip message", channel_id)
            return
        message_text = update.message.message
        logger.debug("handle message_text=%s", message_text)
        area = re.findall("Район: (.*)\n", message_text)[0]
        prices_strs = re.findall(r"Цена: ([0-9]*) zł \[\+([0-9]*) zł ", message_text)
        price = sum(map(lambda x: float(x), prices_strs[0]))
        offer_link = None
        for reply_markup_row in update.message.reply_markup.rows:
            for reply_markup_button in reply_markup_row.buttons:
                if reply_markup_button.text == "К объявлению":
                    offer_link = reply_markup_button.url
        # XXX: send telegram notify here
        logger.info("Found area=%s price=%s offer_link=%s", area, price, offer_link)

    with TelegramClient("default-persistant-session", api_id, api_hash) as client:
        # XXX: store this session between runs
        client.add_event_handler(handler)
        client.run_until_disconnected()


def print_telegram_updates() -> typing.NoReturn:
    asyncio.get_event_loop().run_until_complete(_print_telegram_updates())


def _print_telegram_updates() -> asyncio.Future:
    api_id, api_hash = (os.environ["API_ID"], os.environ["API_HASH"])

    async def handler(update):
        print(update)

    with TelegramClient("", api_id, api_hash) as client:
        client.add_event_handler(handler)
        client.run_until_disconnected()


if __name__ == "__main__":
    main()
