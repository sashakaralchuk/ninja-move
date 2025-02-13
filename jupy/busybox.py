import argparse
import json
import logging
import time
import typing

import asynch
import clickhouse_connect
import playwright._impl._errors
import pydantic as pc
import requests
from playwright import sync_api as p_sync_api

logger = logging.getLogger()


def main() -> typing.NoReturn:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, help="Input file name", required=True)
    parser.add_argument("--ex", type=str, required=False)
    args_ = parser.parse_args()
    match args_.mode:
        case "parse-coinglass-funding-rates-2025-01-12":
            parse_coinglass_funding_rates_2025_01_12()
        case "parse-coingecko-token-2025-01-06":
            parse_coingecko_token_2025_01_06()
        case "match-bitunix-tokens-with-ccid":
            match_bitunix_tokens_with_ccid(ex=args_.ex)
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


def match_bitunix_tokens_with_ccid(ex: typing.Optional[str]) -> typing.NoReturn:
    if ex is None:
        raise ValueError("ex is mandatory")
    clickhouse_client = clickhouse_connect.get_client(
        dsn="clickhousedb://127.0.0.1:18123/default"
    )
    match ex:
        case "bitunix":
            last_price_path = "$.lastPrice"
        case "coinex":
            last_price_path = "$.mark_price"
        case "bingx":
            last_price_path = "$.indexPrice"
        case "mexc":
            last_price_path = "$.lastPrice"
        case _:
            raise Exception(f"Unknown {ex=}")
    symbols_in_tickers = clickhouse_client.query(
        f"""
        SELECT * EXCEPT(rank_out, ts_write)
        FROM (
            SELECT *, RANK() OVER (ORDER BY ts_write DESC) AS rank_out
            FROM (
                SELECT
                    symbol,
                    toFloat64(JSON_VALUE(ticker_raw, {last_price_path!r})) last_price,
                    ts_write
                FROM default.fundings_curr_2025_01_12
                WHERE ex = {ex!r}
            )
        )
        WHERE rank_out = 1
        ORDER BY symbol
        """,
    ).result_rows
    logger.info("len(symbols_in_tickers)=%s", len(symbols_in_tickers))
    for symbol_in_ticker, price_in_ticker in symbols_in_tickers:
        coin_in_ticker = (
            symbol_in_ticker.replace("_USDT", "")
            .replace("-USDT", "")
            .replace("USDT", "")
        )
        url_search = (
            f"https://www.coingecko.com/en/search_v2?"
            f"query={coin_in_ticker}&vs_currency=usd"
        )
        res_search = requests.get(url_search)
        res_search.raise_for_status()
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
            print(match_list[0][0], match_list[0][1])
        else:
            print(f"{symbol_in_ticker=} match_list !=1 {match_list=}")


if __name__ == "__main__":
    main()
