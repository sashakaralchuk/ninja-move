import json
import typing

import playwright._impl._errors
import pydantic as pc
from playwright import sync_api as p_sync_api


def main() -> None:
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


if __name__ == "__main__":
    main()
