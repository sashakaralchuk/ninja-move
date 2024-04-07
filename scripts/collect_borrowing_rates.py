import logging
import time
import os

import dotenv
import pydantic as pc
import sqlalchemy as sa
import playwright._impl._errors
from playwright import sync_api as p_sync_api

import shared


logger = logging.getLogger()


class Rate(pc.BaseModel):
    asset: pc.StrictStr
    service: pc.StrictStr
    supply_rate: pc.StrictFloat
    borrow_rate: pc.StrictFloat


class RatesPort:
    def __init__(self, engine: sa.Engine) -> None:
        self._engine = engine

    def create_table(self) -> None:
        q = """
            CREATE TABLE IF NOT EXISTS public.borrowing_rates (
                created_at TIMESTAMP NOT NULL,
                asset VARCHAR NOT NULL,
                service VARCHAR NOT NULL,
                supply_rate real NOT NULL,
                borrow_rate real NOT NULL
            );
        """
        logger.info('creata table if exists')
        with self._engine.connect() as con:
            con.execute(sa.text(q))
            con.commit()

    def insert_rates(self, rates: list[Rate]) -> None:
        candles_strs = [
            f'(now(),{rate.asset!r},{rate.service!r},{rate.supply_rate},{rate.borrow_rate})'
            for rate in rates
        ]
        q = f"""
            insert into public.borrowing_rates(created_at,asset,service,supply_rate,borrow_rate)
            values {','.join(candles_strs)};
        """
        logger.info('insert rates')
        with self._engine.connect() as con:
            # XXX: re-make with params
            con.execute(sa.text(q))
            con.commit()


def select_nostra_rates(page: p_sync_api.Page) -> list[Rate]:
    selector_row = (
        '#root > div.nostra__app__root > '
        'div.nostra__shadow-container.nostra__markets-container > '
        'div.nostra__markets__body > div.nostra__markets__table > '
        'table > tbody > tr'
    )
    selector_assets = selector_row + (
        ' > td > div > div > div.nostra__tooltip-wrapper-anchor > '
        'div > div:nth-child(1)'
    )
    selector_asset = (
        'td:nth-child(1) > div > div > div.nostra__tooltip-wrapper-anchor > '
        'div > div:nth-child(1)'
    )
    selector_supply_rate_rewards = 'td:nth-child(4) > div > div > div > div'
    selector_supply_rate_default = 'td:nth-child(4) > div > div'
    selector_borrow_rate = 'td:nth-child(5) > div > div'
    page.goto("https://app.nostra.finance")
    p_sync_api.expect(page.locator(selector_assets)).to_contain_text(['USDC'], timeout=10_000)
    out = []
    for l in page.locator(selector_row).all():
        try:
            supply_rate_str = l.locator(selector_supply_rate_rewards).text_content(timeout=100)
        except playwright._impl._errors.TimeoutError:
            logger.debug('try to get rate with another selector')
            supply_rate_str = l.locator(selector_supply_rate_default).text_content(timeout=100)
        asset = l.locator(selector_asset).text_content()
        supply_rate = float(supply_rate_str.replace('%', ''))
        borrow_rate = float(l.locator(selector_borrow_rate).text_content().replace('%', ''))
        rate = Rate(asset=asset, supply_rate=supply_rate, borrow_rate=borrow_rate,
                    service='nostra')
        logger.info('collected rate=%s', rate)
        out.append(rate)
    return out


def load_and_save_rates(rates_port: RatesPort) -> None:
    # NOTE: could be usefull for aave workout
    #       https://medium.com/covalent-hq/a-comparison-of-historical-variable-borrow-rates-for-usdc-on-aave-3ce65df18dab
    #       briefly: get events ReserveDataUpdated and see what was rate there
    logger.info('open browser and get rates')
    with p_sync_api.sync_playwright() as p:
        browser = p.chromium.launch(headless=True, channel="chrome")
        page = browser.new_page()
        rates = select_nostra_rates(page=page)
        browser.close()
    rates_port.insert_rates(rates=rates)


def main() -> None:
    dotenv.load_dotenv()
    shared.configure_logger()
    rates_port = RatesPort(engine=shared.get_db_engine())
    rates_port.create_table()
    while True:
        try:
            load_and_save_rates(rates_port=rates_port)
        except:
            shared.send_telegram_notify(message='fall', action=os.path.basename(__file__))
        secs = 60 * 60
        logger.info(f'wait {secs} secs')
        time.sleep(secs)


if __name__ == '__main__':
    main()
