import os
import logging
import json
import datetime as dt

import requests
import sqlalchemy as sa


def configure_logger() -> None:
    logging.basicConfig(level=logging.INFO)


def get_db_engine() -> sa.Engine:
    # XXX: cache calls
    host = os.environ['POSTGRES_HOST']
    port = os.environ['POSTGRES_PORT']
    user = os.environ['POSTGRES_USER']
    password = os.environ['POSTGRES_PASSWORD']
    dbname = os.environ['POSTGRES_DBNAME']
    conn_str = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    return sa.create_engine(conn_str)


def send_telegram_notify(message: str, action: str) -> None:
    token = os.environ['TELEGRAM_BOT_API_KEY']
    chat_id = os.environ['TELEGRAM_BOT_CHAT_ID']
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    o_str = json.dumps({"message": message, "action": action, "now": now}, indent=4)
    m = f"```%0A{o_str}```"
    url = (
        f"https://api.telegram.org/bot{token}/sendMessage"
        f"?chat_id={chat_id}&text={m}&parse_mode=Markdown"
    )
    res = requests.get(url)
    res.raise_for_status()
