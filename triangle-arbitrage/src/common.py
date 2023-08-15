import typing
import logging
import os

import requests
import pandas as pd


logger = logging.getLogger()


def configure_logger() -> None:
    level_env = os.environ.get('LOG_LEVEL', 'WARNING')
    logging.basicConfig(
        level=getattr(logging, level_env, 'WARNING'),
        format='[%(asctime)s][%(levelname)s] %(message)s',
    )


class TelegramPort:
    token: str
    chat_id: str

    def __init__(self, token: str, chat_id: int):
        self._token = token
        self._chat_id = chat_id

    def notify(self, message: str, parse_mode: typing.Optional[str] = None) -> None:
        url = f'https://api.telegram.org/bot{self._token}/sendMessage'
        params = dict(chat_id=self._chat_id, text=message[:4000], parse_mode=parse_mode)
        requests.get(url=url, params=params)

    def notify_markdown(self, message: str) -> None:
        self.notify(message=f'```\n{message}```', parse_mode='Markdown')
