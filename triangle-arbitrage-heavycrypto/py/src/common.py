import typing
import logging
import traceback
import sys
import os

import requests


logger = logging.getLogger()


def configure_logger() -> None:
    level_env = os.environ.get('LOG_LEVEL', 'WARNING')
    logging.basicConfig(
        level=getattr(logging, level_env, 'WARNING'),
        format='[%(asctime)s][%(levelname)s] %(message)s',
    )


class TelegramPort:
    TEXT_LEN: int = 4000

    def __init__(self, token: str, chat_id: int):
        self._token = token
        self._chat_id = chat_id

    def notify(self, message: str, parse_mode: typing.Optional[str] = None) -> None:
        text = message[:self.TEXT_LEN]
        url = f'https://api.telegram.org/bot{self._token}/sendMessage'
        params = dict(chat_id=self._chat_id, text=text, parse_mode=parse_mode)
        response = requests.get(url=url, params=params)
        if response.status_code == 200:
            logger.info('telegram notification sent')
        else:
            logger.warning(
                'telegram notification failed with status %s, error: %s',
                response.status_code,
                response.text,
            )

    def notify_markdown(self, message: str) -> None:
        text = message[:self.TEXT_LEN-7]
        self.notify(message=f'```\n{text}```', parse_mode='Markdown')

    def notify_error(self, error: Exception) -> None:
        _, _, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace_list = [
            '  File "%s", line: %d, in %s\n    %s'
                % (trace[0], trace[1], trace[2], trace[3])
            for trace in trace_back
        ]
        header = 'Traceback (most recent call last):\n'
        stack_trace = '\n'.join(stack_trace_list)
        footer = f'\n{error.__class__.__name__}: {error}'
        message = header + stack_trace + footer
        self.notify(message=message)
