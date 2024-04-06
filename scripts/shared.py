import os
import logging

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
