import os
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

CT_DSN = os.environ["CONTROL_TOWER_DSN"]
AIRFLOW_DSN = os.environ.get("AIRFLOW_DSN")


@contextmanager
def ct_conn():
    with psycopg.connect(CT_DSN, row_factory=dict_row, autocommit=True) as conn:
        yield conn


@contextmanager
def airflow_conn():
    if not AIRFLOW_DSN:
        raise RuntimeError("AIRFLOW_DSN not configured")
    with psycopg.connect(AIRFLOW_DSN, row_factory=dict_row, autocommit=True) as conn:
        yield conn
