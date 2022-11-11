import os
import sqlite3
from typing import Callable, Generator
from sqlite3 import Connection

import pytest


@pytest.fixture
def connect() -> Callable[[], Connection]:
    def connection_factory() -> Connection:
        con = sqlite3.connect('example.db')
        with con:
            con.execute('''CREATE TABLE IF NOT EXISTS test_table (id, name, text)''')
        return con

    return connection_factory


# FixMe (tribunsky.kir): potentially dangerous, use smth like mkstemp
@pytest.fixture(autouse=True)
def run_around_tests() -> Generator[None, None, None]:
    file_name = 'example.db'
    if os.path.exists(file_name):
        raise
    try:
        yield
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)
