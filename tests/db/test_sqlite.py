import os.path
import sqlite3
from dataclasses import dataclass

import pytest

from blocks.db.types import Row
from blocks.db.next.sql import Query, Dialects
from blocks.sqlite import SQLiteWriter, SQLiteReader


@dataclass
class TableRow(Row):
    id: int
    name: str
    text: str


@pytest.fixture
def connect():
    def connection_factory():
        con = sqlite3.connect('example.db')
        with con:
            con.execute('''CREATE TABLE IF NOT EXISTS test_table (id, name, text)''')
        return con

    return connection_factory


# FixMe (tribunski.kir): potentially dangerous, use smth like mkstemp
@pytest.fixture(autouse=True)
def run_around_tests():
    if os.path.exists('example.db'):
        raise
    yield
    os.remove('example.db')


def test_read_write(connect) -> None:
    read_query = Query(TableRow, dialect=Dialects.sqlite).SELECT('*').FROM('test_table')
    reader = SQLiteReader([read_query], connect)
    events = reader()
    assert events == []

    insert_query = Query(dialect=Dialects.sqlite).INSERT(TableRow).INTO('test_table')
    writer = SQLiteWriter([insert_query], connect)
    row_original = TableRow(id=1, name='one', text='test')
    writer(row_original)

    [event] = reader()
    assert event == row_original

    update_query = Query(dialect=Dialects.sqlite).UPDATE('test_table').SET(TableRow).WHERE('id')
    writer = SQLiteWriter([update_query], connect)
    row_updated = TableRow(id=1, name='one', text='updated')

    writer(row_updated)

    [event] = reader()
    assert event == row_updated
