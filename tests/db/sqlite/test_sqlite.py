from dataclasses import dataclass

from blocks.sqlite import SQLiteReader, SQLiteWriter
from blocks.db.types import Row
from blocks.db.next.sql import Query, Dialects


@dataclass
class TableRow(Row):
    id: int
    name: str
    text: str


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
