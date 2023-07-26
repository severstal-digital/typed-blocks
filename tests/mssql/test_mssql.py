from unittest import mock
from unittest.mock import Mock

import pytest
from dataclasses import dataclass, asdict

try:
    import pymssql
except ImportError:
    pytest.skip("skipping mssql-only tests", allow_module_level=True)

from blocks.mssql import MssqlReader, Query, Row, MssqlWriter


@dataclass
class MyRow(Row):
    id: int
    name: str
    quantity: int


def test_mssql_reader() -> None:
    description = (
        ('id', 3, None, None, None, None, None),
        ('name', 1, None, None, None, None, None),
        ('quantity', 3, None, None, None, None, None)
    )
    result_data = [(1, 'banana', 150), (2, 'orange', 154)]
    query = 'select * from mytable'

    with mock.patch('pymssql.connect') as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()

        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = result_data
        mock_cursor.__iter__ = Mock(return_value=iter(result_data))
        mock_cursor.description = description

        queries = [Query(query, MyRow)]
        block = MssqlReader(queries, pymssql.connect)
        events = block()

        assert mock_cursor.execute.call_args[0][0] == query
        assert len(events) == 2
        for event, tp in zip(events, result_data):
            assert tuple(asdict(event).values()) == tp


def test_mssql_writer():
    with mock.patch('pymssql.connect') as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()

        mock_connect.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        query = 'insert into inventory ({}) values ({})'

        queries = [Query(query, MyRow)]
        block = MssqlWriter(queries, pymssql.connect)
        new_row = MyRow(id=3, name="apple", quantity=100)
        block(new_row)

        assert mock_cursor.execute.call_args[0][0] == 'insert into inventory (id, name, quantity) values (%s, %s, %s)'
