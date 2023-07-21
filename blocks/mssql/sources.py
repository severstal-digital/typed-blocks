# ToDo (ep.strogonova): replace it with a more generalized functionality (see blocks.db.sql.sourses)
from __future__ import annotations

import inspect
from typing import Any, List, Union, Callable

from blocks import Event, Source
from blocks.logger import logger
from blocks.db.types import Row, Query, Table
from blocks.mssql.protocols import Connection


def _get_rows_or_table(conn: Connection, query: Query) -> Union[List[Row], Table]:
    events = []
    if issubclass(query.codec, Row):
        row_codec = query.codec
    elif issubclass(query.codec, Table):
        row_codec = query.codec.type()
    else:
        raise TypeError('Not valid codec type: {0}'.format(type(query.codec)))
    with conn.cursor() as cur:
        logger.info(f'Executing query: {query.text}')
        cur.execute(query.text)
        logger.info('Validating')
        keys = [col[0] for col in cur.description]
        fields = inspect.signature(row_codec).parameters
        fields_match = set(keys) == set(fields)
        if fields_match is False:
            logger.warning("Looks like query doesn't exactly matches model. Please, check.")
        for row in cur:
            row_dict = {key: value for key, value in zip(keys, row)}
            if fields_match is False:
                row_dict = {key: row[key] for key in fields}
            events.append(row_codec(**row_dict))
    if issubclass(query.codec, Row):
        return events
    elif issubclass(query.codec, Table):
        return query.codec(rows=events)
    raise TypeError('Not valid codec type: {0}'.format(type(query.codec)))


class MssqlReader(Source):
    """
    Class represents event source that reads Mssql tables
    and wraps them into given events. On initialization must get list
    of queries in order to perform arbitrary mapping. In most cases
    you don't need to use MssqlReader directly, MssqlApp can make this for you.

    Example::

      >>> from dataclasses import dataclass
      >>>
      >>> from blocks import Graph
      >>> from blocks.mssql import MssqlReader, Query, Row

      >>> @dataclass
      ... class TableRow(Row):
      ...     x: int

      >>> queries = [Query('select * from some_table', TableRow)]
      >>> blocks = (MssqlReader(queries), ...)
    """
    # ToDo (tribunsky.kir): re-do on factory, which is able to create Connection on demand, or context manager.
    def __init__(self, queries: List[Query], conn: Callable[[], Connection]) -> None:
        self._conn_factory = conn
        self._queries = queries
        self.patch_annotations({query.codec for query in queries})

    def __call__(self) -> List[Event]:
        with self._conn_factory() as conn:
            rows_or_tables = self._run_queries(conn)
        return rows_or_tables

    def _run_queries(self, conn: Any) -> List[Event]:
        rows_or_tables: List[Event] = []
        for query in self._queries:
            row_or_table = _get_rows_or_table(conn, query)
            if isinstance(row_or_table, list):
                rows_or_tables.extend(row_or_table)
            else:
                rows_or_tables.append(row_or_table)
        return rows_or_tables
