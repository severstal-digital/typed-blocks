from __future__ import annotations

import inspect
from typing import Any, List, Union, Callable

from blocks import Event, Source
from blocks.logger import logger
from blocks.db.types import Row, Query, Table

from pyhive.hive import Connection


def _get_rows_or_table(conn: Connection, query: Query, table_name: str) -> Union[List[Row], Table]:
    events = []
    if not (issubclass(query.codec, Row) or issubclass(query.codec, Table)):
        raise TypeError('Not valid codec type: {0}'.format(type(query.codec)))
    row_codec = query.codec if issubclass(query.codec, Row) else query.codec.type()
    with conn.cursor() as cur:
        logger.info(f'Executing query: {query.text}')
        cur.execute(query.text)
        logger.info('Validating')

        # Each column in description returns Tuple where first value is column name.
        keys = [col[0] for col in cur.description]
        # Column name contains table name - remove it for validation
        if keys[0].startswith('{0}.'.format(table_name)):
            keys = ['.'.join(col.split('.')[1:]) for col in keys]
        fields = inspect.signature(row_codec).parameters
        fields_match = set(keys) == set(fields)
        if fields_match is False:
            logger.warning("Looks like query doesn't exactly matches model. Please, check.")
        for row in cur:
            row_dict = {key: value for key, value in zip(keys, row)}
            if fields_match is False:
                row_dict = {key: row_dict[key] for key in fields}
            events.append(row_codec(**row_dict))
    if issubclass(query.codec, Row):
        return events
    elif issubclass(query.codec, Table):
        return query.codec(rows=events)
    raise TypeError('Not valid codec type: {0}'.format(type(query.codec)))


class HiveReader(Source):
    def __init__(self, queries: List[Query], conn: Callable[[], Connection], table_name: str) -> None:
        self._conn_factory = conn
        self._table_name = table_name
        self._queries = queries
        self.patch_annotations({query.codec for query in queries})

    def __call__(self) -> List[Event]:
        conn = self._conn_factory()
        with conn:
            rows_or_tables = self._run_queries(conn)
        return rows_or_tables

    def _run_queries(self, conn: Any) -> List[Event]:
        rows_or_tables: List[Event] = []
        for query in self._queries:
            row_or_table = _get_rows_or_table(conn, query, self._table_name)
            if isinstance(row_or_table, list):
                rows_or_tables.extend(row_or_table)
            else:
                rows_or_tables.append(row_or_table)
        return rows_or_tables
