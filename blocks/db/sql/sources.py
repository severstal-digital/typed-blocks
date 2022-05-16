import inspect
from sqlite3 import Connection

from typing import List, Union, Callable, Any

from blocks import Source, Event
from blocks.logger import logger
from blocks.db.types import Row, Table
from blocks.db.next.sql import Query


def _get_rows_or_table(conn: Connection, query: Query) -> Union[List[Row], Table]:
    """Or how to make psycopg SLOW."""
    events = []
    if issubclass(query.codec, Row):
        row_codec = query.codec
    elif issubclass(query.codec, Table):
        # pydantic:
        # row_codec = query.codec.__annotations__['rows'].__args__[0]
        # dataclass:
        row_codec = query.codec.rows.__args__[0]
    else:
        raise TypeError('Not valid codec type: {0}'.format(type(query.codec)))
    # ToDo (tribunsky.kir): sqlite raises `AttributeError: __enter__`
    # with conn.cursor() as cur:
    cur = conn.cursor()
    logger.info(f'Executing query: {query.text}')
    cur.execute(query.text)
    logger.info('Validating')
    # ToDo (tribunsky.kir): sqlite returns only tuple with names
    # keys = [col.name for col in cur.description]
    keys = [col[0] for col in cur.description]
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


class SQLReader(Source):

    def __init__(self, queries: List[Query], conn: Callable[[], Connection]) -> None:
        self._conn_factory = conn
        self._queries = queries
        out_annots = {query.codec for query in queries}
        self.__call__.__annotations__['return'] = List[Union[tuple(out_annots)]]  # type: ignore

    def __call__(self) -> List[Event]:
        conn = self._conn_factory()
        try:
            # with conn:
            rows_or_tables = self._run_queries(conn)
        finally:
            conn.close()
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

