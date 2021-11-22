from typing import Any, Dict, List, Union

from blocks import Processor
from blocks.logger import logger
from blocks.db.types import Row, Query, Table
from blocks.postgres.protocols import Connection


def _exec_query(conn: Connection, query_text: str, q_params: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        logger.info(f'Executing query: {query_text}')
        cur.execute(query_text, q_params)


def _exec_queries(conn: Connection, query_text: str, params_list: List[Dict[str, Any]]) -> None:
    with conn.cursor() as cur:
        logger.info(f'Executing query: {query_text}')
        cur.executemany(query_text, params_list)


class PostgresWriter(Processor):
    """
    Class represents event processor that writes messages to Postgres tables
    based on data received from event. On initialization must get list
    of queries in order to perform arbitrary mapping. In most cases
    you don't need to use PostgresReader directly, PostgresApp can make this for you.

    Example::

      >>> from blocks import Graph
      >>> from blocks.postgres import PostgresWriter, Query, Row
      >>> class TableRow(Row):
      ...     x: int
      >>> queries = [Query('insert into some_table values (%s)', TableRow)]
      >>> graph = Graph()
      >>> graph.add_block(PostgresWriter(queries))
    """

    def __init__(self, queries: List[Query], conn: Connection) -> None:
        self._conn = conn
        self._queries = {query.codec: query for query in queries}
        self._closed = False
        self.__call__.__annotations__.update(
            {
                'event': Union[tuple(query.codec for query in queries)],
            },
        )

    def __call__(self, event: Union[Row, Table]) -> None:
        with self._conn:
            self._run_query(self._conn, event)

    def _run_query(self, conn: Connection, event: Union[Row, Table]) -> None:
        query = self._queries[type(event)]
        if isinstance(event, Row):
            _exec_query(conn, query.text, dict(event))
        elif isinstance(event, Table):
            _exec_queries(conn, query.text, [dict(row) for row in event.rows])

    def close(self) -> None:
        if not self._closed:
            self._conn.close()
            self._closed = True
