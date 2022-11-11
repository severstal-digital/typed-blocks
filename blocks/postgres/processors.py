from typing import List, Tuple, Union, Callable

from psycopg2 import sql

from blocks import Processor
from blocks.logger import logger
from blocks.db.types import Row, Query, Table
from blocks.postgres.protocols import Connection


# ToDo (tribunsky.kir): not sure if it is good idea to give the user execute any queries
def _exec_queries(conn: Connection, query_text: str, rows: Table) -> None:
    columns = rows.columns
    # Looks like issues in types-psycopg2
    query = sql.SQL(query_text).format(                                                                   # type: ignore
        sql.SQL(', ').join([sql.Identifier(col) for col in columns]),                                     # type: ignore
        sql.SQL(', ').join(sql.Placeholder() * len(columns)),                                             # type: ignore
    )
    with conn.cursor() as cursor:
        insert_tuples = rows.values
        # ToDo (tribunsky.kir): optimize; executemany is slow too https://github.com/psycopg/psycopg2/issues/491
        #                       Maybe a nice approach would be: psycopg2.extras.execute_values
        total_count = len(insert_tuples)
        query_formatted = query.as_string(conn)
        for ix, tpl in enumerate(insert_tuples, 1):
            logger.info('[{0}/{1}] Executing query: {2} {3}'.format(ix, total_count, query_formatted, tpl))
            cursor.execute(query, tpl)


class PostgresWriter(Processor):
    """
    Class represents event processor that writes messages to Postgres tables
    based on data received from event. On initialization must get list
    of queries in order to perform arbitrary mapping. In most cases
    you don't need to use PostgresReader directly, PostgresApp can make this for you.

    Example::

      >>> from dataclasses import dataclass
      >>>
      >>> from blocks import Graph
      >>> from blocks.postgres import PostgresWriter, Query, Row

      >>> @dataclass
      ... class TableRow(Row):
      ...     x: int

      >>> queries = [Query('insert into table ({}) values ({})', TableRow)]
      >>> blocks = (PostgresWriter(queries), ...)
    """

    def __init__(self, queries: List[Query], connection_factory: Callable[[], Connection]) -> None:
        self._connection_factory = connection_factory
        self._conn = self._connection_factory()
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
            table = Table(rows=[event])
        elif isinstance(event, Table):
            table = event
        else:
            raise ValueError('Wrong event type {0}'.format(event))
        _exec_queries(conn, query.text, table)

    def close(self) -> None:
        if not self._closed:
            # Looks like an issue in types-psycopg2
            self._conn.close()                                                                            # type: ignore
            self._closed = True
