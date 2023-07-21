# ToDo (ep.strogonova): replace it with a more generalized functionality (see blocks.db.sql.processors)
from typing import List, Union, Callable

from blocks import Processor
from blocks.logger import logger
from blocks.db.types import Row, Query, Table
from blocks.mssql.protocols import Connection


# ToDo (tribunsky.kir): not sure if it is good idea to give the user execute any queries
def _exec_queries(conn: Connection, query_text: str, rows: Table) -> None:
    columns = rows.columns
    query_formatted = query_text.format(
        ', '.join(columns),
        ', '.join(['%s' for _ in range(len(columns))])
    )
    with conn.cursor() as cursor:
        insert_tuples = rows.values
        total_count = len(insert_tuples)
        for ix, tpl in enumerate(insert_tuples, 1):
            logger.info('[{0}/{1}] Executing query: {2} {3}'.format(ix, total_count, query_formatted, tpl))
            cursor.execute(query_formatted, tpl)


class MssqlWriter(Processor):
    """
    Class represents event processor that writes messages to Mssql tables
    based on data received from event. On initialization must get list
    of queries in order to perform arbitrary mapping. In most cases
    you don't need to use MssqlReader directly, MssqlApp can make this for you.

    Example::

      >>> from dataclasses import dataclass
      >>>
      >>> from blocks import Graph
      >>> from blocks.mssql import MssqlWriter, Query, Row

      >>> @dataclass
      ... class TableRow(Row):
      ...     x: int

      >>> queries = [Query('insert into table ({}) values ({})', TableRow)]
      >>> blocks = (MssqlWriter(queries), ...)
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
            self._conn.close()
            self._closed = True
