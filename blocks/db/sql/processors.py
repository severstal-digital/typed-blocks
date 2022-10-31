# ToDo (tribunski.kir): change it to the protocol
from typing import List, Union, Callable
from sqlite3 import Connection

from blocks import Processor
from blocks.logger import logger
from blocks.db.types import Row, Table
from blocks.db.next.sql import Query


def _exec_queries(conn: Connection, query: Query, table: Table) -> None:
    query_formatted = str(query)
    insert_tuples = [query.parametrize(row) for row in table.rows]
    total_count = len(insert_tuples)

    cursor = conn.cursor()

    for ix, tpl in enumerate(insert_tuples, 1):
        logger.info('[{0}/{1}] Executing query: {2} {3}'.format(ix, total_count, query_formatted, tpl))
        cursor.execute(query_formatted, tpl)

    conn.commit()
    conn.close()


class SQLWriter(Processor):

    def __init__(self, queries: List[Query], connection_factory: Callable[[], Connection]) -> None:
        self._connection_factory = connection_factory
        self._queries = {query._codec: query for query in queries}
        self._closed = False
        self.__call__.__annotations__.update(
            {
                'event': Union[tuple(query._codec for query in queries)],
            },
        )

    def __call__(self, event: Union[Row, Table]) -> None:
        conn = self._connection_factory()
        self._run_query(conn, event)

    def _run_query(self, conn: Connection, event: Union[Row, Table]) -> None:
        query = self._queries[type(event)]
        if isinstance(event, Row):
            table = Table(rows=[event])
        elif isinstance(event, Table):
            table = event
        else:
            raise ValueError('Wrong event type {0}'.format(event))
        _exec_queries(conn, query, table)

    def close(self) -> None:
        self._closed = True
