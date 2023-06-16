from typing import List, Union, Callable

from blocks import Processor
from blocks.logger import logger
from blocks.db.types import Query, Row, Table

from pyhive.hive import Connection


def prepare_query_value(value: Union[int, float, str]) -> str:
    new_value = str(value)
    if isinstance(value, str):
        new_value = "'{0}'".format(new_value)
    return new_value


def _exec_queries(conn: Connection, query_text: str, rows: Table) -> None:
    total_count = len(rows.values)
    with conn.cursor() as cursor:
        for ix, row in enumerate(rows.rows, 1):
            columns = row.columns
            values = [prepare_query_value(value) for value in row.values]
            query_args: List[str] = []

            if 'partition' in query_text.lower():
                query_args.append(columns[0])
                columns = columns[1:]
                query_args.append(values[0])
                values = values[1:]

            query_args.append(', '.join(columns))
            query_args.append(', '.join(values))
            query = query_text.format(*query_args)

            logger.info('[{0}/{1}] Executing query: {2}'.format(ix, total_count, query))
            cursor.execute(query)
            logger.info('[{0}/{1}] Query execution finished'.format(ix, total_count))


class HiveWriter(Processor):
    """
        Class represents event processor that writes messages to HiveWriter tables
        based on data received from event. On initialization must get list
        of queries in order to perform arbitrary mapping.
        Query arguments which will be pasted from instance of Table:
        - {0} partition key, takes from first column
        - {1} partition value, takes from firs value
        - {2} table columns
        - {3} table values
        Example::

          >>> from dataclasses import dataclass
          >>>
          >>> from blocks import Graph
          >>> from blocks.hive import HiveWriter, Query, Row

          >>> @dataclass
          ... class TableRow(Row):
          ...     x: int

          >>> queries = [Query('INSERT INTO table PARTITION ({0}={1}) ({2}) values ({3})', TableRow)]
          >>> blocks = (HiveWriter(queries), ...)
        """
    def __init__(self, queries: List[Query], connection_factory: Callable[[], Connection]) -> None:
        self._connection_factory = connection_factory
        self._conn = self._connection_factory()
        self._queries = {query.codec: query for query in queries}
        self.__call__.__annotations__.update(
            {
                'event': Union[tuple(query.codec for query in queries)],
            },
        )

    def __call__(self, event: Union[Row, Table]) -> None:
        with self._conn:
            self._run_queries(self._conn, event)

    def _run_queries(self, conn: Connection, event: Union[Row, Table]) -> None:
        query = self._queries[type(event)]
        if isinstance(event, Row):
            table = Table(rows=[event])
        elif isinstance(event, Table):
            table = event
        else:
            raise ValueError('Wrong event type {0}'.format(event))
        _exec_queries(conn, query.text, table)
