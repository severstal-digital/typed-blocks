from typing import List, Union, Callable, Any

from blocks import Processor
from blocks.logger import logger
from blocks.db.types import Query, Row, Table

from pyhive.hive import Connection


def _exec_queries(conn: Connection, query_text: str, partition_key: Union[str, None], rows: Table) -> None:
    total_count = len(rows.values)
    with conn.cursor() as cursor:
        for ix, row in enumerate(rows.rows, 1):
            row_dict = row.as_dict
            query = query_text
            query_args: List[Any] = []

            if ' partition ' in query_text.lower():
                if partition_key is None:
                    raise KeyError("Please specify partition key for Query object if using PARTITION in query!")

                partition_value = row_dict.get(partition_key)
                if not partition_value:
                    raise KeyError("Missed partition key column '{0}' in row table. Row columns: {1}".format(
                            partition_key, list(row_dict.keys())))

                query = query.format('{0}=%s'.format(partition_key), '{0}', '{1}')
                query_args.append(partition_value)
                del row_dict[partition_key]

            columns = row_dict.keys()
            query = query.format(
                ', '.join([col for col in columns]),
                ', '.join(['%s' for i in range(len(columns))])
            )
            query_args += row_dict.values()

            logger.info('[{0}/{1}] Executing query: {2} {3}'.format(ix, total_count, query, query_args))
            cursor.execute(query, query_args)
            logger.info('[{0}/{1}] Query execution finished'.format(ix, total_count))


class HiveWriter(Processor):
    """
        Class represents event processor that writes messages to HiveWriter tables
        based on data received from event. On initialization must get list
        of queries in order to perform arbitrary mapping.
        Query arguments which will be pasted from instance of Table:
        - {0} partition key and value, takes from table row, need to pass partition key as argument to Query
        - {1} table columns
        - {2} table values
        Example::

          >>> from dataclasses import dataclass
          >>>
          >>> from blocks import Graph
          >>> from blocks.hive import HiveWriter, Query, Row

          >>> @dataclass
          ... class TableRow(Row):
          ...     partition_col: str
          ...     x: int

          >>> partition_key = 'partition_col'
          >>> queries = [Query('INSERT INTO table PARTITION ({0}) ({1}) VALUES ({2})', TableRow, partition_key)]
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
        _exec_queries(conn, query.text, query.partition_key, table)
