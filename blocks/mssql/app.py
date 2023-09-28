from typing import List, Type, Callable, Optional, Sequence

from blocks import App, Event, Runner, Processor, Block
from blocks.db import Query
from blocks.validation import validate_blocks
from blocks.mssql.sources import MssqlReader
from blocks.mssql.protocols import Connection
from blocks.mssql.processors import MssqlWriter


class MssqlApp(App):
    """
    High level API for building applications
    with Mssql tables as inputs and possibly outputs.
    Wraps Graph creation and builds BatchApp as initialization.
    On initialization MssqlApp receives list of read queries,
    list of processors, list of update queries
    and save_graph flag with True as default.

    Example::

      >>> import pymssql
      >>> from blocks import processor
      >>> from blocks.mssql import MssqlApp, Query, Row

      >>> class MyTableRow(Row):
      ...     x: int

      >>> @processor
      ... def printer(e: MyTableRow) -> None:
      ...     print(e)

      >>> def get_connection():
      ...     return pymssql.connect(...)

      >>> read_queries = [Query('select * from some_table', MyTableRow)]
      >>> blocks = [printer()]
      >>> MssqlApp(get_connection, read_queries, blocks).run()
    """

    # ToDo (tribunsky.kir): define uniform API: queries in list which are separated in __init__
    def __init__(
        self,
        # ToDo (tribunsky.kir): leaky abstraction, check on more examples of libraries which implement PEP 249
        connection_factory: Callable[[], Connection],
        read_queries: Optional[List[Query]] = None,
        blocks: Optional[Sequence[Block]] = None,
        update_queries: Optional[List[Query]] = None,
        terminal_event: Optional[Type[Event]] = None,
        collect_metric: bool = False,
        *,
        metric_time_interval: int = 60
    ) -> None:
        super().__init__(
            blocks=[],
            terminal_event=terminal_event,
            collect_metric=collect_metric,
            metric_time_interval=metric_time_interval
        )
        if read_queries:
            self._graph.add_block(MssqlReader(read_queries, connection_factory))

        if blocks is not None:
            for processor in blocks:
                self._graph.add_block(processor)

        if update_queries:
            self._graph.add_block(MssqlWriter(update_queries, connection_factory))

        validate_blocks(self._graph.blocks)

    def run(self, *, min_interval: float = 0.0, once: bool = True) -> None:
        Runner(self._graph, self._terminal_event).run(interval=min_interval, once=once)
