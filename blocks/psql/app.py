from typing import Sequence, Optional, Type, List

from sqlalchemy import Engine
from sqlmodel import SQLModel

from blocks import App, Block, Event, Runner
from blocks.psql.processors import PsqlWriter
from blocks.psql.sources import PsqlReader
from blocks.psql.types import Query
from blocks.validation import validate_blocks


def _write_events(blocks: Sequence[Block]) -> List[Type[SQLModel]]:
    type_events = []
    for block in blocks:
        type_event = block.__call__.__annotations__.get('return')
        if type_event and issubclass(type_event, SQLModel):
            type_events.append(type_event)
    return type_events


class PsqlApp(App):
    """
    A high-level API for creating applications
    with Postgres tables as input and possibly output, based on SQLModel classes.
    Wraps graph creation and builds BatchApp as initialization.

    Version 2.0

    Example::
        >>> from sqlalchemy import create_engine
        >>> from sqlmodel import select, SQLModel, Field

        >>> from blocks import processor
        >>> from blocks.psql.app import PsqlApp
        >>> from blocks.psql.types import Query


        >>> class Project(SQLModel, table=True):
        ...     id: int = Field(primary_key=True)
        ...     name: str
        ...     namespace: str


        >>> @processor
        >>> def printer(event: Project) -> None:
        ...     print(event)


        >>> def main() -> None:
        ...     queries = [
        ...         Query(
        ...             text=select(Project),
        ...             codec=Project
        ...         )
        ...     ]
        ...     engine = create_engine(
        ...        "postgresql://..."
        ...     )
        ...
        ...     app = PsqlApp(
        ...         engine=engine,
        ...         queries=queries,
        ...         blocks=[printer()]
        ...     )
        ...     app.run(once=True)

        >>> if __name__ == '__main__':
        ...     main()

    """

    def __init__(
        self,
        engine: Engine,
        queries: Optional[List[Query]] = None,
        blocks: Optional[Sequence[Block]] = None,
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
        if queries:
            self._graph.add_block(PsqlReader(queries, engine))
        if blocks is not None:
            for proc in blocks:
                self._graph.add_block(proc)

        wr_events = _write_events(blocks)
        if wr_events:
            self._graph.add_block(PsqlWriter(wr_events, engine))

        validate_blocks(self._graph.blocks)

    def run(self, *, min_interval: float = 0.0, once: bool = True) -> None:
        Runner(self._graph, self._terminal_event).run(interval=min_interval, once=once)

    # todo: Make asynchronous runner with asynchronous driver possible
    # def async_run(self):
