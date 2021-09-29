from typing import Type, Optional, Sequence

from blocks.graph import Graph
from blocks.types import Block, Event
from blocks.runners import Runner, AsyncRunner
from blocks.validation import validate_blocks


class App:
    def __init__(
        self,
        blocks: Sequence[Block],
        terminal_event: Optional[Type[Event]] = None,
    ) -> None:
        validate_blocks(blocks)
        self._graph = Graph(blocks)
        self._terminal_event = terminal_event

    def run(self, *, interval: float = 1, once: bool = False) -> None:
        Runner(self._graph, self._terminal_event).run(interval=interval, once=once)

    async def run_async(self, *, interval: float = 1, once: bool = False) -> None:
        await AsyncRunner(self._graph, self._terminal_event).run(interval=interval, once=once)
