"""App with given blocks - is all that you need to start you application in most cases."""

from typing import Type, Optional, Sequence

from blocks.graph import Graph
from blocks.types import Block, Event
from blocks.runners import Runner, AsyncRunner
from blocks.validation import validate_blocks


class App(object):
    """
    Represents the main entrypoint for predefined sequence of blocks.

    Builds computational graph and provides interface to actually start execution.

    Example::

      >>> from blocks import App

      >>> blocks = [...]

      >>> App(blocks).run()
    """

    def __init__(
        self,
        blocks: Sequence[Block],
        terminal_event: Optional[Type[Event]] = None,
        collect_metric: bool = False,
        *,
        metric_time_interval: int = 60
    ) -> None:
        """
        Init application instance.

        :param blocks:                  Sources and processors to be included in application graph.
        :param terminal_event:          Special event which simply stops execution, when processed.
        :param collect_metric:          Flag responsible for collecting metrics. If the flag is True,
                                        the metrics will be collected
        :param metric_time_interval:    Time interval for metric aggregation.
        """
        self._mti = metric_time_interval
        self._collect_metric = collect_metric
        validate_blocks(blocks)
        self._graph = Graph(blocks)
        self._terminal_event = terminal_event

    def run(self, *, min_interval: float = 0.0, once: bool = False) -> None:
        """
        Start execution.

        :param min_interval:    Minimal timeout (seconds) between repetition of the whole computational sequence.
        :param once:            If True, executes the whole computational sequence only once, otherwise won't stop until
                                specific conditions (such as terminal event) will occur.
        """

        Runner(
            self._graph, self._terminal_event, collect_metric=self._collect_metric, metric_time_interval=self._mti,
        ).run(interval=min_interval, once=once)

    async def run_async(self, *, min_interval: float = 0.0, once: bool = False) -> None:
        """
        Run awaitable execution.

        :param min_interval:    Minimal timeout (seconds) between repetition of the whole computational sequence.
        :param once:            If True, executes the whole computational sequence only once, otherwise won't stop until
                                specific conditions (such as terminal event) will occur.
        """
        await AsyncRunner(self._graph, self._terminal_event).run(interval=min_interval, once=once)
