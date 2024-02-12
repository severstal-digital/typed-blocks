"""Runners are actually runtime for statically built graph."""

from queue import Queue
from typing import Type, Optional

from blocks.graph import Graph
from blocks.runners.runner import Runner
from blocks.rest.threads import RestParser
from blocks.types import Event


class RestRunner(Runner):
    """
    Allows to actually run graph.

    - Checks sources for new events,
    - handles internal queue,
    - passes events to a specific processors,
    - check conditions for termination
    - proceeds actual shutdown of the app.
    """

    def __init__(
        self,
        graph: Graph,
        base_host_model: str,
        input_rest_event: Type[Event],
        output_rest_event: Type[Event],
        terminal_event: Optional[Type[Event]],
        collect_metric: bool = False,
        *,
        metric_time_interval: int = 60,
    ) -> None:
        """
        Init runner instance.

        :param graph:                   Computational graph to execute.
        :param terminal_event:          Special event which simply stops execution, when processed.
        :param collect_metric:          Flag responsible for collecting metrics. If the flag is True,
                                        the metrics will be collected
        :param metric_time_interval:    Time interval for metric aggregation.
        """
        # fixme: circular imports :D
        from blocks.rest.processors import RestProcessor
        from blocks.rest.sources import RestQueueSource

        out_q, ids_q, = Queue(), Queue()

        self._threads = [
            RestParser(base_host_model, ids_q, out_q),
        ]
        for thr in self._threads:
            thr.start()
        rest_source = RestProcessor(input_rest_event, base_host_model, ids_q)
        rest_proc = RestQueueSource(output_rest_event, out_q)

        graph.add_block(rest_source)
        graph.add_block(rest_proc)

        super().__init__(graph, terminal_event, collect_metric, metric_time_interval=metric_time_interval)

    def _close_threads(self) -> None:
        for thr in self._threads:
            thr.stop()

    def _close_resources(self) -> None:
        self._close_pool()
        self._close_sources()
        self._close_processors()
        self._close_threads()
