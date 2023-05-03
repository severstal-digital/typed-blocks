from __future__ import annotations

import time
from collections import OrderedDict, defaultdict
from types import TracebackType
from typing import Optional, Type, List, Dict

from blocks.types.base import Processor, Event
from blocks.types.metrics import EventTime, AggregatedMetric


class _ProcTimer:

    def __enter__(self) -> _ProcTimer:
        self.start = time.perf_counter()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ) -> None:
        self.end = time.perf_counter()


class MetricCollector(object):
    """
    Collecting information about processor operation


    - The number of events passed through the processor in N time (1 minute by default)
    - Maximum, minimum and average operating time of the processor with 1 event in N time (1 minute by default)

    Example::

      >>> from blocks import App, source, processor
      >>> from blocks.types.metrics import AggregatedMetric

      >>> @processor
      >>> def printer(e: AggregatedMetric) -> None:
      ...       print(e)

      >>> blocks  = (..., printer())
      >>> App(blocks, collect_metric=True).run()
    """

    def __init__(self, metric_time_interval: int) -> None:
        """
        Init metric collector instance

        :param metric_time_interval:    Time interval for metric aggregation.
        """

        self._tc = metric_time_interval
        self._collected_data: Dict[Processor, Dict[Event, List[EventTime]]] = OrderedDict()

        self.timer = _ProcTimer()

    def collect(self, processor_type: Processor, event_time: EventTime) -> None:
        if processor_type not in self._collected_data:
            self._collected_data[processor_type] = defaultdict(list)
        self._collected_data[processor_type][event_time.event].append(event_time)

    def get_aggregate_events(self) -> List[AggregatedMetric]:
        agg_metrics: List[AggregatedMetric] = []
        to_delete = []
        for processor, events in self._collected_data.items():
            for event, times in events.items():
                interval = times[-1].end - times[0].start
                if interval < self._tc:
                    continue
                intervals = [t.interval for t in times]
                agg_metrics.append(
                    AggregatedMetric(
                        processor=processor,
                        interval=round(interval, 2),
                        type_event=event,
                        count_events=len(times),
                        max_processing=max(intervals),
                        min_processing=min(intervals),
                        avg_processing=round(sum(intervals) / len(intervals), 2),
                    )
                )
                to_delete.append((processor, event))
        for pr, ev in to_delete:
            del self._collected_data[pr][ev]

        return agg_metrics
