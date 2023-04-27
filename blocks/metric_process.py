import time
from collections import OrderedDict, defaultdict
from typing import Optional, Type, List, Dict

from blocks.types.base import Processor, Event
from blocks.types.process_metrics import EventTime, AggregatedMetric


class _ProcTimer:
    def __enter__(self) -> "_ProcTimer":
        self.start = time.perf_counter()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type],
        exc_val: Optional[Exception],
        exc_tb: Optional[Exception]
    ) -> None:
        self.end = time.perf_counter()
        self.interval = self.end - self.start


class MetricProcess(object):
    """
    Collecting information about processor operation


    - The number of events passed through the processor in N time (1 minute by default)
    - Maximum, minimum and average operating time of the processor with 1 event in N time (1 minute by default)

    Example::

      >>> from blocks import App, source, processor
      >>> from blocks.types.process_metrics import AggregatedMetric

      >>> @processor
      >>> def printer(e: AggregatedMetric) -> None:
      ...       print(e)

      >>> blocks  = (..., printer())
      >>> App(blocks).run()
    """

    def __init__(self, time_collect: int = 60) -> None:
        """
        Init metric collector instance

        :param time_collect: Time interval for metrics aggregation
        """
        # todo: throw the time_collect outward
        self._tc = time_collect

        self._collected_data: Dict[Processor, Dict[Event, List[EventTime]]] = OrderedDict()
        self._agg_metrics: List[AggregatedMetric] = []

        self.timer = _ProcTimer

    def collect(self, processor_type: Processor, event_time: EventTime) -> None:
        if processor_type not in self._collected_data:
            self._collected_data[processor_type] = defaultdict(list)
        self._collected_data[processor_type][event_time.event].append(event_time)
        self._check_duration()

    def get_events(self) -> List[AggregatedMetric]:
        data = self._agg_metrics.copy()
        self._agg_metrics = []
        return data

    def _check_duration(self) -> None:
        for processor, events in self._collected_data.copy().items():
            for event, times in events.copy().items():
                interval = times[-1].end - times[0].start
                if interval < self._tc:
                    continue
                intervals = [t.interval for t in times]
                self._agg_metrics.append(
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
                del self._collected_data[processor][event]
