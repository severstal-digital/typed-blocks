from dataclasses import dataclass

from blocks.types import Processor, Event


@dataclass
class MetricEvent:
    event: Event
    start: float
    end: float
    size_object: float = 0

    @property
    def interval(self) -> float:
        return self.end - self.start


@dataclass
class AggregatedMetric:
    """
    Dataclass metric

    The size attributes are essentially
    just the amount of memory occupied
    by the processor already after __call__ is called.

    :param processor: Processor that processed the events
    :param interval: Interval for which the metric was collected
    :param type_event: Type of event that was processed by this processor
    :param count_events: Number of events processed per `interval`
    :param max_processing: Maximum time spent on processing 1 event
    :param min_processing: Minimum time spent on processing 1 event
    :param avg_processing: Average time spent on processing 1 event
    :param max_size: Maximum number of MegaBytes occupied by the processor
    :param min_size: Minimum number of MegaBytes occupied by the processor
    :param avg_size: Average number of MegaBytes occupied by the processor
    """
    processor: Processor
    interval: float
    type_event: Event
    count_events: int
    max_processing: float
    min_processing: float
    avg_processing: float
    max_size: float
    min_size: float
    avg_size: float

    def __str__(self) -> str:
        return '[{0} - {1}] Processed {2} events for {3}.({4}, {5}, {6}) ({7}, {8}, {9})'.format(
            self.processor,
            self.type_event,
            self.count_events,
            self.interval,
            self.min_processing,
            self.max_processing,
            self.avg_processing,
            self.min_size,
            self.max_size,
            self.avg_size
        )
