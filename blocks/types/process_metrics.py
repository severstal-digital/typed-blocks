from dataclasses import dataclass

from blocks.types import Processor, Event


@dataclass
class EventTime:
    event: Event
    start: float
    end: float

    @property
    def interval(self) -> float:
        return self.end - self.start


@dataclass
class AggregatedMetric:
    processor: Processor
    interval: float
    type_event: Event
    count_events: int
    max_processing: float
    min_processing: float
    avg_processing: float

    def __str__(self) -> str:
        return '[{0} - {1}] Processed {2} events for {3}. MIN: {4}, MAX: {5}, AVG: {6}'.format(
            self.processor,
            self.type_event,
            self.count_events,
            self.interval,
            self.min_processing,
            self.max_processing,
            self.avg_processing
        )
