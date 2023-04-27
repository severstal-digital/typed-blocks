from dataclasses import dataclass

from blocks.types import Processor, Event


@dataclass
class EventTime:
    event: Event
    start: float
    end: float
    interval: float


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
        return f'[{self.processor} - {self.type_event}] ' \
               f'Processed {self.count_events} events for {self.interval}. ' \
               f'MIN: {self.min_processing}, MAX: {self.max_processing}, AVG: {self.avg_processing}'

