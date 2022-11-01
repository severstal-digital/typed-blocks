from typing import Optional

from blocks.types import EventOrEvents, ParallelEvent


def run_parallel_processor(payload: bytes) -> Optional[EventOrEvents]:
    event = ParallelEvent.decode(payload)
    return event.function(event.trigger)
