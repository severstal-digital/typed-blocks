from blocks.types import EventOrEvents, ParallelEvent


def run_parallel_processor(payload: bytes) -> EventOrEvents:
    event = ParallelEvent.decode(payload)
    return event.function(event.trigger)
