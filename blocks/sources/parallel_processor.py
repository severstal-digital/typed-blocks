from blocks.types import ParallelEvent


def run_parallel_processor(payload: bytes):
    event = ParallelEvent.decode(payload)
    return event.function(event.trigger)
