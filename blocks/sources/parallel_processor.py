from blocks.types import ParallelEvent
try:
    import dill
except ImportError:
    dill = None


def run_parallel_processor(payload: bytes):
    event = ParallelEvent.decode(payload)
    return event.function(event.trigger)