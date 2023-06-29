import time
from typing import List, Optional

import pytest

try:
    import schedule
except ImportError:
    pytest.skip("skipping schedule-only tests", allow_module_level=True)

from blocks import Event
from blocks.sources.schedule import Scheduler


class ScheduledTrigger(Event):
    ...


def init_source(*, skip_when_busy: Optional[bool] = None) -> Scheduler:
    if skip_when_busy is None:
        source = Scheduler(ScheduledTrigger).every(1).seconds
    else:
        source = Scheduler(ScheduledTrigger, skip_when_busy=skip_when_busy).every(1).seconds
    source()
    return source


TEST_CASES = [
    # Default behaviour
    (None, 5.1, 0.01, 5),
    # Long work in main loop
    (None, 4.1, 2.0, 2),
    # Long work in main loop (2)
    (None, 5.1, 5.0, 1),
    # Should behave same as default
    (False, 5.1, 0.01, 5),
    # Should not behave same as long work as we emit late events
    (False, 5.1, 5.0, 5),
]


@pytest.mark.parametrize("skip_when_busy,duration,delay,answer", TEST_CASES)
def test_scheduler(skip_when_busy: bool, duration: float, delay: float, answer: int) -> None:
    source = init_source(skip_when_busy=skip_when_busy)
    events = []
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < duration:
        new_events = source()
        assert isinstance(new_events, list)
        events.extend(new_events)
        time.sleep(delay)
    assert len(events) == answer
