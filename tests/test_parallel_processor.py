import os
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pytest

from blocks import App, Event, parallel_processor, Processor, source

from blocks.types.multiprocess import dill

if dill is None:
    pytest.skip("skipping dill-only tests", allow_module_level=True)


@dataclass
class InputEvent(Event):
    value: int


@dataclass
class OutPutEvent(Event):
    value: int
    pid: int
    process: int


@dataclass
class TerminalEvent(Event):
    ...


@source
def generator() -> InputEvent:
    return InputEvent(42)


@parallel_processor
def parallel_process_1(e: InputEvent) -> OutPutEvent:
    time.sleep(3)
    return OutPutEvent(value=e.value * 2, pid=os.getpid(), process=1)


@parallel_processor
def parallel_process_2(e: InputEvent) -> OutPutEvent:
    time.sleep(3)
    return OutPutEvent(value=e.value * 4, pid=os.getpid(), process=2)


@parallel_processor
def parallel_process_3(e: InputEvent) -> OutPutEvent:
    time.sleep(3)
    return OutPutEvent(value=e.value * 6, pid=os.getpid(), process=3)


class PostProcess(Processor):
    def __init__(self) -> None:
        self.out_events: List[OutPutEvent] = []

    def __call__(self, e: OutPutEvent) -> Optional[TerminalEvent]:
        if len(self.out_events) >= 3:
            return TerminalEvent()
        self.out_events.append(e)


@pytest.mark.parametrize("blocks, count", (
        ((generator(), parallel_process_1(), PostProcess()), 1),
        ((generator(), parallel_process_1(), parallel_process_2(), PostProcess()), 2),
        ((generator(), parallel_process_1(), parallel_process_2(), parallel_process_3(), PostProcess()), 3)
))
def test_count_of_parallel_processor(blocks: Tuple, count: int) -> None:
    assert App(blocks=blocks)._graph.count_of_parallel_tasks == count


def test_run_parallel_processor() -> None:
    blocks = (generator(), parallel_process_1(), parallel_process_2(), parallel_process_3(), PostProcess())
    App(blocks=blocks, terminal_event=TerminalEvent).run()
    for event in blocks[-1].out_events:
        assert event.pid != os.getpid()
        assert event.value == event.process * 2 * 42
