import os
import time
from dataclasses import dataclass

from blocks import App, Event, parallel_processor, processor, source


@dataclass
class InputEvent(Event):
    value: int


@dataclass
class OutPutEvent(Event):
    value: int
    pid: int
    process: int


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


@processor
def post_process(e: OutPutEvent) -> None:
    print("Process:", e.process)
    print("Parent pid:", os.getpid())
    print("Child pid:", e.pid)
    print("Sub task result:", e.value)


blocks = (generator(), parallel_process_1(), parallel_process_2(), parallel_process_3(), post_process())
App(blocks).run()
