import os
import time
from dataclasses import dataclass

from blocks import App, source, processor, sub_processor


@dataclass
class InputEvent:
    value: int


@dataclass
class OutPutEvent:
    value: int
    pid: int


@source
def generator() -> InputEvent:
    return InputEvent(42)


@sub_processor
def sub_task(e: InputEvent) -> OutPutEvent:
    time.sleep(3)
    return OutPutEvent(value=e.value * 2, pid=os.getpid())


@processor
def printer(e: OutPutEvent) -> None:
    print("Parent pid:", os.getpid())
    print("Child pid:", e.pid)
    print("Sub task result:", e.value)


blocks = (generator(), sub_task, printer())
App(blocks).run()
