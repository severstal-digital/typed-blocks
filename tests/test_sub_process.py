import os
from dataclasses import dataclass
from blocks import sub_processor, Event, App, source


@dataclass
class InputEvent(Event):
    value: int
    pid: int
    sub_process_pid: int


@dataclass
class OutEvent(Event):
    ...


@sub_processor
def some_processor(e: InputEvent) -> OutEvent:
    pid = os.getpid()
    assert e.value == 42
    assert e.pid != pid
    assert e.sub_process_pid == pid
    return OutEvent()


def test_sub_process():
    @source
    def generator() -> InputEvent:
        return InputEvent(42, os.getpid(), some_processor._subprocess.pid)

    blocks = (generator(), some_processor)
    App(blocks).run(once=True)
