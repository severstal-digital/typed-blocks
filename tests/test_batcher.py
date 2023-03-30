import time
import array
import random
from typing import List, Union, Optional
from dataclasses import dataclass

from blocks import App, Event, Processor, source
from blocks.processors import Batcher, TimeoutedBatcher


@dataclass
class InputEvent(Event):
    camera_no: int
    timestamp: str
    image: array.array


@dataclass
class InputEventProcessed(Event):
    camera_no: int
    timestamp: str
    image: array.array


@dataclass
class ImagesBatch(Event):
    data: List[InputEvent]


@dataclass
class TriggerEvent(Event):
    ...


@dataclass
class InternalTriggerEvent(Event):
    ...


@dataclass
class TerminalEvent(Event):
    ...


@source
def generator() -> InputEvent:
    time.sleep(0.1)
    return InputEvent(random.randint(1, 9), "1668671381190", array.array('d', [2.4, 14.8, 7, 6]))


class PreProcessor(Processor):
    def __init__(self) -> None:
        self.trigger_enabled = False
        self.count = 0

    def __call__(
        self,
        e: Union[InputEvent, InternalTriggerEvent],
    ) -> Optional[Union[InputEventProcessed, TriggerEvent]]:
        if self.trigger_enabled:
            if self.count > 5:
                self.trigger_enabled = False
                self.count = 0
                return TriggerEvent()
            else:
                self.count += 1
        if isinstance(e, InputEvent):
            return InputEventProcessed(e.camera_no, e.timestamp, e.image)
        if isinstance(e, InternalTriggerEvent):
            self.trigger_enabled = True
        return None


class PostProcessor(Processor):
    def __init__(self) -> None:
        self.out_events: List[ImagesBatch] = []

    def __call__(self, e: ImagesBatch) -> Optional[TerminalEvent]:
        if len(self.out_events) >= 4:
            print('terminate')
            return TerminalEvent()
        self.out_events.append(e)
        return None


class TimeoutPostProcessor(Processor):
    def __init__(self) -> None:
        self.out_events: List[ImagesBatch] = []

    def __call__(self, e: ImagesBatch) -> Optional[Union[TerminalEvent, InternalTriggerEvent]]:
        self.out_events.append(e)
        if len(self.out_events) >= 4:
            return TerminalEvent()
        if len(self.out_events) == 2:
            return InternalTriggerEvent()
        return None


def test_run_batcher() -> None:
    blocks = (generator(), Batcher(InputEvent, ImagesBatch, 5), PostProcessor())
    App(blocks=blocks, terminal_event=TerminalEvent).run()


def test_run_timeout_batcher() -> None:
    blocks = (
        generator(),
        PreProcessor(),
        TimeoutedBatcher(InputEventProcessed, TriggerEvent, ImagesBatch, 10, 0.01),
        TimeoutPostProcessor(),
    )
    App(blocks=blocks, terminal_event=TerminalEvent).run()
