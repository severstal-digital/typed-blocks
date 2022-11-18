from dataclasses import dataclass
from typing import List, Optional, Tuple, Union
import random
import numpy as np
import time
from blocks import App, Event, Processor, source
from blocks.processors import TimeoutedBatcher, Batcher


@dataclass
class InputEvent(Event):
    camera_no: int
    timestamp: str
    image: np.array

@dataclass
class InputEventProcessed(Event):
    camera_no: int
    timestamp: str
    image: np.array

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
    return InputEvent(random.randint(1,9), "1668671381190", np.ones((2,4), dtype=np.int32))

class PreProcessor(Processor):
    def __init__(self) -> None:
        self.trigger_enabled = False
        self.count = 0

    def __call__(self, e: Union[InputEvent, InternalTriggerEvent]) -> Optional[Union[InputEventProcessed, TriggerEvent]]:
        if self.trigger_enabled:
            if self.count > 5:
                self.trigger_enabled = False
                self.count = 0
                return TriggerEvent()
            else:
                self.count+=1
        if isinstance(e, InputEvent):
            return InputEventProcessed(e.camera_no, e.timestamp, e.image)
        if isinstance(e, InternalTriggerEvent):
            self.trigger_enabled = True
            
class PostProcessor(Processor):
    def __init__(self) -> None:
        self.out_events: List[InputEvent] = []

    def __call__(self, e: ImagesBatch) -> Optional[TerminalEvent]:
        if len(self.out_events) >= 4:
            print('terminate')
            return TerminalEvent()
        self.out_events.append(e)

class PostProcessTimeouted(Processor):
    def __init__(self) -> None:
        self.out_events: List[InputEvent] = []

    def __call__(self, e: ImagesBatch) -> Optional[TerminalEvent]:
        self.out_events.append(e)
        if len(self.out_events) >= 4:
            return TerminalEvent()
        if len(self.out_events) == 2:
            return InternalTriggerEvent()

def test_run_batcher() -> None:
    blocks = (generator(), Batcher(InputEvent, ImagesBatch, 5), PostProcessor())
    App(blocks=blocks, terminal_event=TerminalEvent).run()

def test_run_timeoutedbatcher() -> None:
    blocks = (generator(), PreProcessor(), TimeoutedBatcher(InputEventProcessed, TriggerEvent, ImagesBatch, 10, 0.01), PostProcessTimeouted())
    App(blocks=blocks, terminal_event=TerminalEvent).run()