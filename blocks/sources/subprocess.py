import multiprocessing as mp
import queue
from typing import Iterable, NoReturn

from blocks.types import Event, ProcessorFunction


class SubProcess(mp.Process):
    def __init__(
            self,
            input_queue: mp.Queue,
            output_queue: mp.Queue,
            processor: ProcessorFunction,
    ) -> None:
        super().__init__(daemon=True)
        self._input_queue: mp.Queue[Event] = input_queue
        self._output_queue: mp.Queue[Event] = output_queue
        self._processor: ProcessorFunction = processor

    def run(self) -> NoReturn:
        while True:
            try:
                in_event = self._input_queue.get(block=False)
            except queue.Empty:
                continue
            else:
                out_event = self._processor(in_event)
                if isinstance(out_event, Iterable):
                    for event in out_event:
                        self._output_queue.put(event)
                else:
                    self._output_queue.put(out_event)
