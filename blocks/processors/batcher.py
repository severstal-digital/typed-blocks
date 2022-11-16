import time
from typing import Type, Deque, Union, Optional, List, Any
from collections import deque

from blocks.types import Processor, Event, Source


def _timeout_exceed(timeout: float, ts: float) -> bool:
    return (time.time() - ts) > timeout


class Batcher(Processor):
    """
    Processor that implements batching of events by caching
    them in accumulator until accumulator size reached batch size.
    Then flushes events and clear accumulator. On initialization
    requires event for batching, event for list of events wrapping
    and batch size.

    Example::

      >>> from typing import List
      >>> from blocks import Event
      >>> from blocks.processors import Batcher
      >>> class MyEvent(Event):
      ...     field1: int
      ...     field2: str
      >>> class BatchedEvents(Event):
      ...     events: List[MyEvent]
      >>> batcher = Batcher(MyEvent, BatchedEvents, 2)
    """

    def __init__(
        self,
        input_event: Type[Event],
        batch_event: Type[Event],
        batch_size: int,
    ) -> None:
        self._int_deque: Deque[Event] = deque()
        self._batch_size = batch_size
        self._input_event = input_event
        self._batch_event = batch_event
 

    def __call__(self, event: Type[Event]) -> Optional[Event]:
        self._int_deque.append(event)
        if len(self._int_deque) == self._batch_size:
            return self._get_batch()
        return None

    def _get_batch(self) -> Event:
        items = list(self._int_deque)
        self._int_deque.clear()
        return self._batch_event(items)


class TimeoutedBatcher(Batcher):
    """
    Processor that implements batching of events by caching
    them in accumulator until accumulator size reached batch size.
    Then flushes events and clear accumulator. On initialization
    requires event for batching, event for list of events wrapping,
    batch size and timeout in seconds. Almost identical to Batcher
    with one key difference that TimeoutedBatcher performs early flush
    of accumulator if timeout exceeded. Timeout nullifies when
    batching event received.

    Example::

      >>> from typing import List
      >>> from blocks import Event 
      >>> from blocks.processors import TimeoutedBatcher
      >>> class MyEvent(Event):
      ...     field1: int
      ...     field2: str
      >>> class BatchedEvents(Event):
      ...     events: List[MyEvent]
      >>> batcher = TimeoutedBatcher(MyEvent, BatchedEvents, 2, 1.5)
    """

    def __init__(
        self,
        input_event: Type[Event],
        trigger_event: Type[Event],
        batch_event: Type[Event],
        batch_size: int,
        timeout: float,
    ) -> None:
        self._timeout = timeout
        self._trigger_event = trigger_event
        self._last_assembly_time: Optional[float] = None
        super().__init__(input_event, batch_event, batch_size)

    def __call__(self, event: Type[Event]) -> Optional[Event]:
        if isinstance(event, self._input_event):
            if self._last_assembly_time is None:
                self._last_assembly_time = time.time()
            batch = super().__call__(event)
            if batch:
                self._last_assembly_time = time.time()
                return batch
        elif (
            isinstance(event, self._trigger_event)
            and self._acc
            and self._last_assembly_time is not None
            and _timeout_exceed(self._timeout, self._last_assembly_time)
        ):
            self._last_assembly_time = time.time()
            return self._get_batch()
        return None
