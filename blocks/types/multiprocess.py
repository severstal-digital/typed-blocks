from typing import Callable, Optional
from dataclasses import dataclass

from blocks.types.base import Event, EventOrEvents

try:
    import dill
except ImportError:
    dill = None


@dataclass
class ParallelEvent(Event):
    function: Callable[[Event], Optional[EventOrEvents]]
    trigger: Event

    def encode(self) -> bytes:
        if dill is None:
            raise RuntimeError("For parallel events needed dill library. Please install it")
        return dill.dumps((self.function, self.trigger))

    @classmethod
    def decode(cls, payload: bytes) -> "ParallelEvent":
        if dill is None:
            raise RuntimeError("For parallel events needed dill library. Please install it")
        function, trigger = dill.loads(payload)
        return cls(function=function, trigger=trigger)
