from dataclasses import dataclass
from typing import Callable, Any, Optional

from blocks.types.base import Event, EventOrEvents


@dataclass
class ParallelEvent(Event):
    function: Callable[[Any], Optional[EventOrEvents]]
    trigger: EventOrEvents
    timeout: float
    daemon: bool
    force_terminating: bool
