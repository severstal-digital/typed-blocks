from typing import Any, Dict
from dataclasses import asdict, is_dataclass

from blocks.types import Event

try:
    from pydantic import BaseModel
except ImportError:
    HAS_PYDANTIC = False
else:
    HAS_PYDANTIC = True


def event2dict(event: Event) -> Dict[str, Any]:
    if HAS_PYDANTIC is True:
        if isinstance(event, BaseModel):
            return event.dict()
    else:
        method = getattr(event, 'dict')
        if callable(method):
            return event.dict()  # type: ignore
    if is_dataclass(event):
        return asdict(event)
    if isinstance(event, Event):
        return {attr: getattr(event, attr) for attr in dir(event) if not attr.startswith('__')}
