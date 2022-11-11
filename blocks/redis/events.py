from typing import Any, Dict
from dataclasses import asdict, is_dataclass

from blocks.types import Event
from blocks.compat import HAS_PYDANTIC

if HAS_PYDANTIC:
    from pydantic import BaseModel


def event2dict(event: Any) -> Dict[str, Any]:
    if HAS_PYDANTIC is True:
        if isinstance(event, BaseModel):
            return event.dict()
    else:
        method = getattr(event, 'dict')
        if callable(method):
            return event.dict()
    if is_dataclass(event):
        return asdict(event)
    if isinstance(event, Event):
        return {attr: getattr(event, attr) for attr in dir(event) if not attr.startswith('__')}
    raise RuntimeError('Unable to treat event as dict: {0} (type: {1})'.format(event, type(event)))
