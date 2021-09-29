from abc import ABC, abstractmethod
from typing import Any, Union, Iterable, Optional

Event = object
EventOrEvents = Union[Event, Iterable[Event]]


class Processor(ABC):
    @abstractmethod
    def __call__(self, event: Any) -> Optional[EventOrEvents]: ...
    def close(self) -> None: ...


class Source(ABC):
    @abstractmethod
    def __call__(self) -> EventOrEvents: ...
    def close(self) -> None: ...


class AsyncSource(ABC):
    @abstractmethod
    async def __call__(self) -> EventOrEvents: ...
    async def close(self) -> None: ...


class AsyncProcessor(ABC):
    @abstractmethod
    async def __call__(self, event: Any) -> Optional[EventOrEvents]: ...
    async def close(self) -> None: ...


AnySource = Union[AsyncSource, Source]
AnyProcessor = Union[AsyncProcessor, Processor]
Block = Union[AnySource, AnyProcessor]
