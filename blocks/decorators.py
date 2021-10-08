from typing import Any, Type, Callable, Optional, Awaitable

from blocks.types import Event, Source, Processor, AsyncSource, EventOrEvents, AsyncProcessor

SourceFunction = Callable[[], EventOrEvents]
SourceAwaitable = Callable[[], Awaitable[EventOrEvents]]
ProcessorFunction = Callable[[Any], Optional[EventOrEvents]]
ProcessorAwaitable = Callable[[Any], Awaitable[Optional[EventOrEvents]]]


def processor(function: ProcessorFunction) -> Type[Processor]:

    def _call(self: Processor, event: Event) -> Optional[EventOrEvents]:
        return function(event)

    T = type(f'{function.__name__}', (Processor, ), {'__call__': _call})
    T.__call__.__annotations__ = function.__annotations__
    return T


def async_processor(function: ProcessorAwaitable) -> Type[AsyncProcessor]:

    async def _call(self: AsyncProcessor, event: Event) -> Optional[EventOrEvents]:
        return await function(event)

    T = type(f'{function.__name__}', (AsyncProcessor, ), {'__call__': _call})
    T.__call__.__annotations__ = function.__annotations__
    return T


def source(function: SourceFunction) -> Type[Source]:

    def _call(self: Source) -> EventOrEvents:
        return function()

    T = type(f'{function.__name__}', (Source, ), {'__call__': _call})
    T.__call__.__annotations__ = function.__annotations__
    return T


def async_source(function: SourceAwaitable) -> Type[AsyncSource]:

    async def _call(self: AsyncSource) -> EventOrEvents:
        return await function()

    T = type(f'{function.__name__}', (AsyncSource, ), {'__call__': _call})
    T.__call__.__annotations__ = function.__annotations__
    return T
