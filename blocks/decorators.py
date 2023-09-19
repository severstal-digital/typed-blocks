"""Handful decorators to create *Sources* and *Processors* quickly."""

from typing import Any, Type, Callable, Optional, Awaitable

from blocks.types import (
    Event,
    Source,
    Processor,
    AsyncSource,
    EventOrEvents,
    ParallelEvent,
    AsyncProcessor,
    TypeOfProcessor,
)

# ToDo (tribunsky.kir): redo annotations to avoid "error: Untyped decorator makes function <...>" untyped
# ToDo (tribunsky.kir): remove closures and explicit code duplication

SourceFunction = Callable[[], EventOrEvents]
SourceAwaitable = Callable[[], Awaitable[EventOrEvents]]
ProcessorFunction = Callable[[Any], Optional[EventOrEvents]]
ProcessorAwaitable = Callable[[Any], Awaitable[Optional[EventOrEvents]]]


def source(function: SourceFunction) -> Type[Source]:
    """
    Make a Source from the decorated function.

    Example::

      >>> from dataclasses import dataclass

      >>> from blocks import source


      >>> @dataclass
      ... class E:
      ...     x: int


      >>> @source
      >>> def generator() -> E:
      ...     return E(1)


      >>> blocks = (generator(), ...)

    :param function:    Given function with no arguments.
                        Function should return event or sequence of events to have an effect.
    :return:            Factory which creates a Source.
    """

    def _call(self: Source) -> EventOrEvents:
        return function()

    T = type(
        f'{function.__name__}',
        (Source, ),
        {
            '__call__': _call,
            '__module__': function.__module__
        }
    )
    T.__call__.__annotations__ = function.__annotations__
    return T


def processor(function: ProcessorFunction) -> Type[Processor]:
    """
    Make a Processor from the decorated function.

    Example::

      >>> from blocks import Event

      >>> from blocks import processor

      >>> class MyEvent(Event):
      ...     ...

      >>> @processor
      ... def printer(event: MyEvent) -> None:
      ...     print(event)

      >>> blocks = (printer(), ...)

    :param function:    Given function with a single argument (event).
                        Function may return None, event or sequence of events.
    :return:            Factory which creates a Processor.
    """

    def _call(self: Processor, event: Event) -> Optional[EventOrEvents]:
        return function(event)

    T = type(
        f'{function.__name__}',
        (Processor, ),
        {
            '__call__': _call,
            'type_of_processor': TypeOfProcessor.SYNC,
            # When the processor/source decorator is used,
            # an object based on Processor/Source is created
            # and the namespace of the underlying processor is used for it,
            # so you can get a NameError.
            '__module__': function.__module__,
        }
    )
    T.__call__.__annotations__ = function.__annotations__
    return T


def parallel_processor(function: ProcessorFunction) -> Type[Processor]:
    """
    Make a Parallel event from the decorated function.

    Example::
      >>> from dataclasses import dataclass

      >>> from blocks import parallel_processor

      >>> @dataclass
      >>> class MyEvent:
      ...     ...

      >>> @parallel_processor
      ... def printer(event: MyEvent) -> None:
      ...     print(event)

      >>> blocks = (printer(), ...)

    :param function:            Given function with a single argument (event).
                                Function may return None, event or sequence of events.

    param timeout:              Param for join process.
    param daemon:               Allows us daemon processes.
    param force_terminating:    Allows force termiante process if this didn`t end by timeout
    :return:                    Factory which creates a Processor.
    """
    def _call(self: Processor, event: Event) -> Optional[EventOrEvents]:
        return ParallelEvent(function=function, trigger=event)

    T = type(
        f'{function.__name__}',
        (Processor, ),
        {
            '__call__': _call,
            'type_of_processor': TypeOfProcessor.PARALLEL,
            '__module__': function.__module__
        }
    )
    T.__call__.__annotations__ = function.__annotations__
    return T


def async_source(function: SourceAwaitable) -> Type[AsyncSource]:
    """
    Make an awaitable Source from the decorated awaitable function.

    :param function:    Given function with no arguments.
                        Function should return event or sequence of events to have an effect.
    :return:            Factory which creates an awaitable Source.
    """

    async def _call(self: AsyncSource) -> EventOrEvents:
        return await function()

    T = type(f'{function.__name__}', (AsyncSource, ), {'__call__': _call, '__module__': function.__module__})
    T.__call__.__annotations__ = function.__annotations__
    return T


def async_processor(function: ProcessorAwaitable) -> Type[AsyncProcessor]:
    """
    Make an awaitable Processor from the decorated awaitable function.

    :param function:    Given function with a single argument (event).
                        Function may return None, event or sequence of events.
    :return:            Factory which creates an awaitable Processor.
    """

    async def _call(self: AsyncProcessor, event: Event) -> Optional[EventOrEvents]:
        return await function(event)

    T = type(f'{function.__name__}', (AsyncProcessor, ), {'__call__': _call, '__module__': function.__module__})
    T.__call__.__annotations__ = function.__annotations__
    return T
