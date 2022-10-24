"""Handful decorators to create *Sources* and *Processors* quickly."""
import multiprocessing as mp
import queue
from typing import List, Optional, Type

from blocks.sources.subprocess import SubProcess
from blocks.types import AsyncProcessor, AsyncSource, Event, EventOrEvents, ParallelEvent, Processor, \
    ProcessorAwaitable, ProcessorFunction, Source, SourceAwaitable, SourceFunction


# ToDo (tribunsky.kir): redo annotations to avoid "error: Untyped decorator makes function <...>" untyped
# ToDo (tribunsky.kir): remove closures and explicit code duplication


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

    T = type(f'{function.__name__}', (Source, ), {'__call__': _call})
    T.__call__.__annotations__ = function.__annotations__
    return T


def processor(function: ProcessorFunction) -> Type[Processor]:
    """
    Make a Processor from the decorated function.

    Example::

      >>> from typing import Event

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

    T = type(f'{function.__name__}', (Processor,), {'__call__': _call})
    T.__call__.__annotations__ = function.__annotations__
    return T


class sub_processor(Processor):
    """Create Processor from given processor function,
    that executes the function in separate system process.
    Function must have single annotated argument,
    which inherits Event. Also function must return None,
    single event or multiple events.
    Example::

      >>> from blocks import sub_processor


      >>> class MyEvent(Event):
      ...     pass

      >>> @sub_processor
      ... def printer(event: MyEvent) -> None:
      ...     print(event)


      >>> processors = [
      ...     printer,
      ...     ...
      ...]

    """

    def __init__(self, function: ProcessorFunction) -> None:
        self._input_queue: mp.Queue[Event] = mp.Queue()
        self._output_queue: mp.Queue[Event] = mp.Queue()
        self._subprocess = SubProcess(self._input_queue, self._output_queue, function)
        self._subprocess.start()
        self.__call__.__annotations__['return'] = function.__annotations__['return']
        input_event_type = next(iter(function.__annotations__.values()))
        self.__call__.__annotations__['event'] = input_event_type

    def _get_outputs(self) -> List[Event]:
        output_events = []
        while self._output_queue.qsize() > 0:
            try:
                while True:
                    event = self._output_queue.get(block=False)
                    output_events.append(event)
            except queue.Empty:
                continue
        return output_events

    def __call__(self, event: Event) -> List[Event]:
        self._input_queue.put(event)
        return self._get_outputs()


def parallel_processor(function: ProcessorFunction,
                       *,
                       timeout: float = 5.0,
                       daemon: bool = True,
                       force_terminating: bool = True
                       ) -> Type[Processor]:
    """
    Make a Parallel event from the decorated function.

    Example::
      >>> from dataclasses import dataclass

      >>> from blocks import processor

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
        return ParallelEvent(
            function=function,
            trigger=event,
            timeout=timeout,
            daemon=daemon,
            force_terminating=force_terminating
        )

    T = type(f'{function.__name__}', (Processor, ), {'__call__': _call})
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

    T = type(f'{function.__name__}', (AsyncSource, ), {'__call__': _call})
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

    T = type(f'{function.__name__}', (AsyncProcessor, ), {'__call__': _call})
    T.__call__.__annotations__ = function.__annotations__
    return T
