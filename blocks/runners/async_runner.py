import time
import asyncio
import functools
from typing import List, Type, Deque, Optional, Awaitable
from collections import deque

from blocks.graph import Graph
from blocks.types import Event, AsyncSource, EventOrEvents, AsyncProcessor

from blocks.types.base import is_named_tuple, AnyProcessors, AnyProcessor


def get_processor_for_event(event: Event, processors: AnyProcessors) -> List[AnyProcessor]:
    type_event = type(event)
    if type_event in processors:
        return processors[type_event]
    for inheritance in type_event.__mro__:
        if inheritance in processors:
            processors[type_event] = processors[inheritance]
    return processors[type_event]


class AsyncRunner(object):
    """Same as Runner, but for async/await syntax."""

    def __init__(self, graph: Graph, terminal_event: Optional[Type[Event]]) -> None:
        """
        Init async runner instance.

        :param graph:           Computational graph to execute.
        :param terminal_event:  Special event which simply stops execution, when processed.
        """
        self._sources = graph.sources
        self._processors = graph.processors
        self._q: Deque[Event] = deque()
        self._alive = True
        self._terminal_event = terminal_event

    async def run(self, interval: float, once: bool) -> None:
        """
        Start execution.

        :param interval:        Minimal timeout (seconds) between repetition of the whole computational sequence.
        :param once:            If True, executes the whole computational sequence only once, otherwise won't stop until
                                specific conditions (such as terminal event) will occur.
        """
        await self._tick()
        if not once:
            while self._alive:
                start = time.perf_counter()
                await self._tick()
                delta = time.perf_counter() - start - interval
                if delta < 0:
                    await asyncio.sleep(abs(delta))
        await self._close_resources()

    def stop(self) -> None:
        """Stop the execution."""
        self._alive = False

    def _append_event(self, event: Event) -> None:
        if not self._is_terminal_event(event):
            self._q.append(event)

    def _append_events(self, events: Optional[EventOrEvents]) -> None:
        if events is None:
            return None
        elif is_named_tuple(events):
            self._append_event(events)
        elif isinstance(events, (list, tuple)):
            for event in events:
                self._append_event(event)
        else:
            self._append_event(events)

    def _is_terminal_event(self, event: Event) -> bool:
        if self._terminal_event is not None and isinstance(event, self._terminal_event):
            self.stop()
            return True
        return False

    async def _close_resources(self) -> None:
        await self._close_sources()
        await self._close_processors()

    async def _close_processors(self) -> None:
        closed = set()
        for processors in self._processors.values():
            for processor in processors:
                identifier = id(processor)
                if identifier not in closed:
                    if isinstance(processor, AsyncProcessor):
                        await processor.close()
                    else:
                        processor.close()
                    closed.add(identifier)

    async def _close_sources(self) -> None:
        for source in self._sources:
            if isinstance(source, AsyncSource):
                await source.close()
            else:
                source.close()

    async def _get_new_events(self) -> None:
        tasks = []
        loop = asyncio.get_running_loop()
        for source in self._sources:
            if isinstance(source, AsyncSource):
                task: Awaitable[EventOrEvents] = loop.create_task(source())
            else:
                task = loop.run_in_executor(None, source)
            tasks.append(task)
        for events in await asyncio.gather(*tasks):
            self._append_events(events)

    async def _process_events(self) -> None:
        loop = asyncio.get_running_loop()
        tasks = []
        while self._q:
            event = self._q.popleft()
            for processor in get_processor_for_event(event, self._processors):
                if isinstance(processor, AsyncProcessor):
                    task: Awaitable[EventOrEvents] = loop.create_task(processor(event))
                else:
                    prepared = functools.partial(processor, event)
                    task = loop.run_in_executor(None, prepared)
                tasks.append(task)

        for events in await asyncio.gather(*tasks):
            self._append_events(events)

        if self._q:
            await self._process_events()

    async def _tick(self) -> None:
        await self._get_new_events()
        await self._process_events()
