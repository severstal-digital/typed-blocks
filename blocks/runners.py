"""Runners are actually runtime for statically built graph."""

import asyncio
import functools
import multiprocessing as mp
import time
import traceback
from collections import deque
from typing import List, Type, Deque, Optional, Awaitable, DefaultDict, cast, Tuple

from blocks.graph import Graph
from blocks.logger import logger
from blocks.types import Event, Source, Processor, AsyncSource, EventOrEvents, AsyncProcessor, ParallelEvent

SyncProcessors = DefaultDict[Type[Event], List[Processor]]


class Runner(object):
    """
    Allows to actually run graph.

    - Checks sources for new events,
    - handles internal queue,
    - passes events to a specific processors,
    - check conditions for termination
    - proceeds actual shutdown of the app.
    """

    def __init__(self, graph: Graph, terminal_event: Optional[Type[Event]]) -> None:
        """
        Init runner instance.

        :param graph:           Computational graph to execute.
        :param terminal_event:  Special event which simply stops execution, when processed.
        """
        if graph.contains_async_blocks:
            raise RuntimeError('Blocks graph contains async blocks, must be run by AsyncRunner')

        self._sources = cast(List[Source], graph.sources)
        self._processors = cast(SyncProcessors, graph.processors)
        self._q: Deque[Event] = deque()
        self._alive = True
        self._terminal_event = terminal_event
        self._parallel_events: mp.Queue[Event] = mp.Queue()
        self._parallel_processors: List[Tuple[mp.Process, float, bool]] = []

    def run(self, interval: float, once: bool) -> None:
        """
        Start execution.

        :param interval:        Minimal timeout (seconds) between repetition of the whole computational sequence.
        :param once:            If True, executes the whole computational sequence only once, otherwise won't stop until
                                specific conditions (such as terminal event) will occur.
        """
        self._tick()
        if not once:
            while self._alive:
                start = time.perf_counter()
                self._tick()
                delta = time.perf_counter() - start - interval
                logger.debug('Delta to sleep: {0}'.format(delta))
                if delta < 0:
                    time.sleep(abs(delta))
            logger.warning('Not alive anymore. Closing all resources...')
        self._close_resources()
        if not once:
            logger.warning('Everything is supposed to be closed')

    def stop(self) -> None:
        """Stop the execution."""
        self._alive = False

    def _append_events(self, events: Optional[EventOrEvents]) -> None:
        if events is None:
            return None
        elif isinstance(events, (list, tuple)):
            for event in reversed(events):
                if not self._is_terminal_event(event):
                    self._q.appendleft(event)
        elif not self._is_terminal_event(events):
            self._q.appendleft(events)

    def _is_terminal_event(self, event: Event) -> bool:
        if self._terminal_event is not None and isinstance(event, self._terminal_event):
            self.stop()
            return True
        return False

    def _close_resources(self) -> None:
        self._close_sources()
        self._close_processors()

    def _close_sources(self) -> None:
        for source in self._sources:
            source.close()

    def _close_processors(self) -> None:
        closed = set()
        for processors in self._processors.values():
            for processor in processors:
                identifier = id(processor)
                if identifier not in closed:
                    processor.close()
                    closed.add(identifier)

    def _get_new_events(self) -> None:
        for source in self._sources:
            self._append_events(source())

    def _process_parallel_event(self, parallel_event: ParallelEvent) -> None:
        p = mp.Process(
            target=lambda *args: self._parallel_events.put_nowait(parallel_event.function(*args)),
            args=(parallel_event.trigger, ),
            daemon=parallel_event.daemon,
            name='{0}({1}) {2}'.format(
                parallel_event.function.__name__,
                parallel_event.trigger.__class__.__name__,
                parallel_event.trigger
            )
        )
        p.start()
        self._parallel_processors.append((p, parallel_event.timeout, parallel_event.force_terminating))

    def _process_events(self, input_event: Event) -> None:
        for processor in self._processors[type(input_event)]:
            try:
                output_event = processor(input_event)
                if isinstance(output_event, ParallelEvent):
                    self._process_parallel_event(output_event)
                else:
                    self._append_events(output_event)
            except Exception:
                self.stop()
                logger.error('Execution failed during processing the event: {0}({1}) {2}'.format(
                    processor.__class__.__name__, input_event.__class__.__name__, input_event,
                ))
                logger.error(traceback.format_exc())

        for p, t, force_terminate in self._parallel_processors:
            p.join(timeout=t)
            if p.is_alive() and force_terminate:
                p.terminate()
                logger.warning('Parallel processor was terminated by timeout: {0}'.format(p.name))

        while self._parallel_events.empty() is False:
            event = self._parallel_events.get_nowait()
            self._append_events(event)

    def _tick(self) -> None:
        self._get_new_events()
        while self._q:
            event = self._q.popleft()
            self._process_events(event)


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

    def _append_events(self, events: Optional[EventOrEvents]) -> None:
        if events is None:
            return None
        elif isinstance(events, (list, tuple)):
            for event in events:
                if not self._is_terminal_event(event):
                    self._q.append(event)
        elif not self._is_terminal_event(events):
            self._q.append(events)

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
            for processor in self._processors[type(event)]:
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
