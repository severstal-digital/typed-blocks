"""Runners are actually runtime for statically built graph."""

import time
import asyncio
import functools
import traceback
from typing import List, Type, Deque, Optional, Awaitable, DefaultDict, cast, Union
from collections import deque
from multiprocessing.pool import Pool

from blocks.graph import Graph
from blocks.types import Event, Source, Processor, AsyncSource, EventOrEvents, ParallelEvent, AsyncProcessor
from blocks.logger import logger
from blocks.types.base import is_named_tuple, AnyProcessors, AnyProcessor
from blocks.metric_collector import MetricCollector, EventTime

SyncProcessors = DefaultDict[Type[Event], List[Processor]]


def run_parallel_processor(payload: bytes) -> Optional[EventOrEvents]:
    event = ParallelEvent.decode(payload)
    return event.function(event.trigger)

def get_processor_for_event(event: Event, processors: AnyProcessors) -> List[AnyProcessor]:
    type_event = type(event)
    if type_event in processors:
        return processors[type_event]
    for inheritance in type_event.__mro__:
        if inheritance in processors:
            processors[type_event] = processors[inheritance]
    return processors[type_event]


class Runner(object):
    """
    Allows to actually run graph.

    - Checks sources for new events,
    - handles internal queue,
    - passes events to a specific processors,
    - check conditions for termination
    - proceeds actual shutdown of the app.
    """

    def __init__(
        self,
        graph: Graph,
        terminal_event: Optional[Type[Event]],
        collect_metric: bool = False,
        *,
        metric_time_interval: int = 60,
    ) -> None:
        """
        Init runner instance.

        :param graph:                   Computational graph to execute.
        :param terminal_event:          Special event which simply stops execution, when processed.
        :param collect_metric:          Flag responsible for collecting metrics. If the flag is True,
                                        the metrics will be collected
        :param metric_time_interval:    Time interval for metric aggregation.
        """
        if graph.contains_async_blocks:
            raise RuntimeError('Blocks graph contains async blocks, must be run by AsyncRunner')

        self._sources = cast(List[Source], graph.sources)
        self._processors = cast(SyncProcessors, graph.processors)
        self._q: Deque[Event] = deque()
        self._alive = True
        self._pool: Optional[Pool] = Pool(graph.count_of_parallel_tasks) if graph.count_of_parallel_tasks > 0 else None
        self._terminal_event = terminal_event
        self._collect_metric = collect_metric

        if self._collect_metric:
            self._run_processor = self._metric_run_proc
            self._mp = MetricCollector(metric_time_interval=metric_time_interval)
        else:
            self._run_processor = self._run_proc

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

    def _append_event(self, event: Event) -> None:
        if not self._is_terminal_event(event):
            self._q.appendleft(event)

    def _append_events(self, events: Optional[EventOrEvents]) -> None:
        if events is None:
            return None
        elif is_named_tuple(events):
            self._append_event(events)
        elif isinstance(events, (list, tuple)):
            for event in reversed(events):
                self._append_event(event)
        else:
            self._append_event(events)

    def _is_terminal_event(self, event: Event) -> bool:
        if self._terminal_event is not None and isinstance(event, self._terminal_event):
            self.stop()
            return True
        return False

    def _close_pool(self) -> None:
        if self._pool is not None:
            self._pool.terminate()

    def _close_resources(self) -> None:
        self._close_pool()
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

    def _handle_pool_exceptions(self, exc: BaseException, parallel_event: ParallelEvent) -> None:
        self.stop()
        logger.error('Execution failed during processing the event: {0}({1}) {2}'.format(
            parallel_event.function.__name__, parallel_event.trigger.__class__.__name__, parallel_event.trigger,
        ))
        logger.error(exc)

    def _metric_run_proc(self, processor: Processor, input_event: Event) -> EventOrEvents:
        with self._mp.timer as timer:
            output_event = processor(input_event)
        self._mp.collect(processor, EventTime(type(input_event), timer.start, timer.end))
        return output_event

    def _run_proc(self, processor: Processor, input_event: Event) -> EventOrEvents:
        return processor(input_event)

    def _process_events(self, input_event: Event) -> None:
        for processor in get_processor_for_event(input_event, self._processors): # type: ignore[arg-type]
            logger.debug('Processor: {0} event: {1}'.format(processor, input_event))
            try:
                output_event = self._run_processor(processor, input_event) # type: ignore[arg-type]
            except:
                self.stop()
                logger.error('Execution failed during processing the event: {0}({1}) {2}'.format(
                    processor.__class__.__name__, input_event.__class__.__name__, input_event,
                ))
                logger.error(traceback.format_exc())
            else:
                if isinstance(output_event, ParallelEvent):
                    parallel_event: ParallelEvent = output_event
                    if self._pool is None:
                        self.stop()
                        raise ValueError('Tried to process parallel event while should not be!')
                    self._pool.apply_async(
                        run_parallel_processor,
                        (parallel_event.encode(),),
                        callback=self._append_events,
                        error_callback=lambda exc: self._handle_pool_exceptions(exc, parallel_event)
                    )
                else:
                    self._append_events(output_event)

    def _tick(self) -> None:
        self._get_new_events()
        while self._q:
            event = self._q.popleft()
            self._process_events(event)
            if self._collect_metric:
                self._append_events(self._mp.get_aggregate_events())


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
