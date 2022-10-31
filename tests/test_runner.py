from dataclasses import dataclass
from unittest.mock import MagicMock
from typing import NamedTuple

import pytest
from blocks.runners import Runner, AsyncRunner, is_named_tuple, EventOrEvents, Event


@dataclass
class SimpleEvent(Event):
    value: int = 42


@dataclass
class TerminalEvent(Event):
    ...


class EmptyNamedTuple(NamedTuple):
    ...


class NotEmptyNamedTuple(NamedTuple):
    values: int = 42


events_by_status = (
    (
        None, False
    ),
    (
        SimpleEvent(), False
    ),
    (
        (SimpleEvent(), SimpleEvent()), False
    ),
    (
        [SimpleEvent(), SimpleEvent()], False
    ),
    (
        (EmptyNamedTuple(), EmptyNamedTuple()), False
    ),
    (
        [EmptyNamedTuple(), EmptyNamedTuple()], False
    ),
    (
        (NotEmptyNamedTuple(), NotEmptyNamedTuple()), False
    ),
    (
        [NotEmptyNamedTuple(), NotEmptyNamedTuple()], False
    ),
    (
        (NotEmptyNamedTuple(), EmptyNamedTuple()), False
    ),
    (
        [NotEmptyNamedTuple(), EmptyNamedTuple()], False
    ),
    (
        EmptyNamedTuple(), True
    ),
    (
        NotEmptyNamedTuple(), True
    ),
    (
        TerminalEvent(), False
    )
)

events = [events for events, status in events_by_status]


@pytest.mark.parametrize("events, status", events_by_status)
def test_is_named_tuple(events: EventOrEvents, status: bool):
    assert is_named_tuple(events) == status


@pytest.mark.parametrize("events", events)
def test_runner_append_event(events: EventOrEvents):
    graph = MagicMock()
    graph.contains_async_blocks = False
    graph.count_of_parallel_tasks = 0
    runner = Runner(graph=graph, terminal_event=TerminalEvent)
    runner.stop = MagicMock()
    runner._q = MagicMock()
    runner._append_events(events)
    if isinstance(events, TerminalEvent):
        assert len(runner._q) == 0
        runner._q.appendleft.assert_not_called()
        runner.stop.assert_called()
    elif isinstance(events, (NotEmptyNamedTuple, EmptyNamedTuple)):
        runner._q.appendleft.assert_called_once_with(events)
    elif isinstance(events, (tuple, list)):
        for event in events:
            runner._q.appendleft.assert_any_call(event)
    elif events is None:
        runner._q.appendleft.assert_not_called()
    elif isinstance(events, Event):
        runner._q.appendleft.assert_called_once_with(events)


@pytest.mark.parametrize("events", events)
def test_async_runner_append_event(events: EventOrEvents):
    graph = MagicMock()
    runner = AsyncRunner(graph=graph, terminal_event=TerminalEvent)
    runner.stop = MagicMock()
    runner._q = MagicMock()
    runner._append_events(events)
    if isinstance(events, TerminalEvent):
        assert len(runner._q) == 0
        runner._q.append.assert_not_called()
        runner.stop.assert_called()
    elif isinstance(events, (NotEmptyNamedTuple, EmptyNamedTuple)):
        runner._q.append.assert_called_once_with(events)
    elif isinstance(events, (tuple, list)):
        for event in events:
            runner._q.append.assert_any_call(event)
    elif events is None:
        runner._q.append.assert_not_called()
    elif isinstance(events, Event):
        runner._q.append.assert_called_once_with(events)
