from typing import List, NamedTuple, Union
from dataclasses import dataclass

import pytest

from blocks.types import Event, Processor
from blocks.decorators import source, processor
from blocks.annotations import get_input_events_type, get_output_events_type


class E1(Event):
    def __init__(self, x: int) -> None:
        self.x = x


class E2(NamedTuple):
    x: int


@dataclass
class E3:
    x: int


@processor
def proc0(event: E1) -> List[Union[E1, E2, E3]]:
    return [E2(x=event.x)]


class ClassProc0(Processor):
    def __call__(self, event: E1) -> "List[Union[E1, E2, E3]]":
        return [E2(x=event.x)]


class ClassProc1(Processor):
    def __call__(self, event: "Union[E1, E2, E3]") -> 'List[E2]':
        return [E2(x=event.x)]


class ClassStringProc0(Processor):
    def __call__(self, event: E1) -> "List[Union[E1, E2, E3]]":
        return [E2(x=event.x)]


class ClassStringProc1(Processor):
    def __call__(self, event: "Union[E1, E2, E3]") -> 'List[E2]':
        return [E2(x=event.x)]


@processor
def proc1(event: E1) -> List[E2]:
    return [E2(x=event.x)]


@processor
def proc2(event: E2) -> E3:
    return E3(x=event.x)


@processor
def proc3(event: Union[E1, E2, E3]) -> E3:
    return E3(x=event.x)


@processor
def printer(event: E3) -> None:
    print(event)


@source
def generator() -> E2:
    return E2(x=1)


@pytest.mark.parametrize(
    'func_processor, func_res',
    [
        (proc3, get_input_events_type),
        (proc0, get_output_events_type),
        (ClassProc1, get_input_events_type),
        (ClassProc0, get_output_events_type),
        (ClassStringProc1, get_input_events_type),
        (ClassStringProc0, get_output_events_type)
    ]
)
def test_annotation(func_processor, func_res) -> None:
    events = func_res(func_processor())

    assert len(events) == 3
    assert events[0] is E1
    assert events[1] is E2
    assert events[2] is E3


@pytest.mark.parametrize(
    'func_processor, func_res, event_res',
    [
        (proc1, get_input_events_type, E1),
        (proc1, get_output_events_type, E2),
    ]
)
def test_annotation_parse(func_processor, func_res, event_res) -> None:
    events = func_res(func_processor())

    assert len(events) == 1
    assert events[0] is event_res


@pytest.mark.parametrize(
    'func_processor, func_res',
    [
        (generator, get_input_events_type),
        (printer, get_output_events_type),
    ]
)
def test_annotations_none(func_processor, func_res) -> None:
    events = func_res(func_processor())

    assert len(events) == 0
