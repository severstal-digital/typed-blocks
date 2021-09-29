from typing import List, Union, NamedTuple
from dataclasses import dataclass

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
def proс0(event: E1) -> List[Union[E1, E2, E3]]:
    return [E2(x=event.x)]


class ClassProc0(Processor):
    def __call__(self, event: E1) -> List[Union[E1, E2, E3]]:
        return [E2(x=event.x)]


class ClassProc1(Processor):
    def __call__(self, event: Union[E1, E2, E3]) -> List[E2]:
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


def test_input_union_annotation_flatten() -> None:
    events = get_input_events_type(proc3())

    assert len(events) == 3
    assert events[0] is E1
    assert events[1] is E2
    assert events[2] is E3


def test_output_union_annotation_flatten() -> None:
    events = get_output_events_type(proс0())

    assert len(events) == 3
    assert events[0] is E1
    assert events[1] is E2
    assert events[2] is E3


def test_input_class_processor_union_annotation_flatten() -> None:
    events = get_input_events_type(ClassProc1())

    assert len(events) == 3
    assert events[0] is E1
    assert events[1] is E2
    assert events[2] is E3


def test_output_class_processor_union_annotation_flatten() -> None:
    events = get_output_events_type(ClassProc0())

    assert len(events) == 3
    assert events[0] is E1
    assert events[1] is E2
    assert events[2] is E3


def test_input_annotations_parse() -> None:
    events = get_input_events_type(proc1())

    assert len(events) == 1
    assert events[0] is E1


def test_output_annotations_parse() -> None:
    events = get_output_events_type(proc1())

    assert len(events) == 1
    assert events[0] is E2


def test_input_annotations_none() -> None:
    events = get_input_events_type(generator())

    assert len(events) == 0


def test_output_annotations_none() -> None:
    events = get_output_events_type(printer())

    assert len(events) == 0
