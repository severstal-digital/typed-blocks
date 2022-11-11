import logging
from typing import Any, List, NamedTuple
from dataclasses import dataclass

import pytest

from blocks.types import Event, Processor
from blocks.decorators import source, processor
from blocks.validation import validate_blocks, validate_annotations
from blocks.annotations import AnnotationError


class ListHandler(logging.Handler):
    def __init__(self, *args: Any, message_list: List[str], **kwargs: Any) -> None:
        logging.Handler.__init__(self, *args, **kwargs)
        self.message_list = message_list

    def emit(self, record: logging.LogRecord) -> None:
        self.message_list.append(self.format(record).rstrip('\n'))


class E1(Event):
    def __init__(self, x: int) -> None:
        self.x = x


class E2(NamedTuple):
    x: int


@dataclass
class E3:
    x: int


@processor
def p1(e: E1) -> E2:
    return E2(x=1)


@processor
def p2(e: E1):
    return E2(x=1)


@processor
def p3(e):
    return E2(x=1)


class P4(Processor):
    def __call__(self, event: E1):
        return E2(x=1)


class P5(Processor):
    def __call__(self, e1: E1, e2: E2) -> E3:
        return E3(x=1)


class P6(Processor):
    def __call__(self, event: E1) -> E2:
        return E2(x=2)


def test_valid_annotations() -> None:
    validate_annotations(p1())
    validate_annotations(P6())


def test_invalid_annotations() -> None:
    with pytest.raises(AnnotationError):
        validate_annotations(p2())
    with pytest.raises(AnnotationError):
        validate_annotations(p3())
    with pytest.raises(AnnotationError):
        validate_annotations(P4())
    with pytest.raises(AnnotationError):
        validate_annotations(P5())


@source
def g1() -> E1:
    return E1(x=1)


@processor
def d1(e: E2) -> None:
    ...


def test_invalid_blocks() -> None:
    logger = logging.getLogger('blocks')
    messages = []
    logger.addHandler(ListHandler(message_list=messages))

    validate_blocks([p1()])
    assert len(messages) == 3
    assert 'E2' in messages[0] and "isn't processed" in messages[0]
    assert 'p1' in messages[1] and 'There is no events generated for' in messages[1]
    assert 'p1' in messages[2] and 'There is no receivers for generated events' in messages[2]
    messages.clear()

    validate_blocks([g1(), p1()])
    assert len(messages) == 2
    assert 'E2' in messages[0] and "isn't processed" in messages[0]
    assert 'p1' in messages[1] and 'There is no receivers for generated events' in messages[1]


def test_valid_blocks() -> None:
    logger = logging.getLogger('blocks')
    messages = []
    logger.addHandler(ListHandler(message_list=messages))

    validate_blocks([g1(), p1(), d1()])
    assert not messages
