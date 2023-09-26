from collections import defaultdict
from dataclasses import dataclass
from typing import NamedTuple, DefaultDict, Type, List

from blocks import Event, Processor
from blocks.runners import get_processor_for_event



class E1(Event):
    def __init__(self, x: int) -> None:
        self.x = x

class E2(NamedTuple):
    x: str

@dataclass
class E3:
    x: int

class Printer(Processor):
    def __init__(self, args: int) -> None:
        self.args = args
    def __call__(self, event: E1) -> None:
        print(event)

p=Printer(1)


def test_e1() -> None:

    class EventChild(E1):
        pass
    class EventChildChild(EventChild):
        pass
    class EventChildChildChild(EventChildChild):
        pass

    parent = E1(1)
    child = EventChild(1)
    childchild = EventChildChild(1)
    childchildchild = EventChildChildChild(1)
    dict_processors: DefaultDict[Type[Event], List[Processor]] = defaultdict(list)
    dict_processors[type(parent)].append(p)

    assert get_processor_for_event(parent, dict_processors) == [p]
    assert get_processor_for_event(child, dict_processors) == [p]
    assert get_processor_for_event(childchild, dict_processors) == [p]
    assert get_processor_for_event(childchildchild, dict_processors) == [p]
    print(dict_processors)


def test_e2() -> None:

    class EventChild(E2):
        pass
    class EventChildChild(EventChild):
        pass
    class EventChildChildChild(EventChildChild):
        pass

    parent = E2("1")
    child = EventChild("1")
    childchild = EventChildChild("1")
    childchildchild = EventChildChildChild("1")
    dict_processors: DefaultDict[Type[Event], List[Processor]] = defaultdict(list)
    dict_processors[type(parent)].append(p)

    assert get_processor_for_event(parent, dict_processors) == [p]
    assert get_processor_for_event(child, dict_processors) == [p]
    assert get_processor_for_event(childchild, dict_processors) == [p]
    assert get_processor_for_event(childchildchild, dict_processors) == [p]


def test_e3() -> None:

    class EventChild(E3):
        pass
    class EventChildChild(EventChild):
        pass
    class EventChildChildChild(EventChildChild):
        pass

    parent = E3(1)
    child = EventChild(1)
    childchild = EventChildChild(1)
    childchildchild = EventChildChildChild(1)
    dict_processors: DefaultDict[Type[Event], List[Processor]] = defaultdict(list)
    dict_processors[type(parent)].append(p)

    assert get_processor_for_event(parent, dict_processors) == [p]
    assert get_processor_for_event(child, dict_processors) == [p]
    assert get_processor_for_event(childchild, dict_processors) == [p]
    assert get_processor_for_event(childchildchild, dict_processors) == [p]