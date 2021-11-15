from dataclasses import dataclass
from blocks import Event, Graph
from blocks import processor


@dataclass
class MyEvent(Event):
    x: int


@dataclass
class MyOtherEvent(MyEvent):
    y: float


@processor
def printer(event: MyEvent) -> MyOtherEvent:
    print(event)
    return MyOtherEvent(event.x, 1.0)


def test_event_not_in_graph() -> None:
    graph = Graph()

    assert MyEvent not in graph.outputs
    assert MyOtherEvent not in graph.outputs


def test_event_in_graph() -> None:
    graph = Graph()
    graph.add_block(printer())

    assert MyEvent not in graph.outputs
    assert MyOtherEvent in graph.outputs
