import os.path
from dataclasses import dataclass
from typing import Union


from blocks import source, processor, Graph


@dataclass
class KafkaMessage:
    x: int


@dataclass
class MappingWithSize:
    msg: KafkaMessage
    width: int
    height: int


@dataclass
class Meta:
    x: int
    meta: str


@dataclass
class OutputMapping:
    msg: KafkaMessage
    height: int
    width: int
    meta: str


@source
def FastKafkaConsumerMessages() -> KafkaMessage:
    return KafkaMessage(x=1)


@source
def ConsumerMeta() -> Meta:
    return Meta(meta='meta', x=1)


@processor
def MapperSize(e: KafkaMessage) -> MappingWithSize:
    return MappingWithSize(msg=e, width=100, height=200)


@processor
def Predictor(e: Union[MappingWithSize, Meta]) -> OutputMapping:
    print(e)
    return OutputMapping(msg=KafkaMessage(x=1), height=200, width=100, meta='meta')


def test_event_in_graph_specific_annotation() -> None:
    blocks = (FastKafkaConsumerMessages(), ConsumerMeta(), MapperSize(), Predictor())
    graph = Graph(blocks)
    graph.save_visualization()
    assert os.path.exists('./graph.png')
