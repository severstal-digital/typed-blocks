import os.path
from typing import Union
from dataclasses import dataclass

import pytest

from blocks import Graph, source, processor
from blocks.visualization import HAS_NETWORKX, HAS_MATPLOTLIB

if not (HAS_NETWORKX and HAS_MATPLOTLIB):
    pytest.skip("skipping visualization-only tests", allow_module_level=True)

from blocks.types.graph import RenderingKernelType


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


def test_matplotlib_visualization() -> None:
    blocks = (FastKafkaConsumerMessages(), ConsumerMeta(), MapperSize(), Predictor())
    graph = Graph(blocks)
    graph.save_visualization(rendering_type=RenderingKernelType.matplotlib)
    assert os.path.exists('GRAPH.png')


def test_graphviz_visualization() -> None:
    blocks = (FastKafkaConsumerMessages(), ConsumerMeta(), MapperSize(), Predictor())
    graph = Graph(blocks)
    graph.save_visualization(rendering_type=RenderingKernelType.graphviz)
    assert os.path.exists('DAG.svg')

def test_graphviz_node_list() -> None:
    blocks = (FastKafkaConsumerMessages(), ConsumerMeta(), MapperSize(), Predictor())
    graph = Graph(blocks)
    blocks_in_graph = graph.blocks
    for block in blocks:
        assert block in blocks_in_graph
    assert len(graph.blocks) == len(blocks)

