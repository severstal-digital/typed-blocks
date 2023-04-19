import re
import subprocess
from typing import List, Union
from collections import defaultdict

from blocks.logger import logger
from blocks.types.graph import RenderingKernelType

try:
    from graphviz import Digraph, ExecutableNotFound
except ImportError:
    HAS_GRAPHVIZ = False
else:
    HAS_GRAPHVIZ = True

try:
    import networkx as nx
except ImportError:
    HAS_NETWORKX = False
else:
    HAS_NETWORKX = True

try:
    from matplotlib import pyplot as plt
    from matplotlib.axes import Axes
except ImportError:
    HAS_MATPLOTLIB = False
else:
    HAS_MATPLOTLIB = True

from blocks.types.base import Block, AnyProcessors
from blocks.annotations import get_output_events_type


def build_graph(
    blocks: List[Block],
    processors_dict: AnyProcessors,
    rendering_type: Union[str, RenderingKernelType]
) -> None:
    if rendering_type == RenderingKernelType.matplotlib:
        _build_graph_matplot(blocks, processors_dict)
    else:
        _build_graph_graphviz(blocks, processors_dict)


def _build_graph_matplot(
    blocks: List[Block],
    processors_dict: AnyProcessors,
    *,
    file_name: str = 'GRAPH'
) -> None:
    if not HAS_MATPLOTLIB or not HAS_NETWORKX:
        logger.error('Install the NetworkX and matplotlib packages before using')
        raise RuntimeError('Install the NetworkX and matplotlib packages before using')

    event_to_processor = defaultdict(list)
    g = nx.DiGraph()
    for s in blocks:
        sn = type(s).__name__
        g.add_node(sn)
        out_events = get_output_events_type(s)
        for out_event in out_events:
            event_to_processor[out_event].append(sn)
    edge_labels = {}
    for event, processors in processors_dict.items():
        for processor in processors:
            processor_name = type(processor).__name__
            if event in event_to_processor:
                for proc in event_to_processor[event]:
                    g.add_edge(
                        proc,
                        processor_name,
                        length=400
                    )
                    edge_labels[(proc, processor_name)] = event.__name__
    pos = nx.spring_layout(g)
    ax: Axes = plt.subplot()
    ax.margins(0.2)

    nx.draw_networkx(
        g,
        pos,
        ax=ax,
        edge_color='black',
        node_color='pink',
        with_labels=True,
        arrows=True,
        font_size=8,
    )
    nx.draw_networkx_edge_labels(
        g, pos,
        font_color='red',
        edge_labels=edge_labels,
        font_size=8,
    )
    plt.axis('off')
    plt.savefig(file_name, dpi=100)


def _build_graph_graphviz(blocks: List[Block], processors_dict: AnyProcessors) -> None:
    if not HAS_GRAPHVIZ:
        logger.error('Install the graphviz package before using')
        raise RuntimeError('Install the graphviz package before using')
    output = subprocess.run(['dot', '-V'], capture_output=True, timeout=5)
    result = re.search(r'graphviz version \d+?.\d+?.\d+?', output.stderr.decode('utf-8'))
    if result is None:
        raise Exception("Can't render graph visualization, "
                        "make sure that graphviz C-library installed in your system"
                        )
    logger.info('Using system {}'.format(result.group()))

    dag = Digraph(
        'DAG',
        format='svg',
        filename='DAG',
        node_attr={'color': 'lightblue2', 'style': 'filled'},
    )
    event_to_processor = defaultdict(list)
    for processor in blocks:
        processor_name = type(processor).__name__
        dag.node(processor_name)
        out_events = get_output_events_type(processor)
        for out_event in out_events:
            event_to_processor[out_event].append(processor_name)
    for event, processors in processors_dict.items():
        for processor in processors:
            processor_name = type(processor).__name__
            if event in event_to_processor:
                for proc in event_to_processor[event]:
                    dag.edge(
                        proc,
                        processor_name,
                        event.__name__,
                    )
    dag.render(cleanup=True)
