from collections import defaultdict
from typing import List

try:
    import networkx as nx
except ImportError:
    pass

try:
    from graphviz import Digraph
except ImportError:
    pass

try:
    from matplotlib import pyplot as plt
except ImportError:
    pass

try:
    from matplotlib.axes import Axes
except ImportError:
    pass


from blocks.types.base import Block, AnyProcessors
from blocks.annotations import get_output_events_type


def build_graph(blocks: List[Block], processors_dict: AnyProcessors) -> None:
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


def build_by_graph_vis(blocks: List[Block], processors_dict: AnyProcessors) -> "Digraph":
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
    return dag
