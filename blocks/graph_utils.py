from collections import defaultdict

import networkx as nx
from matplotlib import pyplot as plt

from blocks import Graph
from blocks.annotations import get_output_events_type


def build_graph(graph: Graph):
    event_to_processor = defaultdict(list)
    g = nx.DiGraph()
    for s in graph.blocks:
        sn = type(s).__name__
        g.add_node(sn)
        out_events = get_output_events_type(s)
        for out_event in out_events:
            event_to_processor[out_event].append(sn)
    edge_labels = {}
    for e, pre in graph.processors.items():
        for p in pre:
            pn = type(p).__name__
            if e in event_to_processor:
                for proc in event_to_processor[e]:
                    g.add_edge(
                        proc,
                        pn,
                    )
                    edge_labels[(proc, pn)] = e.__name__
    pos = nx.spring_layout(g)
    nx.draw_networkx(
        g,
        pos,
        edge_color='black',
        node_color='pink',
        with_labels=True
    )
    nx.draw_networkx_edge_labels(
        g, pos,
        font_color='red',
        edge_labels=edge_labels
    )
    plt.axis('off')
    return g
