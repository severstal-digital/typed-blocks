"""Graph - is just static collection of blocks, which is waiting a moment to be executed."""

from typing import Set, List, Type, Optional, Sequence
from collections import defaultdict
try:
    from graphviz import ExecutableNotFound
except ImportError:
    HAS_GRAPHVIZ = False
else:
    HAS_GRAPHVIZ = True

from matplotlib import pyplot as plt

from blocks.logger import logger
from blocks.graph_utils import build_graph, build_by_graph_vis
from blocks.types import Block, Event, Source, AnySource, Processor, AsyncSource, AsyncProcessor
from blocks.types.base import AnyProcessors
from blocks.validation import validate_annotations
from blocks.annotations import get_input_events_type, get_output_events_type


class Graph(object):
    """Represents computational graph, consisting from the given blocks and events."""

    def __init__(self, blocks: Optional[Sequence[Block]] = None) -> None:
        """
        Init Graph instance.

        Splits all given blocks into two groups: sources and processors.

        :param blocks:      Sequence of blocks to be added to computational graph.
        """
        self.sources: List[AnySource] = []
        self.processors: AnyProcessors = defaultdict(list)

        self._output_events: Set[Type[Event]] = set()

        if blocks is not None:
            for block in blocks:
                self.add_block(block)

    def add_block(self, block: Block) -> None:
        """
        Add block to current graph.

        :param block:       Processor or Source to be included in graph.
        """
        validate_annotations(block)
        if isinstance(block, (AsyncSource, Source)):
            self.sources.append(block)
        elif isinstance(block, (AsyncProcessor, Processor)):
            input_events = get_input_events_type(block)
            for event in input_events:
                self.processors[event].append(block)

            for event in get_output_events_type(block):
                self._output_events.add(event)

        else:
            raise TypeError(
                'Wrong block type: {0}, should be AsyncSource, Source, AsyncProcessor or Processor subclass'.format(
                    type(block),
                ),
            )

    def save_visualization(self, file_name: str = 'graph') -> None:
        """
        Build graph by using NetworkX and render by matplotlib
        saves default 'graph.png' visualization on disk in the root folder

        :param file_name:   name of saved file (default: 'graph'.png)

        """
        try:
            build_graph(self.blocks, self.processors)

            plt.axis('off')
            plt.savefig(file_name, dpi=100)
        except Exception as ex:
            logger.error(ex)

    def save_by_graph_vis(self) -> None:
        """
        Build graphviz Digraph self representation
        and if renderer installed in the system saves
        'DAG.svg' visualization on disk in the root folder
        """
        if not HAS_GRAPHVIZ:
            logger.error('Install the graphviz package before using')
            raise ImportError('Install the graphviz package before using')
        g = build_by_graph_vis(self.blocks, self.processors)
        try:
            g.render(cleanup=True)
        except ExecutableNotFound as exc:
            logger.error(exc)
            logger.error(
                "Can't render graph visualization, "
                "make sure that graphviz C-library installed in your system"
            )

    @property
    def contains_async_blocks(self) -> bool:
        for source in self.sources:
            if isinstance(source, AsyncSource):
                return True
        for processors in self.processors.values():
            for processor in processors:
                if isinstance(processor, AsyncProcessor):
                    return True
        return False

    @property
    def outputs(self) -> Set[Type[Event]]:
        return self._output_events

    @property
    def blocks(self) -> List[Block]:
        list_of_lists = [lst for lst in self.processors.values()]
        processors: List[Block] = [item for sublist in list_of_lists for item in sublist]
        sources: List[Block] = list(self.sources)
        blocks = processors + sources
        return blocks
