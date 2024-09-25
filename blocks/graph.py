"""Graph - is just static collection of blocks, which is waiting a moment to be executed."""

from typing import Set, List, Type, Union, Optional, Sequence
from collections import defaultdict

from blocks.types import Block, Event, Source, AnySource, Processor, AsyncSource, AsyncProcessor
from blocks.logger import logger
from blocks.types.base import AnyProcessors, TypeOfProcessor
from blocks.validation import validate_annotations
from blocks.annotations import get_input_events_type, get_output_events_type
from blocks.visualization import build_graph
from blocks.types.graph import RenderingKernelType


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
        self.count_of_parallel_tasks: int = 0

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
        if isinstance(block, Processor) and block.type_of_processor == TypeOfProcessor.PARALLEL:
            self.count_of_parallel_tasks += 1
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

    def save_visualization(
        self,
        rendering_type: Union[str, RenderingKernelType] = RenderingKernelType.matplotlib
    ) -> None:
        """
        Build graph by using NetworkX and render by matplotlib
        saves default 'GRAPH.png' visualization on disk in the root folder
        or
        Build graphviz Digraph self representation
        and if renderer installed in the system saves
        'DAG.svg' visualization on disk in the root folder

        :param rendering_type: string or Enum type
            string only one of ['matplotlib', 'graphviz']
        """
        _types = [RenderingKernelType.matplotlib, RenderingKernelType.graphviz]
        if rendering_type not in _types:
            raise ValueError('Bad value for the argument, the value must be one of [matplotlib, graphviz]')
        try:
            build_graph(self.blocks, self.processors, rendering_type)
        except RuntimeError as ex:
            raise ex
        except Exception as ex:
            logger.error(ex)
            raise ex

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
        processors: List[Block] = list({item for sublist in list_of_lists for item in sublist})
        sources: List[Block] = list(self.sources)
        blocks = processors + sources
        return blocks
