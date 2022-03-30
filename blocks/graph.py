"""Graph - is just static collection of blocks, which is waiting a moment to be executed."""

from typing import Set, List, Type, Optional, Sequence, DefaultDict
from collections import defaultdict

from blocks.types import Block, Event, Source, AnySource, Processor, AsyncSource, AnyProcessor, AsyncProcessor
from blocks.validation import validate_annotations
from blocks.annotations import get_input_events_type, get_output_events_type

AnyProcessors = DefaultDict[Type[Event], List[AnyProcessor]]


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
