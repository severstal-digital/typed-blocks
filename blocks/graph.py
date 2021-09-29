from typing import List, Type, Optional, Sequence, DefaultDict
from collections import defaultdict

from blocks.types import (
    Block,
    Event,
    Source,
    AnySource,
    Processor,
    AsyncSource,
    AnyProcessor,
    AsyncProcessor,
)
from blocks.validation import validate_annotations
from blocks.annotations import get_input_events_type

AnyProcessors = DefaultDict[Type[Event], List[AnyProcessor]]


class Graph:
    def __init__(self, blocks: Optional[Sequence[Block]] = None) -> None:
        self.sources: List[AnySource] = []
        self.processors: AnyProcessors = defaultdict(list)
        if blocks is not None:
            for block in blocks:
                self.add_block(block)

    def add_block(self, block: Block) -> None:
        validate_annotations(block)
        if isinstance(block, (AsyncSource, Source)):
            self.sources.append(block)
        elif isinstance(block, (AsyncProcessor, Processor)):
            input_events = get_input_events_type(block)
            for event in input_events:
                self.processors[event].append(block)
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
