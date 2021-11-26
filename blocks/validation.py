from typing import Any, Set, List, Type, Sequence

from blocks.types import Block, Event, Source, Processor, AsyncSource, AsyncProcessor
from blocks.logger import logger
from blocks.annotations import AnnotationError, _get_annotations, get_input_events_type, get_output_events_type

EXCLUDED_EVENTS = [type(None)]


def _flatten(list_of_lists: List[List[Any]]) -> List[Any]:
    return [val for sublist in list_of_lists for val in sublist]


def _output_events(blocks: Sequence[Block], excluded_events: Set[Type[Event]]) -> Set[Type[Event]]:
    nested_output_events = []
    for block in blocks:
        output_events = get_output_events_type(block)
        nested_output_events.append(output_events)
    return set(_flatten(nested_output_events)) - excluded_events


def _input_events(blocks: Sequence[Block]) -> Set[Type[Event]]:
    nested_input_events = []
    for block in blocks:
        if not isinstance(block, (AsyncSource, Source)):
            input_events = get_input_events_type(block)
            nested_input_events.append(input_events)
    return set(_flatten(nested_input_events))


def _useless_events(blocks: Sequence[Block], excluded_events: Set[Type[Event]]) -> Set[Type[Event]]:
    return _output_events(blocks, excluded_events) - _input_events(blocks)


def _useless_receivers(blocks: Sequence[Block], excluded_events: Set[Type[Event]]) -> Set[Block]:
    output_events_set = _output_events(blocks, excluded_events)
    useless_receivers: Set[Block] = set()
    for block in blocks:
        block_input_events = get_input_events_type(block)
        no_events_to_handle = all(event not in output_events_set for event in block_input_events)
        if not isinstance(block, (AsyncSource, Source)) and no_events_to_handle:
            useless_receivers.add(block)
    return useless_receivers


def _useless_producers(blocks: Sequence[Block], excluded_events: Set[Type[Event]]) -> Set[Block]:
    input_events_set = _input_events(blocks)
    useless_producers: Set[Block] = set()
    for ev in excluded_events:
        input_events_set.add(ev)
    for block in blocks:
        block_output_events = get_output_events_type(block)
        no_handlers_for_produced_events = all(event not in input_events_set for event in block_output_events)
        if block_output_events and no_handlers_for_produced_events:
            useless_producers.add(block)
    return useless_producers


def validate_blocks(blocks: Sequence[Block], excluded_events: Sequence[Type[Event]] = EXCLUDED_EVENTS) -> None:
    excl_set = set(excluded_events)
    for event in _useless_events(blocks, excl_set):
        logger.warning("{0} isn't processed".format(event))
    # ToDo (tribunsky.kir): bad validation, doesn't find Batches on output
    for processor in _useless_receivers(blocks, excl_set):
        logger.warning('{0} is useless. There is no events generated for'.format(processor))
    for processor in _useless_producers(blocks, excl_set):
        logger.warning('{0} is useless. There is no receivers for generated events'.format(processor))


def validate_annotations(block: Block) -> None:
    annotations = _get_annotations(block)
    if isinstance(block, (AsyncProcessor, Processor)):
        if len(annotations) < 2:
            raise AnnotationError(
                f"Given processor: {type(block).__name__} doesn't annotated well."
                'Make sure that input and output annotations properly written'
            )
        elif len(annotations) > 2:
            raise AnnotationError('Processor should have exactly one argument')

    elif isinstance(block, (AsyncSource, Source)):
        if len(annotations) != 1 and 'return' not in annotations:
            raise AnnotationError(
                f"Given source: {type(block).__name__} doesn't annotated well."
                'Make sure that output annotations properly written'
            )
