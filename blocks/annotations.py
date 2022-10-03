from typing import Any, Dict, List, Type

from blocks.types import Block, Event


class AnnotationError(Exception):
    ...


def _get_annotations(block: Block) -> Dict[str, Any]:
    return dict(block.__call__.__annotations__)


def get_input_events_type(block: Block) -> List[Type[Event]]:
    annots = _get_annotations(block)
    annots.pop('return', None)
    if len(annots) == 0:
        return []
    input_type = list(annots.values())[0]
    generic_args = getattr(input_type, '__args__', None)
    if generic_args is not None:
        return list(generic_args)
    return [input_type]


def get_output_events_type(block: Block) -> List[Type[Event]]:
    annots = _get_annotations(block)
    annotations_to_flatten = [annots.get('return', None)]
    result = []
    while annotations_to_flatten:
        annotation = annotations_to_flatten.pop(0)
        if annotation is not None:
            generic_args = getattr(annotation, '__args__', None)
            if generic_args is not None:
                annotations_to_flatten.extend(list(generic_args))
            else:
                result.append(annotation)
    return result
