from typing import Any, Dict, List, Type, _GenericAlias  # type: ignore

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
    if isinstance(input_type, _GenericAlias):
        return list(input_type.__args__)
    return [input_type]


def get_output_events_type(block: Block) -> List[Type[Event]]:
    annots = _get_annotations(block)
    annotations_to_flatten = [annots.get('return', None)]
    result = []
    while annotations_to_flatten:
        annotation = annotations_to_flatten.pop(0)
        if annotation is None:
            continue
        if isinstance(annotation, _GenericAlias):
            annotations_to_flatten.extend(list(annotation.__args__))
        else:
            result.append(annotation)
    return result
