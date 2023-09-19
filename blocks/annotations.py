import inspect
import sys
import typing
from typing import Any, Dict, List, Type

from blocks.types import Block, Event


class AnnotationError(Exception):
    ...

class NotSupportedPythonVersion(Exception):
    ...

def _get_annotations(block: Block) -> Dict[str, Any]:
    if sys.version_info  >= (3, 9):
        # Note: inspect can't return normal annotation from string (like: def test(event: "E1") -> None: ...)
        # That's why the typing module is used
        return typing.get_type_hints(
            block.__call__,
            globalns=getattr(sys.modules.get(getattr(block, '__module__'), None), '__dict__', {})
        )
    raise NotSupportedPythonVersion("Supported only version python3.9+")


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
        # Note: typing.get_type_hints returns NoneType if function return None
        if annotation is not None and annotation is not type(None):
            generic_args = getattr(annotation, '__args__', None)
            if generic_args is not None:
                annotations_to_flatten.extend(list(generic_args))
            else:
                result.append(annotation)
    return result
