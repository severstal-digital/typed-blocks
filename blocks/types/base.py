"""
Blocks - *Sources* and *Processors* - are a backbone of you app.

Finally, all functions, decorated with :code:`@source` or :code:`@processor` are reborn to these classes.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Set, List, Type, Union, Iterable, Optional, DefaultDict


Event = object
EventOrEvents = Union[Event, Iterable[Event]]

def is_named_tuple(event: EventOrEvents) -> bool:
    return isinstance(event, tuple) and hasattr(event, '_asdict') and hasattr(event, '_fields')


class TypeOfProcessor(Enum):
    SYNC = 0
    PARALLEL = 1


class Source(ABC):
    """
    Interface to represent single source of events, which emits them for the further processing.

    Class usage allows easily handle some internal state, if needed.

    Example::

      >>> from typing import NamedTuple, List

      >>> from blocks import Source

      >>> class MyEvent(NamedTuple):
      ...     ...

      >>> class MySource(Source):
      ...
      ...     def __init__(self, events: List[MyEvent]) -> None:
      ...         self.events = events
      ...
      ...     def __call__(self) -> List[MyEvent]:
      ...         return self.events[:]

      >>> blocks = (MySource(events=[MyEvent()]))
    """
    def patch_annotations(self, out_types: Set[Type[Event]]) -> None:
        # Just magic
        # https://mypy.readthedocs.io/en/stable/common_issues.html#variables-vs-type-aliases
        self.__call__.__annotations__['return'] = List[Union[tuple(out_types)]]                           # type: ignore

    @abstractmethod
    def __call__(self) -> EventOrEvents:
        """
        Emit new events to the internal queue.

        :return:        Single event or sequence of events.
        """

    def close(self) -> None:
        """Define your graceful shutdown here."""


class Processor(ABC):
    """
    Interface to represent single unit of calculations, which reacts to some specific event.

    Class usage allows easily handle some state in processors, if needed.

    Example::

      >>> from typing import NamedTuple, Dict

      >>> from blocks import Processor

      >>> class MyEvent(NamedTuple):
      ...     ...

      >>> class Printer(Processor):
      ...
      ...     def __init__(self, state: Dict[str, int]) -> None:
      ...         self.state = state
      ...
      ...     def __call__(self, event: MyEvent) -> None:
      ...         print(event)

      >>> blocks = (Printer(state={}),)
    """

    _type_of_processor: TypeOfProcessor = TypeOfProcessor.SYNC

    @property
    def type_of_processor(self) -> TypeOfProcessor:
        return self._type_of_processor

    @type_of_processor.setter
    def type_of_processor(self, v: int) -> None:
        self._type_of_processor = TypeOfProcessor(v)

    @abstractmethod
    def __call__(self, event: Any) -> Optional[EventOrEvents]:
        """
        Handle event.

        :param event:   Specific event to handle. If, for some reason, you want to process every event,
                        it is also possible by passing Any to implementation.
        :return:        Event or sequence of events to be processed later or by other processed.
                        Processor may return None, e.g. while implementing some effects in outer systems and
                        when result processing is unnecessary.
        """

    def close(self) -> None:
        """Define your graceful shutdown here."""


class AsyncSource(ABC):
    """
    Interface to represent single source of events, which emits them for the further processing.

    Class usage allows easily handle some internal state, if needed.

    Same as usual :code:`Source`, but awaitable.
    """

    @abstractmethod
    async def __call__(self) -> EventOrEvents:
        """
        Emit new events to the internal queue.

        :return:        Single event or sequence of events.
        """

    async def close(self) -> None:
        """Define your graceful shutdown here."""


class AsyncProcessor(ABC):
    """
    Interface to represent single unit of calculations, which reacts to some specific event.

    Class usage allows easily handle some state in processors, if needed.

    Same as usual :code:`Processor`, but awaitable.
    """

    @abstractmethod
    async def __call__(self, event: Any) -> Optional[EventOrEvents]:
        """
        Handle event.

        :param event:   Specific event to handle. If, for some reason, you want to process every event,
                        it is also possible by passing Any to implementation.
        :return:        Event or sequence of events to be processed later or by other processed.
                        Processor may return None, e.g. while implementing some effects in outer systems and
                        when result processing is unnecessary.
        """

    async def close(self) -> None:
        """Define your graceful shutdown here."""


AnySource = Union[AsyncSource, Source]
AnyProcessor = Union[AsyncProcessor, Processor]
Block = Union[AnySource, AnyProcessor]
AnyProcessors = DefaultDict[Type[Event], List[AnyProcessor]]
