"""
Blocks - *Sources* and *Processors* - are a backbone of you app.

Finally, all functions, decorated with :code:`@source` or :code:`@processor` are reborn to these classes.
"""

from abc import ABC, abstractmethod
from typing import Any, Union, Iterable, Optional


class Event(object):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        err = '. '.join([
            'You are going to use lightweight version of event',
            'Define __init__-method, or use dataclasses or pydantic instead.',
        ])
        raise NotImplementedError(err)


EventOrEvents = Union[Event, Iterable[Event]]


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

    def close(self) -> None:
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

    def close(self) -> None:
        """Define your graceful shutdown here."""


AnySource = Union[AsyncSource, Source]
AnyProcessor = Union[AsyncProcessor, Processor]
Block = Union[AnySource, AnyProcessor]
