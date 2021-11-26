from typing import List
from dataclasses import dataclass

from blocks.types import Event


@dataclass
class EndTsIsReached(Event):
    """
    Built-in event which notifies app that all messages since application start have been already received.

    Not supposed to be used directly.
    """

    topic_name: str


# ToDo (tribunsky.kir): limited usage ability. Maybe we need more tracing between blocks.
@dataclass
class CommitEvent(Event):
    """
    Built-in event which allows to commit offset if specific message has been already processed.

    Currently limited by events which are manually casted from kafka messages.

    Example::

      >>> from typing import Optional, NamedTuple
      >>> from blocks import Event, processor
      >>> from blocks.kafka import CommitEvent

      >>> class MyEvent(NamedTuple):
      ...     field1: int
      ...     field2: str

      >>> @processor
      >>> def proc(event: MyEvent) -> Optional[CommitEvent]:
      ...     ...
      ...     if event.field2 == 'some':
      ...         return CommitEvent(e=event)
    """

    e: Event  # noqa: WPS111  # ToDo (tribunsky.kir): awkward API. Do something with `e=` w/o overloading core event.


# ToDo (tribunsky.kir): IMHO, it is idiomatic enough to spawn such event, but there was an idea to get rid of it.
@dataclass
class NoNewEvents(Event):
    """
    Built-in event which allows to notify application, that there is no new messages polled from Kafka InputTopic.

    Example::

        >>> from blocks.kafka import NoNewEvents
        >>> from blocks import processor

        >>> @processor
        >>> def generator(event: NoNewEvents) -> None:
        ...     print('No new events from {event.source}')
    """

    source: str


@dataclass
class Batch(Event):
    """Built-in event which allows to send multiple messages to processors within single event."""

    events: List[Event]
