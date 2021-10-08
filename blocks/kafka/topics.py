"""This module contains specific per topic configuration definitions."""

import datetime
from enum import Enum
from typing import Type, Optional

from wunderkafka import AnyConsumer, AnyProducer, AvroConsumer, ConsumerConfig, ProducerConfig, AvroModelProducer

from blocks.types import Event
from blocks.logger import logger
from blocks.kafka.types import Batch


class Consumer(object):
    type: AnyConsumer = AvroConsumer
    conf: Optional[ConsumerConfig] = None


class Producer(object):
    type: AnyProducer = AvroModelProducer
    conf: Optional[ProducerConfig] = None


class CommitChoices(Enum):
    never = 'never'
    regular = 'regular'
    manual = 'manual'


class _Topic(object):

    def __init__(
        self,
        name: str,
        event: Type[Event],
        *,
        key: Optional[Type[Event]] = None,
    ) -> None:
        self.name = name
        self.event = event
        self.key = key


class OutputTopic(_Topic):
    """
    Binds event to Kafka topic and additionally provides debug mode topic reading and customizable key if necessary.

    Example::

      >>> from blocks import Event, Graph
      >>> from blocks.kafka import KafkaProducer,
      >>>
      >>> class MyEvent(Event):
      ...     x: int
      >>>
      >>> topics = [OutputTopic('some_topic', MyEvent, debug='all')]
      >>> graph = Graph()
      >>> graph.add_block(KafkaProducer(topics))
    """

    def __init__(
        self,
        name: str,
        event: Type[Event],
        *,
        key: Optional[Type[Event]] = None,
        producer: Producer = Producer(),
    ) -> None:
        super().__init__(name, event, key=key)
        self.producer = producer


# ToDo (tribunsky.kir): InputTopic overloaded.
class InputTopic(_Topic):
    """
    Binds Kafka topic to event and additionally provides a bunch of topic properties to set up.

    Example::

      >>> from blocks import Event, Graph
      >>> from blocks.kafka import KafkaSource, InputTopic
      >>> class MyEvent(Event):
      ...     x: int
      >>> topics = [InputTopic('some_topic', MyEvent, from_beginning=True)]
      >>> graph = Graph()
      >>> graph.add_block(KafkaSource(topics))
    """

    def __init__(
        self,
        name: str,
        event: Type[Event],
        *,
        group_id: Optional[str] = None,
        key: Optional[Type[Event]] = None,
        commit_offset: str = 'never',
        ignore_keys: bool = True,

        from_beginning: Optional[bool] = False,
        with_timedelta: Optional[datetime.timedelta] = None,
        poll_timeout: float = 0.05,
        messages_limit: int = 10000,

        dummy_events: bool = False,

        batch_event: Optional[Type[Batch]] = None,
        read_till: Optional[int] = None,
        max_empty_polls: int = 0,

        consumer: Consumer = Consumer(),
    ) -> None:
        self.read_till = read_till
        self.max_empty_polls = max_empty_polls
        if self.max_empty_polls and self.read_till:
            exc_msg = 'Only one condition at time per topic is allowed, but both max_empty_polls and read_till are set'
            raise ValueError(exc_msg)
        if batch_event is not None:
            if self.read_till is None:
                failover = int(abs(10 / poll_timeout)) or 1
                logger.warning('Using {0} as analog if 10 seconds as failover for batch waiting.'.format(failover))
                self.max_empty_polls = self.max_empty_polls

        super().__init__(name, event, key=key)
        self.group_id = group_id
        self.from_beginning = from_beginning
        self.commit_offset = commit_offset
        self.ignore_keys = ignore_keys
        self.with_timedelta = with_timedelta
        self.dummy_events = dummy_events
        self.batch_event = batch_event

        self.poll_timeout = poll_timeout
        self.messages_limit = messages_limit
        self.consumer = consumer

    # Shortening code in Consumers, but still exhibiting strings in API.
    @property
    def committable(self) -> bool:
        return CommitChoices(self.commit_offset) is not CommitChoices.never

    @property
    def commit_manually(self) -> bool:
        return CommitChoices(self.commit_offset) is CommitChoices.manual

    @property
    def commit_regularly(self) -> bool:
        return CommitChoices(self.commit_offset) is CommitChoices.regular

    @property
    def batched(self) -> bool:
        return self.batch_event is not None or self.read_till is not None
