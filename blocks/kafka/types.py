"""This module contains specific event types for Kafka."""

from typing import Dict, List, Optional, NamedTuple
from dataclasses import dataclass

from wunderkafka import AnyConsumer
from confluent_kafka import TopicPartition

from blocks.types import Event
from blocks.logger import logger
# from blocks.kafka.topics import InputTopic

# ToDo (tribunsky.kir): make immutable?
ConsumersMapping = Dict['InputTopic', AnyConsumer]


class KafkaMessageMeta(NamedTuple):
    """
    Meta required for topic offset commit. Not supposed to be used directly.

    Optionals from original API:
    https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#message
    """

    topic: Optional[str]
    partition: Optional[int]
    offset: Optional[int]

    def to_commit(self) -> List[TopicPartition]:
        """
        As we do not use binary Message itself, we should increase offset by ourselves.

        - message (confluent_kafka.Message) – Commit message’s offset+1.
        - offsets (list(TopicPartition)) – List of topic+partitions+offsets to commit.

        https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.commit

        :return:    List of TopicPartitions to be committed by real consumer.
        """
        if self.topic is not None and self.partition is not None and self.offset is not None:
            tpn = TopicPartition(
                topic=self.topic,
                partition=self.partition,
                offset=self.offset + 1,
            )
            return [tpn]
        logger.warning('Some of required to commit data is absent: topic={0}, partition={1}, offset={2}'.format(
            self.topic,
            self.partition,
            self.offset,
        ))
        return []


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

      >>> from typing import Optional
      >>> from blocks import Event, processor
      >>> from blocks.kafka import CommitEvent
      >>>
      >>> class MyEvent(Event):
      ...     field1: int
      ...     field2: str
      >>>
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
        >>>
        >>> @processor
        >>> def generator(event: NoNewEvents) -> None:
        ...     print('No new events from {event.source}')
    """
    source: str


@dataclass
class Batch(Event):
    events: List[Event]
