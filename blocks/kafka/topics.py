"""This module contains specific per topic configuration definitions."""

import datetime
from enum import Enum
from typing import Type, Optional, Callable, Dict

from wunderkafka import AnyConsumer, AnyProducer, ConsumerConfig, ProducerConfig

from blocks.types import Event
from blocks.logger import logger
from blocks.kafka.events import Batch


class ConsumerFactory(object):
    """Class to allow some narrow customization of consumers via InputTopic."""

    def __init__(
        self,
        # Already set by KafkaSource by default, so it will be used globally if not specified via topic
        cls: Optional[Type[AnyConsumer]] = None,
        config: Optional[ConsumerConfig] = None,
    ) -> None:
        self.type = cls
        self.conf = config


class ProducerFactory(object):
    """Class to allow some narrow customization of producers via OutputTopic."""

    def __init__(
        self,
        # Already set by KafkaSource by default, so it will be used globally if not specified via topic
        cls: Optional[Type[AnyProducer]] = None,
        config: Optional[ProducerConfig] = None,
    ) -> None:
        self.type = cls
        self.conf = config


DEFAULT_CONSUMER = ConsumerFactory()
DEFAULT_PRODUCER = ProducerFactory()


class CommitChoices(Enum):
    """
    Enumeration represents allowed strategies for committing offsets of processed events via InputTopic.

    Options are:

        - 'never'
            Do not commit offset.
        - 'regular'
            Commit offset after the batch of messages has been consumed.
        - 'manual'
            For complicated cases when we want to commit offset only after the message has been actually processed.
            In that case, specific CommitEvent should be returned from specific processor.
    """

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
    Describes output event and specific producer's options for a given Kafka topic with a customizable key if necessary.

    Example::

      >>> from typing import NamedTuple

      >>> from blocks import Event
      >>> from blocks.kafka import KafkaProducer,

      >>> class MyEvent(NamedTuple):
      ...     x: int

      >>> topics = [OutputTopic('some_topic', MyEvent, debug='all')]
      >>> blocks = (KafkaProducer(topics), ...)
    """

    def __init__(
        self,
        name: str,
        event: Type[Event],
        *,
        key: Optional[Type[Event]] = None,
        producer: ProducerFactory = DEFAULT_PRODUCER,
    ) -> None:
        """
        Init Output's topic instance.

        :param name:            Kafka topic name to produce messages.
        :param event:           Event-inherited model to be serialized to kafka message's value.
        :param key:             Event-inherited model to be serialized to kafka message's key, if any.
        :param producer:        Factory which provides the way to instantiate producer.
        """
        super().__init__(name, event, key=key)
        self.producer = producer


class InputTopic(_Topic):
    """
    Describes input event stream from a given Kafka topic with a great flexibility.

    Example::

      >>> from typing import NamedTuple

      >>> from blocks import Event
      >>> from blocks.kafka import KafkaSource, InputTopic

      >>> class MyEvent(NamedTuple):
      ...     x: int

      >>> topics = [InputTopic('some_topic', MyEvent, from_beginning=True)]
      >>> blocks = (KafkaSource(topics), )
    """

    def __init__(  # noqa: WPS211  # ToDo (tribunsky.kir): InputTopic overloaded.
        self,
        name: str,
        event: Type[Event],
        *,
        filter_function: Callable[[Event], bool] = lambda x: True,
        group_id: Optional[str] = None,
        key: Optional[Type[Event]] = None,
        commit_offset: str = 'never',
        ignore_keys: bool = True,
        verbose_log_errors: bool = True,
        # per 'physical' consumer parameters, if needed.
        from_beginning: Optional[bool] = False,
        with_timedelta: Optional[datetime.timedelta] = None,
        poll_timeout: float = 0.05,
        messages_limit: int = 10000,
        # if any, send NoNewEvents
        dummy_events: bool = False,
        # it's all about batching.
        batch_event: Optional[Type[Batch]] = None,
        read_till: Optional[int] = None,
        max_empty_polls: int = 0,
        # allow to rebuild consumer
        consumer: ConsumerFactory = DEFAULT_CONSUMER,
    ) -> None:
        """
        Init InputTopic's instance.

        :param name:            Kafka topic name to be consumed.
        :param event:           Event-inherited model for message values to be unpacked to.
        :param filter_function: The function of filtering messages from the topic.
        :param group_id:        Override consumer's group id from config.
        :param key:             Event-inherited model for messages keys to be unpacked to, if any.
        :param commit_offset:   One of possible strategies of how to commit offsets for a given event.
        :param ignore_keys:     If True, don't try to deserialize message's keys. This flag defines behaviour of
                                underlying consumer, so if the event model isn't define for a given key and this flag
                                is not set to False, under the hood key will be deserialized.
        :param from_beginning:  If True, will read topic from the earliest offset, otherwise from the latest.
        :param with_timedelta:  Subscribe to topic via timestamp for a specific timedelta in the past.
        :param poll_timeout:    Interval to await for a new messages from Kafka broker.
        :param messages_limit:  Maximum amount of messages to be awaited for a given interval.
        :param dummy_events:    If True, emit NoNewEvents to the internal queue to notify processors, that there is no
                                new messages has been consumed for the last poll.
        :param batch_event:     If specified, packs all unpacked events to a single Batch event.
        :param read_till:       If set, will read topic until the given timestamp is reached. In addition to sequence
                                of events will also append built-in EndTsIsReached to notify producers.
        :param max_empty_polls: if set, will read topic until there are no new events consumed for a given count of
                                polls. If batch_event is specified, will pack messages to a single event, otherwise
                                behaves like read_till.
        :param consumer:        Factory which provides the way to instantiate consumer.

        :raises ValueError:     For broken invariants or incompatible options.
        """
        self.read_till = read_till
        self.max_empty_polls = max_empty_polls
        if self.max_empty_polls and self.read_till:
            exc_msg = 'Only one condition at time per topic is allowed, but both max_empty_polls and read_till are set'
            raise ValueError(exc_msg)
        if batch_event is not None and self.read_till is None:
            failover = int(abs(10 / poll_timeout)) or 1
            logger.warning('Using {0} as analog of 10 seconds for batch waiting failover.'.format(failover))
            self.max_empty_polls = failover
        if with_timedelta is not None and from_beginning is True:
            raise ValueError("Can't simultaneously read topic from the beginning and from specific timedelta")

        super().__init__(name, event, key=key)
        self.group_id = group_id
        self.from_beginning = from_beginning
        self.commit_offset = commit_offset
        self.ignore_keys = ignore_keys
        self.verbose_log_errors = verbose_log_errors
        self.with_timedelta = with_timedelta
        self.dummy_events = dummy_events
        self.batch_event = batch_event

        self.poll_timeout = poll_timeout
        self.messages_limit = messages_limit
        self.consumer = consumer
        self.filter_function = filter_function

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
