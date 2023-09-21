import inspect
from typing import Any, Dict, List, Type, Tuple, Union, Optional, Sequence
from dataclasses import is_dataclass

from pydantic import ValidationError
from wunderkafka import Message, AnyConsumer, AvroConsumer, ConsumerConfig

from blocks.types import Event, Source
from blocks.logger import logger
from blocks.kafka.types import ConsumersMapping, KafkaMessageMeta
from blocks.kafka.events import NoNewEvents, EndTsIsReached
from blocks.kafka.topics import InputTopic


class KafkaSource(Source):
    """
    Class represents event source that reads messages from Kafka topics
    and wraps them into given events. On initialization must get list
    of input topics in order to perform arbitrary mapping. In most cases
    you don't need to use KafkaSource directly, KafkaApp can make this for you.

    Example::

      >>> from blocks import Event
      >>> from pydantic import BaseModel
      >>> from blocks.kafka import KafkaSource, InputTopic

      >>> class MyEvent(BaseModel):
      ...     x: int

      >>> topics = [InputTopic('some_topic', MyEvent)]
      >>> blocks = (KafkaSource(topics), )
    """

    def __init__(
        self,
        topics: List[InputTopic],
        config: Optional[ConsumerConfig] = None,
        *,
        cls: Type[AnyConsumer] = AvroConsumer,
        ignore_errors: bool = True,
    ) -> None:
        """
        Init KafkaSource instance.

        :param topics:          List of topics with corresponding settings to subscribe.
        :param config:          Failover configuration for consumer if there is no configuration per InputTopic.
        :param cls:             Failover factory to create consumer if there is no configuration per InputTopic.
        :param ignore_errors:   If True, do not fail during casting messages to events. By design, messages in Kafka may
                                be produced with totally different schemas, or without any.
        """
        self.consumers: ConsumersMapping = _init_consumers(topics, config, cls)
        self._commit_messages: Dict[InputTopic, Message] = {}
        self._ignore_errors = ignore_errors

        self._prev_poll: Dict[InputTopic, List[Message]] = {}
        self.patch_annotations(
            {
                topic.batch_event if (topic.batched or topic.max_empty_polls) and topic.batch_event else topic.event
                for topic in topics
            }
        )

    def __call__(self) -> List[Event]:
        """
        Emit messages from all topics as events to the internal queue.

        :return:        Single event or sequence of events.
        """
        self._commit()

        # If we've read some messages on previous iteration, we don't want to lose them just because they are too new
        events = []
        to_next: Dict[InputTopic, List[Message]] = {}

        for topic, consumer in self.consumers.items():
            if topic.batched or topic.max_empty_polls:
                messages, next_poll = _read_batch(consumer, topic)
            else:
                next_poll = []
                messages = consumer.consume(topic.poll_timeout, topic.messages_limit, ignore_keys=topic.ignore_keys)

            if topic.commit_regularly and messages:
                # Not using CommitEvent as we close enough to consumer itself. R. Reliability. C. Consistency.
                self._commit_messages[topic] = messages[-1]

            to_next[topic] = next_poll

            prev_events = self._prev_poll.get(topic, [])

            events.extend(_make_events(prev_events + messages, topic, self._ignore_errors))

        self._prev_poll = to_next

        return events

    def close(self) -> None:
        """Graceful shutdown: commit anything that should be committed as in InputTopic configurations."""
        self._commit()

    def _commit(self) -> None:
        for topic, message in self._commit_messages.items():
            self.consumers[topic].commit(message)
        if self._commit_messages:
            logger.debug(f'Messages: {self._commit_messages} committed')
            self._commit_messages.clear()


def _stash_msg_meta(event: Event, msg: Message) -> None:
    """Picking just necessary data to commit message later."""
    meta = KafkaMessageMeta(topic=msg.topic(), partition=msg.partition(), offset=msg.offset())
    event.__dict__['@msg'] = meta


def shortened(src: Dict[str, Any], n: int = 8) -> Dict[str, Any]:
    target = n - 3
    dct = {}
    for k, v in src.items():
        dct[k] = v
        if isinstance(v, (str, bytes)):
            if len(v) > target:
                if isinstance(v, str):
                    dct[k] = v[:target] + '...'
                else:
                    dct[k] = v[:target] + b'...'

    return dct


def cast(msg: Message, codec: Type[Event], *, ignore_errors: bool, verbose_log_errors: bool = True) -> Optional[Event]:
    # It actually works not only against instance, but against cls too
    message_value = msg.value()
    logger.debug('{0}|{1}|{2} (e: {3}): {4}'.format(
        msg.topic(), msg.partition(), msg.offset(), msg.error(), message_value),
    )
    if is_dataclass(codec):
        # ToDo (tribunsky.kir): move it to external cache OR
        #                       just do not use internally raw InputTopic's model on every message
        fields = inspect.signature(codec).parameters
        dct = {k: v for k, v in message_value.items() if k in fields}
    else:
        dct = message_value

    try:
        return codec(**dct)
    except TypeError as exc:
        if 'takes no arguments' in str(exc):
            event = codec()
            for k, v in dct.items():
                if hasattr(event, k):
                    setattr(event, k , v)
            return event
        if verbose_log_errors:
            logger.error(exc)
        if ignore_errors is False:
            raise
    except (ValidationError, TypeError) as exc:
        if verbose_log_errors:
            logger.error(exc)
        else:
            logger.error('Failed to extract the message ({0})'.format(shortened(dct)))
        if ignore_errors is False:
            raise
    return None


def _make_events(
    messages: List[Message],
    topic: InputTopic,
    ignore_errors: bool,
) -> List[Event]:
    events: List[Event] = []
    for msg in messages:
        event = cast(msg, topic.event, ignore_errors=ignore_errors, verbose_log_errors=topic.verbose_log_errors)
        if topic.filter_function(event):
            if event is not None:
                if topic.commit_manually:
                    # Do not know in advance which event should be committed.
                    # So stashing necessary meta to every event from topics which may be committed manually.
                    _stash_msg_meta(event, msg)
                events.append(event)

    if topic.batched:
        if topic.batch_event is None:
            events.append(EndTsIsReached(topic_name=topic.name))
        else:
            return [topic.batch_event(events=events)]
    else:
        if not events and topic.dummy_events:
            events.append(NoNewEvents(source=f'{topic.name} Kafka topic'))

    return events


def _init_consumers(
    topics: Sequence[InputTopic],
    config: Optional[ConsumerConfig],
    cls: Type[AnyConsumer],
) -> ConsumersMapping:
    # ToDo (tribunsky.kir): looks like gid should be set per topic.
    consumers = {}
    for topic in topics:
        if topic.group_id is None:
            if config is None:
                msg = ' '.join([
                    'Please, check group.id.',
                    'It may be set per InputTopic or should at least be provided via ConsumerConfig.',
                    'Currently both values are set to None.'
                ])
                raise ValueError(msg)
            else:
                gid = config.group_id
        else:
            gid = topic.group_id
        if topic.committable and gid is None:
            raise ValueError('For offset committing group_id must be provided')

        cfg = topic.consumer.conf or config
        if cfg is None:
            raise ValueError('Consumer configuration must be set at least for all topic or per every topic.')
        if topic.group_id:
            cfg.group_id = topic.group_id

        cns = topic.consumer.type or cls
        consumer = cns(cfg)

        if topic.committable:
            consumer.subscribe([topic.name])
        else:
            if topic.with_timedelta is not None:
                consumer.subscribe([topic.name], with_timedelta=topic.with_timedelta)
            else:
                consumer.subscribe([topic.name], from_beginning=topic.from_beginning)
        consumers[topic] = consumer
    return consumers


def _read_batch(consumer: AnyConsumer, topic: InputTopic) -> Tuple[List[Message], List[Message]]:
    if topic.read_till is not None:
        return _read_topic_till_ts(consumer, topic)
    if topic.max_empty_polls > 0:
        return _read_topic_till_end(consumer, topic), []
    msg_template = 'Wrong read_till ({0}) or max_empty_polls ({1}) for {2}'
    logger.warning(msg_template.format(topic.read_till, topic.max_empty_polls, topic.name))
    return [], []


def _read_topic_till_ts(consumer: AnyConsumer, topic: InputTopic) -> Tuple[List[Message], List[Message]]:
    # Search every topic partition till any messages with appropriate ts are polled
    messages = []
    also_polled = []
    should_read = True
    while should_read:
        msgs = consumer.consume(topic.poll_timeout, topic.messages_limit, ignore_keys=topic.ignore_keys)
        if msgs:
            appended = False
            for msg in msgs:
                _, ts = msg.timestamp()
                if ts <= topic.read_till:
                    messages.append(msg)
                    appended = True
                else:
                    also_polled.append(msg)
            should_read = appended is True and len(messages) > 0
    return messages, also_polled


def _read_topic_till_end(consumer: AnyConsumer, topic: InputTopic) -> List[Message]:
    # It may be implemented via other strategy: discover maximum offset in topic via subscription to OFFSET_END
    messages = []
    empty_polls_count = 0
    while empty_polls_count <= topic.max_empty_polls:
        msgs = consumer.consume(topic.poll_timeout, topic.messages_limit, ignore_keys=topic.ignore_keys)
        if msgs:
            messages.extend(msgs)
            empty_polls_count = 0
        else:
            empty_polls_count += 1
    return messages
