import inspect
from typing import Dict, List, Type, Union, Optional, Sequence
from dataclasses import is_dataclass

from pydantic import ValidationError
from wunderkafka import Message, AnyConsumer, AvroConsumer, ConsumerConfig

from blocks.types import Event, Source
from blocks.logger import logger
from blocks.kafka.types import NoNewEvents, EndTsIsReached, ConsumersMapping, KafkaMessageMeta
from blocks.kafka.topics import InputTopic


class KafkaSource(Source):
    """
    Class represents event source that reads messages from Kafka topics
    and wraps them into given events. On initialization must get list
    of input topics in order to perform arbitrary mapping. In most cases
    you don't need to use KafkaConsumer directly, KafkaApp can make this for you.

    Example::

      >>> from blocks import Event, Graph
      >>> from blocks.kafka import KafkaSource, InputTopic
      >>> class MyEvent(Event):
      ...     x: int
      >>> topics = [InputTopic('some_topic', MyEvent)]
      >>> graph = Graph()
      >>> graph.add_block(KafkaSource(topics))
    """

    def __init__(
        self,
        topics: List[InputTopic],
        config: Optional[ConsumerConfig] = None,
        *,
        cls: AnyConsumer = AvroConsumer,
        ignore_errors: bool = True,
    ) -> None:
        self.consumers: ConsumersMapping = _init_consumers(topics, config, cls)
        self._commit_messages: Dict[InputTopic, Message] = {}
        self._ignore_errors = ignore_errors
        out_annots = {topic.event for topic in topics}
        self.__call__.__annotations__['return'] = List[Union[tuple(out_annots)]]  # type: ignore

    def __call__(self) -> List[Event]:
        self._commit()
        events = []
        for topic, consumer in self.consumers.items():

            if topic.batched:
                print('Consuming batched {0}'.format(topic.name))
                messages = _read_batch(consumer, topic)
            else:
                print('Consuming {0}'.format(topic.name))
                messages = consumer.consume(topic.poll_timeout, topic.messages_limit, ignore_keys=topic.ignore_keys)

            if topic.commit_regularly and messages:
                # Not using CommitEvent as we close enough to consumer itself. R. Reliability. C. Consistency.
                self._commit_messages[topic] = messages[-1]

            events.extend(_make_events(messages, topic, self._ignore_errors))

        return events

    def close(self) -> None:
        self._commit()

    def _commit(self) -> None:
        for topic, message in self._commit_messages.items():
            self.consumers[topic].consumer.commit(message)
        if self._commit_messages:
            logger.debug(f'Messages: {self._commit_messages} committed')
            self._commit_messages.clear()


def _stash_msg_meta(event: Event, msg: Message) -> None:
    """Picking just necessary data to commit message later."""
    meta = KafkaMessageMeta(topic=msg.topic(), partition=msg.partition(), offset=msg.offset())
    event.__dict__['@msg'] = meta


def cast(msg: Message, codec: Type[Event], ignore_errors: bool) -> Optional[Event]:
    # It actually works not only against instance, but against cls too
    if is_dataclass(codec):
        # ToDo (tribunsky.kir): move it to external cache OR
        #                       just do not use internally raw InputTopic's model on every message
        dct = {k: v for k, v in msg.value().items() if k in inspect.signature(codec).parameters}
    else:
        dct = msg.value()

    try:
        return codec(**dct)
    except (ValidationError, TypeError) as e:
        logger.error(e)
        if ignore_errors is False:
            raise


def _make_events(
    messages: List[Message],
    topic: InputTopic,
    ignore_errors: bool,
) -> List[Event]:
    events: List[Event] = []
    for msg in messages:
        event = cast(msg, topic.event, ignore_errors)
        if event is not None:
            if topic.commit_manually:
                # Do not knowing in advance which event should be committed.
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
    cls: AnyConsumer,
) -> ConsumersMapping:
    # ToDo (tribunsky.kir): looks like gid should be set per topic.
    consumers = {}
    for topic in topics:
        gid = topic.group_id or config.group_id
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
            consumer.subscribe([topic.name], from_beginning=topic.from_beginning)
        consumers[topic] = consumer
    return consumers


def _read_batch(consumer: AnyConsumer, topic: InputTopic) -> List[Message]:
    if topic.read_till is None:
        messages = _read_topic_till_end(consumer, topic)
    elif topic.max_empty_polls > 0:
        messages = _read_topic_till_ts(consumer, topic)
    else:
        msg_template = 'Wrong read_till ({0}) or max_empty_polls ({1}) for {2}'
        logger.warning(msg_template.format(topic.read_till, topic.max_empty_polls, topic.name))
        return []
    return messages


def _read_topic_till_ts(consumer: AnyConsumer, topic: InputTopic) -> List[Message]:
    # Search every topic partition till any messages with appropriate ts are polled
    messages = []
    should_read = True
    while should_read:
        appended = False
        for msg in consumer.consume(topic.poll_timeout, topic.messages_limit, ignore_keys=topic.ignore_keys):
            _, ts = msg.timestamp()
            if ts <= topic.read_till:
                messages.append(msg)
                appended = True
        if appended is False:
            should_read = False
    return messages


def _read_topic_till_end(consumer: AnyConsumer, topic: InputTopic) -> List[Message]:
    # It may be implemented via other strategy: discover maximum offset in topic via subscription to OFFSET_END
    messages = []
    empty_polls_count = 0
    while empty_polls_count <= topic.max_empty_polls:
        msgs = consumer.consume(topic.poll_timeout, topic.messages_limit, ignore_keys=topic.ignore_keys)
        if msgs:
            messages.extend(msgs)
        else:
            empty_polls_count += 1
    return messages
