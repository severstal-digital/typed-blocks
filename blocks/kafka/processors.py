"""This module contains Kafka-specific processors."""

from typing import Any, Dict, List, Type, Union, Optional

from wunderkafka import AnyProducer, ProducerConfig, AvroModelProducer

from blocks.types import Event, Processor
from blocks.logger import logger
from blocks.kafka.types import ConsumersMapping
from blocks.kafka.events import CommitEvent
from blocks.kafka.topics import OutputTopic


def _assign_producers(
    topics: List[OutputTopic],
    config: Optional[ProducerConfig],
    cls: Type[AnyProducer],
) -> Dict[Type[Event], AnyProducer]:
    if len({topic.event for topic in topics}) != len(topics):
        raise ValueError('All events types should be unique!')

    instances: Dict[Type[AnyProducer], AnyProducer] = {}
    producers: Dict[Type[Event], AnyProducer] = {}

    for topic in topics:
        cfg = topic.producer.conf or config
        if cfg is None:
            raise ValueError('Consumer configuration must be set at least for all topic or per every topic.')
        producer_cls = topic.producer.type or cls
        instance = instances.get(producer_cls)
        if instance is None:
            instance = producer_cls(mapping=None, config=cfg)
            instances[producer_cls] = instance

        producers[topic.event] = instance
        instance.set_target_topic(topic.name, topic.event, topic.key)

    return producers


class KafkaProducer(Processor):
    """
    Class represents event processor that writes messages to Kafka topics
    based on data received from event. On initialization must get list
    of output topics in order to perform arbitrary mapping. In most cases
    you don't need to use KafkaProducer directly, KafkaApp can make this for you.

    Example::

      >>> from typing import NamedTuple

      >>> from blocks import Event, Graph
      >>> from blocks.kafka import KafkaProducer, OutputTopic

      >>> class MyEvent(NamedTuple):
      ...     x: int

      >>> topics = [OutputTopic('some_topic', MyEvent)]

      >>> blocks = (KafkaProducer(topics), ...)
    """

    def __init__(
        self,
        topics: List[OutputTopic],
        config: Optional[ProducerConfig],
        cls: Type[AnyProducer] = AvroModelProducer,
    ) -> None:
        """
        Init KafkaProducer instance.

        :param topics:      Topic to send messages in.
        :param config:      Failover configuration for producer if there is no configuration per InputTopic.
        :param cls:         Failover factory to create producer if there is no configuration per InputTopic.
        """
        self._producers: Dict[Type[Any], AnyProducer] = _assign_producers(topics, config, cls)
        self._topics: Dict[Type[Any], str] = {topic.event: topic.name for topic in topics}

        self.__call__.__annotations__.update(
            {
                'event': Union[tuple(topic.event for topic in topics)],
            },
        )

    def __call__(self, event: Event) -> None:
        """
        Send event-based message to corresponding topic.

        :param event:       Specific event to be serialized and sent to kafka topic.
        """
        key = vars(event).pop('@key', None)
        event_type = type(event)
        topic = self._topics[event_type]
        self._producers[event_type].send_message(topic, event, key)

    def close(self) -> None:
        """Graceful shutdown: flush all producers."""
        for producer in self._producers.values():
            # ToDo (tribunsky.kir): mimic/make public
            producer.flush()


# ToDo (dp.zubakin): handle corner cases, e.g.
#                    - Consistency: we may not commit offset due to the crash, but write result from this part of work.
#                                   So there may be problems with duplications in the next part of the pipeline
#                    - And vise versa, if we commit offset, but result sending failed.
class OffsetCommitter(Processor):
    """
    Class represents CommitEvent event processor that performs manual
    offset commit to list of given Kafka topics. On initialization must get
    refrence on dict of input topics to consumer mapping. In most cases
    you don't need to use OffsetCommitter directly, KafkaApp can make this for you.

    Example::

      >>> from blocks import Event
      >>> from blocks.kafka import KafkaSource, OffsetCommitter

      >>> topics = [...]
      >>> consumer = KafkaSource(topics)
      >>> committer = OffsetCommitter(consumer.consumers)

      >>> blocks = (consumer, committer, ...)
    """

    def __init__(self, consumers: ConsumersMapping) -> None:
        self._consumers = {topic.name: consumer for topic, consumer in consumers.items()}

    def __call__(self, event: CommitEvent) -> None:
        msg_metadata = vars(event.e).get('@msg')
        # ToDo (tribunsky.kir): thus, it is more reliable to put meta on EVERY message.
        #                       As we do not control user's CommitEvent usage, it may misbehave
        if msg_metadata is None:
            logger.warning('Got nothing to commit. Please check your commit_offset option for {0}'.format(event.e))
            return None
        to_commit = msg_metadata.to_commit()
        logger.debug('Really committing! {0} | {1}'.format(msg_metadata, to_commit))
        self._consumers[msg_metadata.topic].commit(offsets=to_commit)
