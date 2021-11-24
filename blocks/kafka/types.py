"""This module contains specific event types for Kafka."""

from typing import Dict, List, Optional, NamedTuple

from wunderkafka import AnyConsumer
from confluent_kafka import TopicPartition

from blocks.logger import logger
from blocks.kafka.topics import InputTopic

# ToDo (tribunsky.kir): make immutable?
ConsumersMapping = Dict[InputTopic, AnyConsumer]


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
