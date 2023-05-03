"""This module contains some anti-boilerplate stuff for a typical Kafka-Kafka stream application."""

from typing import Type, Union, Optional, Sequence

from wunderkafka import ConsumerConfig, ProducerConfig

from blocks import App, Block, Event
from blocks.logger import logger
from blocks.validation import validate_blocks
from blocks.kafka.topics import InputTopic, OutputTopic
from blocks.kafka.sources import KafkaSource
from blocks.kafka.processors import CommitEvent, KafkaProducer, OffsetCommitter

Topic = Union[InputTopic, OutputTopic]


class KafkaApp(App):
    """
    High level API for building applications with Kafka topics as inputs and outputs.

    Also adds handlers for common events for kafka-specific things like commiting offsets.

    Example::

      >>> # Very dummy replicator
      >>> from pydantic import BaseModel
      >>> from wunderkafka import ConsumerConfig, ProducerConfig

      >>> from blocks import Event, processor
      >>> from blocks.kafka import KafkaApp, InputTopic, OutputTopic

      >>> class MyEvent(BaseModel):
      ...     x: int

      >>> @processor
      ... def printer(e: MyEvent) -> None:
      ...     print(e)

      >>> consumer_config = ConsumerConfig(...)
      >>> producer_config = ProducerConfig(...)
      >>> topics = [
      ...     InputTopic('my_topic', MyEvent, from_beginning=False),
      ...     OutputTopic('my_other_topic', MyEvent),
      ... ]
      >>> blocks = (printer(),)
      >>> KafkaApp(topics, blocks, consumer_config, producer_config).run()
    """

    def __init__(
        self,
        topics: Sequence[Topic],
        blocks: Sequence[Block],
        consumer_config: Optional[ConsumerConfig] = None,
        producer_config: Optional[ProducerConfig] = None,
        terminal_event: Optional[Type[Event]] = None,
        collect_metric: bool = False,
        *,
        metric_time_interval: int = 60
    ) -> None:
        """
        Initiate Kafka app instance.

        :param topics:                  Input/OutputTopic to work with.
        :param blocks:                  Additional blocks with business logic, other events sources or just processors
                                        to make effects.
        :param consumer_config:         Default consumer config to be applied, if not defined per InputTopic.
        :param producer_config:         Default producer config to be applied, if not defined per OutputTopic.
        :param terminal_event:          If specified, special event to notify app to stop receiving events and close all
                                        blocks properly.

        :param collect_metric:          Flag responsible for collecting metrics. If the flag is True,
                                        the metrics will be collected
        :param metric_time_interval:    Time interval for metric aggregation.

        """
        super().__init__(
            blocks=[],
            terminal_event=terminal_event,
            metric_time_interval=metric_time_interval,
            collect_metric=collect_metric
        )
        for block in blocks:
            self._graph.add_block(block)

        input_topics = []
        output_topics = []
        for topic in topics:
            # ToDo (tribunsky.kir): check that there is no multi-inherited InputOutputTopic?
            if isinstance(topic, InputTopic):
                input_topics.append(topic)
            elif isinstance(topic, OutputTopic):
                output_topics.append(topic)
            else:
                logger.warning('{0} is not a valid InputTopic/OutputTopic, ignoring it...'.format(topic))

        if output_topics:
            self._graph.add_block(KafkaProducer(output_topics, producer_config))

        if input_topics:
            source = KafkaSource(input_topics, consumer_config)
            self._graph.add_block(source)
            if CommitEvent in self._graph.outputs:
                self._graph.add_block(OffsetCommitter(source.consumers))
        else:
            # ToDo (tribunsky.kir): or should we raise Value Error cause it's KafkaApp, not any hybrid graph.
            #                       or we may check other blocks here if there are any sources.
            logger.warning('No input topics detected. Please, make sure, that you have other sources in your code.')

        # ToDo (tribunsky.kir): "Great" API! Move validation back to the graph
        validate_blocks(self._graph.blocks)
