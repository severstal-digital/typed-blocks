import logging
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
    High level API for building applications
    with Kafka topics as inputs and possibly outputs.
    Wrap Graph creation and build StreamApp as initialization.
    Also add handlers for common events to the Graph such as
    OpcCommand, ComponentMetric etc. On initialization KafkaApp
    receives list of input and output topics, list of processors
    and save_graph flag with True as default.

    Example::

      >>> # Very dummy replicator
      >>> from pydantic import BaseModel
      >>> from wunderkafka import ConsumerConfig, ProducerConfig
      >>>
      >>> from blocks import Event, processor
      >>> from blocks.kafka import KafkaApp, InputTopic, OutputTopic
      >>>
      >>> class MyEvent(Event):
      ...     x: int
      >>>
      >>> @processor
      ... def printer(e: MyEvent) -> None:
      ...     print(e)
      >>>
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
    ) -> None:
        super().__init__(blocks=[], terminal_event=terminal_event)
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
                logging.warning('{0} is not a valid InputTopic/OutputTopic, ignoring it...'.format(topic))

        if output_topics:
            self._graph.add_block(KafkaProducer(output_topics, producer_config))

        if not input_topics:
            # ToDo (tribunsky.kir): or should we raise Value Error cause it's KafkaApp, not any hybrid graph.
            #                       or we may check other blocks here if there are any sources.
            logger.warning('No input topics detected. Please, make sure, that you have other sources in your code.')
        else:
            source = KafkaSource(input_topics, consumer_config)
            self._graph.add_block(source)
            if CommitEvent in self._graph.outputs:
                self._graph.add_block(OffsetCommitter(source.consumers))

        # ToDo (tribunsky.kir): Great API! Move validation back to the graph
        validate_blocks(self._graph.blocks)
