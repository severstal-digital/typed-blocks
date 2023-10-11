from dataclasses import dataclass
from typing import List, Dict, Any

from blocks import processor
from blocks.kafka import OutputTopic, InputTopic, KafkaApp
from blocks.types.metrics import AggregatedMetric

from wunderkafka import ConsumerConfig, ProducerConfig, Message


@dataclass
class InputEvent:
    x: int


@dataclass
class OutputEvent:
    y: int


def filter_messages(msg: Dict[str, Any]) -> bool:
    # The input data is the dictionary from the Message (confluent-kafka)
    return msg['x'] > 80


topics = [
    InputTopic(
        name='input_data',
        event=InputEvent,
        filter_function=filter_messages
    ),

    OutputTopic(
        name='output_data',
        event=OutputEvent
    ),
    OutputTopic(
        name='aggregated_metrics',
        event=AggregatedMetric
    )
]


@processor
def transformer(e: InputEvent) -> OutputEvent:
    print(e)
    return OutputEvent(y=e.x)


if __name__ == '__main__':
    consumer_config = ConsumerConfig(...)
    producer_config = ProducerConfig(...)

    blocks = (transformer())
    KafkaApp(
        topics,
        blocks,
        consumer_config,
        producer_config,
    ).run()
