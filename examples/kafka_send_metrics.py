from dataclasses import dataclass

from blocks import processor
from blocks.kafka import OutputTopic, InputTopic, KafkaApp
from blocks.types.process_metrics import AggregatedMetric

from wunderkafka import ConsumerConfig, ProducerConfig


@dataclass
class InputEvent:
    x: int


@dataclass
class OutputEvent:
    y: int


topics = [
    InputTopic(
        name='input_data',
        event=InputEvent
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
        # By default, the interval for aggregation of metrics is 60 seconds, but you can set it any way you want
        metric_time_interval=30
    ).run()
