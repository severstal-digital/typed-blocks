import sys
import random
from time import time

from blocks.annotations import get_output_events_type
from blocks.compat import HAS_PYDANTIC

if sys.version_info >= (3, 8):
    from typing import Protocol, Union
else:
    from typing_extensions import Protocol

from typing import Any, Dict, List, Optional, NamedTuple
from dataclasses import dataclass

import pytest

try:
    import wunderkafka
except ImportError:
    pytest.skip("skipping kafka-only tests", allow_module_level=True)

from wunderkafka import AvroConsumer
from confluent_kafka import KafkaError

from blocks.kafka import InputTopic
from blocks.kafka.topics import ConsumerFactory
from blocks.kafka.sources import AnyConsumer, KafkaSource, ConsumerConfig


class Message(Protocol):
    def value(self) -> Dict[str, Any]:
        ...

    def key(self) -> Optional[Dict[str, Any]]:
        ...

    def topic(self) -> Optional[str]:
        ...

    def partition(self) -> Optional[int]:
        ...

    def offset(self) -> Optional[int]:
        ...

    def error(self) -> Optional[KafkaError]:
        ...


class MessageMock(Message):
    def __init__(
        self,
        topic: str,
        value: Dict[str, Any],
        key: Optional[Dict[str, Any]] = None,
        error: Optional[KafkaError] = None,
    ) -> None:
        self._topic = topic
        self._value = value
        self._key = key
        self._error = error

    def value(self) -> Dict[str, Any]:
        return self._value

    def topic(self) -> Optional[str]:
        return self._topic


class ConsumerStub(AnyConsumer):

    def __init__(self, config: ConsumerConfig):
        super().__init__(config)

    def consume(
        self,
        timeout: float = 1.0,
        num_messages: int = 1000000,
        *,
        ignore_keys: bool = False,
    ) -> List[Message]:
        max_value = 9000
        messages = []
        for _ in range(10):
            msg = {'id': random.randint(-max_value, max_value), 'value': random.uniform(-max_value, max_value)}
            messages.append(MessageMock(topic='test', value=msg))
        messages.append(MessageMock(topic='test_filter', value={'id': 1, 'value': 10000}))
        messages.append(MessageMock(topic='test_filter', value={'id': 2, 'value': 10001}))
        messages.append(MessageMock(topic='test_filter', value={'id': 3, 'value': 10002}))
        return messages


class SignalNT(NamedTuple):
    id: int
    value: float


class SignalO(object):
    id: int
    value: float


class SignalOI(object):
    def __init__(self, id: int, value: float) -> None:
        self.id = id
        self.value = value


@dataclass
class SignalDC:
    id: int
    value: float


class BatchSignal(NamedTuple):
    events: List[SignalNT]


test_types = [
    SignalNT,
    SignalO,
    SignalOI,
    SignalDC,
]

if HAS_PYDANTIC:
    from pydantic import BaseModel


    class SignalP(BaseModel):
        id: int
        value: float


    test_types.append(SignalP)


@pytest.mark.parametrize('cls', test_types)
def test_smoke_event_creation(cls) -> None:
    topics = [InputTopic(name='test', event=cls)]
    source = KafkaSource(topics, ConsumerConfig(group_id='test'), cls=ConsumerStub, ignore_errors=False)

    events = source()
    assert events
    assert all(isinstance(event, cls) for event in events)


def filter_func(value: Union[SignalNT, SignalO, SignalOI, SignalDC]) -> bool:
    return value.value >= 10000

@pytest.mark.parametrize('cls', (SignalNT, SignalOI, SignalDC))
def test_smoke_event_creation_filter(cls) -> None:
    topics = [InputTopic(name='test', event=cls, filter_function=filter_func)]
    source = KafkaSource(topics, ConsumerConfig(group_id='test'), cls=ConsumerStub, ignore_errors=False)

    events = source()
    assert events
    assert len(events) == 3


def test_smoke_namedtuple_event_topic_override() -> None:
    topics = [InputTopic(name='test', event=SignalNT, consumer=ConsumerFactory(cls=AvroConsumer))]
    with pytest.raises(ValueError):
        KafkaSource(topics, ConsumerConfig(group_id='test'), cls=ConsumerStub)

@pytest.mark.parametrize("event,batch_event,till", [
    (SignalNT, BatchSignal, int(time() * 1000)),
    (SignalNT, BatchSignal, None),
])
def test_batch_annotations(event, batch_event, till) -> None:
    topics = [InputTopic(name='test', event=event, batch_event=batch_event, read_till=till)]
    source = KafkaSource(topics, ConsumerConfig(group_id='test'), cls=ConsumerStub, ignore_errors=False)
    events = get_output_events_type(source)
    assert len(events) == 1
    assert events[0] is BatchSignal
