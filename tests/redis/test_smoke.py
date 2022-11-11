from typing import Dict, List, Tuple, Union, Optional, NamedTuple
from dataclasses import dataclass

import pytest

try:
    from redis import Redis
except ImportError:
    pytest.skip("skipping redis-only tests", allow_module_level=True)
from pytest import approx
from pydantic import BaseModel

from blocks.redis import InputStream, RedisConsumer


class RedisStub(Redis):
    def xread(
        self,
        streams: Dict[bytes, bytes],
        count: Optional[int] = None,
        block: Optional[int] = None,
    ) -> List[List[Union[bytes, List[Tuple[bytes, Dict[bytes, bytes]]]]]]:
        return [[b'test', [(b'1667229601456-0', {b'id': b'181489903', b'value': b'0.0023', b'flag': b'false'})]]]


class SignalNT(NamedTuple):
    id: int
    value: float
    flag: bool


class SignalO(object):
    id: int
    value: float
    flag: bool


class SignalOI(object):
    def __init__(self, id: int, value: float, flag: bool) -> None:
        self.id = id
        self.value = value
        self.flag = flag


@dataclass
class SignalDC:
    id: int
    value: float
    flag: bool


class SignalP(BaseModel):
    id: int
    value: float
    flag: bool


# ToDo (tribunsky.kir): currently only pydantic-based events work out-of-the box.
#                       for enlightened events decoding is necessary or client with decode_responses=True may help.
test_types = [
    # SignalNT,
    # SignalO,
    # SignalOI,
    # SignalDC,
    SignalP,
]


@pytest.fixture
def client() -> Redis:
    return RedisStub()


@pytest.mark.parametrize('cls', test_types)
def test_smoke_event_creation(cls, client) -> None:
    streams = [InputStream(name='test', event=cls)]
    source = RedisConsumer(client, streams, read_timeout=100)

    events = source()
    assert events
    for event in events:
        assert isinstance(event, cls)
        assert event.id == 181489903
        assert event.value == approx(0.0023)
        assert event.flag is False
