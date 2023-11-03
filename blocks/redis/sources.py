import time
from typing import List, Optional, Dict, Type, Tuple
from datetime import datetime, timedelta

from redis import Redis
from pydantic import ValidationError

from blocks.types import Event, Source
from blocks.logger import logger
from blocks.redis.serdes import Deserializer, deserialize
from blocks.redis.streams import InputStream


def _make_events(deserialized: List[Dict[str, Optional[bytes]]], codec: Type[Event]) -> List[Event]:
    events = []
    for msg in deserialized:
        try:
            event = codec(**msg)
        except TypeError as err:
            # Although I think there is a better way to check if the input arguments are needed
            if 'takes no arguments' in str(err):
                event = codec()
                for k, v in msg.items():
                    if hasattr(event, k):
                        setattr(event, k, v)
                events.append(event)
            logger.warning(err)
        except ValidationError as err:
            logger.warning(err)
            logger.warning('Deserialized message: {0}'.format(deserialized))
        else:
            events.append(event)
    return events


class RedisConsumer(Source):
    """
    Class represents event source that reads messages from Redis streams
    and wraps them into given events. On initialization must get list
    of input streams in order to perform arbitrary mapping. In most cases
    you don't need to use RedisConsumer directly, RedisStreamsApp can make this for you.

    Example::

      >>> from typing import NamedTuple

      >>> from redis import Redis
      >>> from blocks import Event, Graph
      >>> from blocks.redis import RedisConsumer, InputStream
      >>>
      >>> class MyEvent(NamedTuple):
      ...     x: int
      >>>
      >>> streams = [InputStream('some_stream', MyEvent)]
      >>> client = Redis(...)
      >>> blocks = (RedisConsumer(client, streams, read_timeout=100), ...)
    """

    def __init__(
        self,
        client: Redis,
        streams: List[InputStream],
        read_timeout: int,
        deserializer: Deserializer = deserialize,
    ) -> None:
        self._client = client
        self._codecs = {stream.name: (stream.event, stream.filter_function) for stream in streams}
        self._offsets = {stream.name.encode(): stream.start_id.encode() for stream in streams}
        self._count = min(stream.messages_limit for stream in streams)
        self._last_call: Optional[datetime] = None
        self._read_timeout = read_timeout
        self._deserializer = deserializer

        self.patch_annotations({stream.event for stream in streams})

    @property
    def _can_run(self) -> bool:
        return (
            self._last_call is None or
            (datetime.now() - self._last_call) > timedelta(milliseconds=self._read_timeout)
        )

    def __call__(self) -> List[Event]:
        if not self._can_run:
            to_sleep = timedelta(milliseconds=self._read_timeout) - (datetime.now() - self._last_call)  # type: ignore[operator]
            time.sleep(to_sleep.microseconds / 1000000)
            return []

        data = self._get_messages()
        if data is None:
            return []
        self._last_call = datetime.now()

        return self._unpack_messages(data)

    def _get_messages(self) -> List[Tuple[bytes, List[Tuple[bytes, Dict[bytes, bytes]]]]]:
        try:
            return self._client.xread(self._offsets, self._count)
        except Exception as exc:
            logger.error(exc)
        return []

    def _unpack_messages(self, data: List[Tuple[bytes, List[Tuple[bytes, Dict[bytes, bytes]]]]]) -> List[Event]:
        events = []
        for stream, msgs in data:
            codec, filter_msgs = self._codecs[stream.decode()]
            deserialized = list(map(lambda x: self._deserializer(x[1]), msgs))

            if filter_msgs is not None:
                deserialized = list(filter(filter_msgs, deserialized))

            events.extend(_make_events(deserialized, codec))
            self._offsets[stream] = msgs[-1][0]
        return events

    def close(self) -> None:
        """Graceful shutdown: close redis client."""
        self._client.close()
