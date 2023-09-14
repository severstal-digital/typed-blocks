from typing import Dict, List, Type, Union

from redis import Redis

from blocks import Event, Processor
from blocks.redis.events import event2dict
from blocks.redis.serdes import Serializer, serialize
from blocks.redis.streams import OutputStream


class RedisProducer(Processor):
    """
    Class represents event processor that writes messages to Redis streams
    based on data received from event. On initialization must get list
    of output streams in order to perform arbitrary mapping. In most cases
    you don't need to use RedisProducer directly, RedisStreamsApp can make this for you.

    Example::

      >>> from typing import NamedTuple

      >>> from redis import Redis
      >>> from blocks import Event, Graph
      >>> from blocks.redis import RedisProducer, OutputStream

      >>> class MyEvent(NamedTuple):
      ...     x: int

      >>> streams = [OutputStream('some_stream', MyEvent)]
      >>> client = Redis(...)
      >>> blocks = (RedisProducer(client, streams), ...)
    """

    def __init__(
        self,
        client: Redis,
        streams: List[OutputStream],
        serializer: Serializer = serialize
    ) -> None:
        self._client = client
        self._serializer = serializer

        self._streams: Dict[Type[Event], OutputStream] = {stream.event: stream for stream in streams}
        self.__call__.__annotations__.update(
            {
                'event': Union[tuple(stream.event for stream in streams)],
            },
        )

    def __call__(self, event: Event) -> None:
        stream = self._streams[type(event)]
        serialized = self._serializer(event2dict(event))
        self._client.xadd(stream.name, fields=serialized, maxlen=stream.max_len)

    def close(self) -> None:
        """Graceful shutdown: close reids client."""
        self._client.close()
