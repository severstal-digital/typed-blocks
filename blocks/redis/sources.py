import time
from typing import List, Union, Optional
from datetime import datetime, timedelta

from redis import Redis
from pydantic import ValidationError

from blocks.types import Event, Source
from blocks.logger import logger
from blocks.redis.serdes import Deserializer, deserialize
from blocks.redis.streams import InputStream


class RedisConsumer(Source):
    """
    Class represents event source that reads messages from Redis streams
    and wraps them into given events. On initialization must get list
    of input streams in order to perform arbitrary mapping. In most cases
    you don't need to use RedisConsumer directly, RedisApp can make this for you.

    Example::

      >>> from redis import Redis
      >>> from blocks import Event, Graph
      >>> from blocks.redis import RedisConsumer, InputStream
      >>>
      >>> class MyEvent(Event):
      ...     x: int
      >>>
      >>> streams = [InputStream('some_stream', MyEvent)]
      >>> graph = Graph()
      >>> client = Redis(...)
      >>> graph.add_block(RedisConsumer(client, streams, read_timeout=100)
    """

    def __init__(
        self,
        client: Redis,
        streams: List[InputStream],
        read_timeout: int,
        deserializer: Deserializer = deserialize,
    ) -> None:
        self._client = client
        self._codecs = {stream.name: stream.event for stream in streams}
        self._offsets = {stream.name.encode(): stream.start_id.encode() for stream in streams}
        self._count = min(stream.messages_limit for stream in streams)
        self._last_call: Optional[datetime] = None
        self._read_timeout = read_timeout
        self._deserializer = deserializer

        out_annots = {stream.event for stream in streams}
        self.__call__.__annotations__['return'] = List[Union[tuple(out_annots)]]  # type: ignore

    # ToDo (tribunsky.kir): complexity? Author didn't even care about complexity.
    def __call__(self) -> List[Event]:
        events = []
        if self._last_call is None or (datetime.now() - self._last_call) > timedelta(milliseconds=self._read_timeout):
            try:
                all_data = self._client.xread(self._offsets, self._count)  # type: ignore
                self._last_call = datetime.now()
            except Exception as exc:
                logger.error(exc)
            else:
                for stream_name, msgs in all_data:
                    for offset, message in msgs:
                        deserialized = self._deserializer(message)
                        codec = self._codecs[stream_name.decode()]
                        try:
                            events.append(codec(**deserialized))
                        except ValidationError as err:
                            logger.warning(err)
                            logger.warning('Deserialized message: {0}'.format(deserialized))
                        self._offsets[stream_name] = offset
        else:
            to_sleep = timedelta(milliseconds=self._read_timeout) - (datetime.now() - self._last_call)
            time.sleep(to_sleep.microseconds / 1000000)
        return events

    def close(self) -> None:
        self._client.close()
