import time
from typing import List, Optional
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

    # ToDo (tribunsky.kir): complexity? Author didn't even care about complexity.
    def __call__(self) -> List[Event]:
        events = []
        if self._last_call is None or (datetime.now() - self._last_call) > timedelta(milliseconds=self._read_timeout):
            try:
                all_data = self._client.xread(self._offsets, self._count)
            except Exception as exc:
                logger.error(exc)
            else:
                self._last_call = datetime.now()
                for stream_name, msgs in all_data:
                    for offset, message in msgs:
                        filter_message = self._codecs[stream_name.decode()][1]
                        deserialized = self._deserializer(message)
                        codec = self._codecs[stream_name.decode()][0]
                        try:
                            event = codec(**deserialized)
                        except TypeError as err:
                            if 'takes no arguments' in str(err):
                                event = codec()
                                for k, v in deserialized.items():
                                    if hasattr(event, k):
                                        setattr(event, k, v)
                                if filter_message(event):
                                    events.append(event)
                            logger.warning(err)
                        except ValidationError as err:
                            logger.warning(err)
                            logger.warning('Deserialized message: {0}'.format(deserialized))
                        else:
                            if filter_message(event):
                                events.append(event)
                        self._offsets[stream_name] = offset
        else:
            to_sleep = timedelta(milliseconds=self._read_timeout) - (datetime.now() - self._last_call)
            time.sleep(to_sleep.microseconds / 1000000)
        return events

    def close(self) -> None:
        """Graceful shutdown: close reids client."""
        self._client.close()
