from typing import Type, Union, Optional, Sequence

from redis import Redis

from blocks import App, Block, Event
from blocks.validation import validate_blocks
from blocks.redis.serdes import Serializer, Deserializer, serialize, deserialize
from blocks.redis.sources import RedisConsumer
from blocks.redis.streams import InputStream, OutputStream
from blocks.redis.processors import RedisProducer


# ToDo (tribunsky.kir): from now it looks like GREAT copypaste of KafkaApp,
#                       so, maybe it makes sense to start using metaclasses here
class RedisStreamsApp(App):
    """
    High level API for building applications with redis streams as inputs and outputs.

    Wraps Graph creation and builds RedisStreamsApp.

    Example::

      >>> from typing import NamedTuple

      >>> from redis import Redis
      >>> from blocks import Event, processor
      >>> from blocks.redis import RedisStreamsApp, InputStream

      >>> class MyEvent(NamedTuple):
      ...     x: int

      >>> @processor
      ... def printer(e: MyEvent) -> None:
      ...     print(e)

      >>> streams = [InputStream('some_stream', MyEvent)]
      >>> blocks = [printer()]
      >>> redis = Redis(host=..., port=..., db=..., password=...)
      >>> RedisStreamsApp(streams, blocks, redis).run()
    """

    # ToDo (tribunsky.kir): maybe it is worth to add serdes per stream.
    def __init__(
        self,
        streams: Sequence[Union[InputStream, OutputStream]],
        blocks: Sequence[Block],
        redis_client: Redis,
        read_timeout: int = 100,
        terminal_event: Optional[Type[Event]] = None,
        serializer: Serializer = serialize,
        deserializer: Deserializer = deserialize,
        collect_metric: bool = False,
        *,
        metric_time_interval: int = 60
    ) -> None:
        super().__init__(
            blocks=[],
            terminal_event=terminal_event, collect_metric=collect_metric,
            metric_time_interval=metric_time_interval
        )
        for block in blocks:
            self._graph.add_block(block)

        input_streams = [stream for stream in streams if isinstance(stream, InputStream)]
        output_streams = [stream for stream in streams if isinstance(stream, OutputStream)]

        if input_streams:
            self._graph.add_block(RedisConsumer(redis_client, input_streams, read_timeout, deserializer=deserializer))

        if output_streams:
            self._graph.add_block(RedisProducer(redis_client, output_streams, serializer=serializer))

        validate_blocks(self._graph.blocks)
