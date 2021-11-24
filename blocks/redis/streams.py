import time
from typing import Type

from blocks import Event


class _Stream:
    def __init__(self, name: str, event: Type[Event]) -> None:
        self.name = name
        self.event = event

# ToDo (Daniil): Add NoNewEvents generation in consumer
# ToDo (Daniil): Add possibility to store offsets in Redis KV instead of app memory


class InputStream(_Stream):
    def __init__(
        self,
        name: str,
        event: Type[Event],
        *,
        start_id: str = f'{int(time.time() * 1000)}-0',
        messages_limit: int = 1000,
    ) -> None:
        super().__init__(name, event)
        self.messages_limit = messages_limit
        self.start_id = start_id


class OutputStream(_Stream):
    def __init__(self, name: str, event: Type[Event], max_len: int = 1000) -> None:
        super().__init__(name, event)
        self.max_len = max_len
