import time
from typing import Type, Callable, Dict, Any, Optional

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
        filter_function: Callable[[Event], bool] = lambda x: True,
        start_id: str = f'{int(time.time() * 1000)}-0',
        messages_limit: int = 1000,
    ) -> None:
        """
        Added the ability to filter events from the topic. Example:

        def filter_func(value: Any) -> bool:
            return ...

        streams = [InputStream(name='test', event=SignalP, filter_function=filter_func)]
        """
        super().__init__(name, event)
        self.messages_limit = messages_limit
        self.start_id = start_id
        self.filter_function = filter_function



class OutputStream(_Stream):
    def __init__(self, name: str, event: Type[Event], max_len: int = 1000) -> None:
        super().__init__(name, event)
        self.max_len = max_len
