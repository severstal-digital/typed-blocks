from queue import Queue
from typing import Any

from blocks import Source


class RestQueueSource(Source):

    def __init__(self, type_event, model_queue: Queue[dict]) -> None:
        self._type = type_event
        self._queue = model_queue
        self.__call__.__annotations__.update({'return': self._type})

    def __call__(self) -> list[Any]:
        data = []
        while not self._queue.empty():
            item = self._queue.get()
            data.append(self._type(**item))
        return data
