from queue import Queue
from typing import Type

import requests

from blocks import Processor


class RestProcessor(Processor):
    """
    Processor for send events to model
    """
    def __init__(
        self,
        input_event: Type,
        url_host: str,
        ids_queue: Queue
    ) -> None:
        self._url_host = url_host
        self._queue = ids_queue
        self.__call__.__annotations__.update({'event': input_event})
        # todo: validate event to convert dict (or as_dict())

    def __call__(self, event) -> None:
        try:
            # fixme: need return response id query
            # todo: maybe redis? not http requests
            resp = requests.post(self._url_host, json=dict(event))
            self._queue.put(resp.json()['request_id'])
        except Exception:
            pass
        else:
            pass
            # todo: create persist cache like redis
