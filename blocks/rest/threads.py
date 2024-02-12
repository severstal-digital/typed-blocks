from queue import Queue
from threading import Thread
from time import sleep

import requests


class RestParser(Thread):
    def __init__(self, host: str, queue_ids: Queue[str], out_queue: Queue[dict]) -> None:
        super().__init__(daemon=True)
        self._host = host
        self._queue = queue_ids
        self._out = out_queue

        self._run = True

    def run(self) -> None:
        while self._run:
            if not self._queue.empty():
                item = self._queue.get()
                while not self._check_status(item):
                    sleep(1)
                new_resp = self._get_result(item)
                self._out.put(new_resp)

    def stop(self) -> None:
        self._run = False
        sleep(1)

    def _get_result(self, request_id: str) -> dict:
        resp = requests.get(self._host, params={'request_id': request_id})
        return resp.json()['data']

    def _check_status(self, request_id: str) -> bool:
        try:
            # todo: create backoff and skip if no answer by 10 tries
            resp = requests.get(self._host, params={'request_id': request_id})
            rj = resp.json()
            if rj['status'] == 'complete':
                return True
        except Exception:
            return False
