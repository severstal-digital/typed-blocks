from typing import List, Optional

import requests
from pydantic import parse_obj_as, BaseModel
from requests import Session

from blocks import Event, Source
from blocks.rest.events import InputUrl


class RestSource(Source):
    """
       Class represents event source that reads messages from REST API
       and wraps them into given events.

       Example::

         >>> from blocks import Event
         >>> from pydantic import BaseModel
         >>> from blocks.rest.events import InputUrl

         >>> class EventUrl1(BaseModel):
         ...     name: str
         ...     email: str

         >>> class EventUrl2(BaseModel):
         ...     name: str
         ...     mobile_number: str

         >>>endpoint1 = InputUrl(url="https://example-url1.com", event=EventUrl1)
         >>>endpoint2 = InputUrl(url="https://example-url2.com", event=EventUrl2)

         >>> blocks = (RestSource(endpoints=[endpoint1, endpoint2]), )
       """

    def __init__(
            self,
            endpoints: List[InputUrl],
            session: Optional[Session] = None,
            ignore_errors: bool = True) -> None:
        self.session = session or Session()
        self.endpoints = endpoints
        self.ignore_errors = ignore_errors
        """
        Init RestSource instance.

        :param endpoints:       List of URLs with their corresponding events.
        :param session:         The session object to receive the response. Passed if it is necessary to use one
                                session for different requests.
        :param ignore_errors:   If True, do not fail during casting messages to events.
        """
    def __call__(self) -> List[BaseModel]:
        """
        Emit messages from all URLs as events to the internal queue.

        :return:        Single event or sequence of events.
        """
        events = []
        for endpoint in self.endpoints:
            try:
                response = self.session.get(endpoint.url, timeout=endpoint.timeout)
                response.raise_for_status()
                data = response.json()
                for obj in data:
                    try:
                        events.append(parse_obj_as(endpoint.event, obj))
                    except Exception:
                        if not self.ignore_errors:
                            raise
            except requests.RequestException:
                if not self.ignore_errors:
                    raise

        return events

    def close(self) -> None:
        self.session.close()
