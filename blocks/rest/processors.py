from typing import List, Optional

from pydantic import BaseModel
from requests import Session

from blocks.rest.events import OutputUrl


class RestProducer:
    """
    The class represents an event handler that sends data to RESTful endpoints based on incoming Events.
    Each endpoint can be configured with a specific HTTP method,
    headers, and cookies.

    Example:

      >>> from pydantic import BaseModel
      >>> from typing import NamedTuple
      >>> from blocks.rest.events import OutputUrl

      >>> class MyEvent(BaseModel):
      ...     x: int

      >>> endpoint = OutputUrl(
      ...     url='http://example.com/api/data',
      ...     event=MyEvent,
      ...     method='POST',
      ...     headers={'Content-Type': 'application/json'},
      ...     cookies={'session_id': 'abc123'}
      ... )
      >>> blocks = (RestProducer([endpoint]), ...)
    """
    def __init__(self,
                 endpoints: List[OutputUrl],
                 session: Optional[Session] = None) -> None:
        """
        Init RestProducer instance.

        :param endpoints: A list of URLs with corresponding events.
        :param session: The session object to send the request.
        Used if you need to use one session for different requests.
        """
        self.endpoints = endpoints
        self.session = session or Session()

    def __call__(self,
                 event: BaseModel) -> None:
        for endpoint in self.endpoints:
            if isinstance(event, endpoint.event):
                data = event.dict()
                headers = {**self.session.headers, **endpoint.headers}
                cookies = {**self.session.cookies.get_dict(), **endpoint.cookies}
                method = getattr(self.session, endpoint.method.lower())
                method(endpoint.url, json=data, headers=headers, cookies=cookies)

    def close(self) -> None:
        self.session.close()
