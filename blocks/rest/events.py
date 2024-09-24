from typing import Type, Optional

from pydantic import BaseModel


class InputUrl(BaseModel):
    url: str
    event: Type[BaseModel]
    timeout: float = 5.0


class OutputUrl:
    def __init__(self,
                 url: str,
                 event: Type[BaseModel],
                 method: str = "POST",
                 headers: Optional[dict] = None,
                 cookies: Optional[dict] = None) -> None:
        self.url = url
        self.event = event
        self.method = method
        self.headers = headers if headers is not None else {}
        self.cookies = cookies if cookies is not None else {}
