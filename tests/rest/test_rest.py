import pytest

try:
    from pydantic import BaseModel
except ImportError:
    pytest.skip("skipping pydantic-only tests", allow_module_level=True)

from requests import Response
from _pytest.monkeypatch import MonkeyPatch

from blocks.rest.events import InputUrl, OutputUrl
from blocks.rest.sources import RestSource
from blocks.rest.processors import RestProducer


class EventUrl1(BaseModel):
    name: str
    email: str

class EventUrl2(BaseModel):
    name: str
    mobile_number: int

@pytest.fixture(scope="session")
def source() -> RestSource:

    endpoint1 = InputUrl(url="https://example-url1.com", event=EventUrl1)
    endpoint2 = InputUrl(url="https://example-url2.com", event=EventUrl2)

    return RestSource(endpoints=[endpoint1, endpoint2])


@pytest.fixture(scope="session")
def producer() -> RestProducer:

    endpoint1 = OutputUrl(url="https://example-url1.com", event=EventUrl1, headers={"test_header": "header"})

    return RestProducer(endpoints=[endpoint1])


def test_rest_source(source: RestSource, monkeypatch: MonkeyPatch) -> None:

    def mock_get(*args, **kwargs) -> Response:
        response = Response()
        response.status_code = 200
        response._content = b'[{"name": "Sheburashka", "email": "sheburashka@severstal.com"}, ' \
                            b'{"name": "Gena_Crocodile", "mobile_number": "911"}]'
        return response

    monkeypatch.setattr("requests.Session.get", mock_get)

    events = source()
    assert len(events) == 2
    assert events[0].name == "Sheburashka"
    assert events[0].email == "sheburashka@severstal.com"
    assert events[1].name == "Gena_Crocodile"
    assert events[1].mobile_number == 911

def test_rest_processors(producer: RestProducer, monkeypatch: MonkeyPatch) -> None:
    test_event = EventUrl1(name="VinniPuh", email="vinnipuh@severstal.com")
    list_for_cheak = []
    def mock_response(*args, **kwargs):
        list_for_cheak.append(kwargs)

    monkeypatch.setattr("requests.Session.post", mock_response)

    producer(test_event)

    assert list_for_cheak[0]["json"]["name"] == 'VinniPuh'
    assert list_for_cheak[0]["json"]["email"] == "vinnipuh@severstal.com"
    assert list_for_cheak[0]["headers"]["test_header"] == "header"