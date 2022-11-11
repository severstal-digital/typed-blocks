import asyncio
from dataclasses import dataclass

from blocks import App, AsyncSource, AsyncProcessor, async_source, async_processor


@dataclass
class E:
    x: int


@dataclass
class E2:
    y: int


@async_source
async def gen() -> E:
    await asyncio.sleep(0.1)
    return E(1)


@async_processor
async def printer(e: E) -> E2:
    print('async_processor', e)
    return E2(e.x)


class Gen(AsyncSource):
    async def __call__(self) -> E:
        await asyncio.sleep(0.2)
        return E(2)


class Printer(AsyncProcessor):
    async def __call__(self, event: E) -> E2:
        print('AsyncProcessor', event)
        return E2(event.x)


class Receiver(AsyncProcessor):
    def __init__(self) -> None:
        self.events = []

    async def __call__(self, event: E2) -> None:
        self.events.append(event)


def test_smoke_async_app() -> None:
    loop = asyncio.get_event_loop()
    receiver = Receiver()
    app = App(blocks=(gen(), Gen(), printer(), Printer(), receiver))
    loop.run_until_complete(app.run_async(once=True))
    assert len(receiver.events) == 4
