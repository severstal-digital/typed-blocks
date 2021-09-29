# typed-blocks
```
from dataclasses import dataclass

from blocks.core.app import App
from blocks.core.decorators import source, processor


@dataclass
class E:
    x: int


@dataclass
class E2:
    y: int


@source
def generator() -> E:
    return E(1)


@processor
def printer(e: E) -> E2:
    print('1', e)
    return E2(e.x)


@processor
def printer2(e: E2) -> None:
    print('2', e)


blocks = (generator(), printer(), printer2())
App(blocks).run(once=True)
```