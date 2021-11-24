# typed-blocks

Modular event-centric python library made for simplification typical stream applications development with python type system strong exploitation.

```python
from dataclasses import dataclass

from blocks import App, source, processor


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