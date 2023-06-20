# typed-blocks

---

**Documentation**: [https://typed-blocks.readthedocs.io](https://typed-blocks.readthedocs.io)

---

_Typed Blocks_ is a modular event-centric python library created to simplify development of typical stream applications. It hardly exploits standard Python type hints.

Distinctive features:
- Boilerplate-less: just ties together pre-defined computational _blocks_.
- Focuses on loose coupling and event-driven design.
- Encourages explicit separation of data and code. Static typing to the rescue!
- Minimalistic at it's core and easy-to-go.
- Modular and open for extension via self-defined connectors for different data sources.

## Installation

In minimal installation typed-blocks requires python 3.7 or greater and has no dependencies, so you may just use:
```bash
$ pip install typed-blocks
````

For each data source or extended features you may need corresponding client library. More details in [documentation](https://typed-blocks.readthedocs.io/).

## Example

If it is hard to understand, what is going on in the following snippet of code, [this article](https://typed-blocks.readthedocs.io/en/stable/pages/concepts.html) also may help.

TL;DR:
- we are defining `events`: minimally significant pieces of data  
- we are defining `source` of all events in our program
- we define `processors` to handle them
- we put all ingredients in the `App`. Type hints are important! It's actually definition of which `processor` should handle `event` and should it emit new `events` or not.
- ???
- it works!

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
