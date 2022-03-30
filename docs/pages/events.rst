Events by example
=================

Typed-blocks broadly exploits ideas of event-driven development, therefore the library hardly relies on the `Event` at it's core.

In terms of Python programming language, events are just objects, so any python class may be passed as event through the system.

So, there are a plenty of options with different trade-offs, some of which are listed below.

NamedTuple
----------
The most lightweight events may be defined via :code:`NamedTuple`. This way has certain limitations (you can't easily inherit :code:`NamedTuples` to define new subtyped events, for example).

.. code-block :: python

    from typing import NamedTuple

    class E2(NamedTuple):
        x: int

object
------
For many years, creating a class for data structure was a default choice. In this case, it is necessary to define the :code:`__init__`-method. It's also fine option, if you don't trust the input data or have some invariants in your domain.

.. code-block :: python

    from blocks import Event

    class E1(Event):
        def __init__(self, x: int) -> None:
            self.x = x

@dataclass
----------
Nowadays dataclasses are a default choice to handle datastructures. You still may declare :code:`__post_init__`, if you want.

.. code-block :: python

    from blocks import Event

    @dataclass
    class Predict(Event):
        value: float
        key: float
        ts: int

pydantic.BaseModel
------------------
:code:`Pydantic` is great, especially for validations and runtime checks. But nothing is free: it will cost you some memory and CPU overhead. Anyway, you can define events as :code:`BaseModel`, and it actually will work.

.. code-block :: python

    class Signal(BaseModel):
        ts: int = Field(alias='dataTs')
        key: str = Field(alias='dataKey')
        value: str = Field(alias='dataValue')
