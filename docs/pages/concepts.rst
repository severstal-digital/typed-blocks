Concepts
========

Despite of many other build-your-app or event sourcing frameworks, you don't need to remember a lot of things. If you are impatient, you may want to check :doc:`quickstart` first.

Three whales of typed-blocks are:

* :code:`Source`
* :code:`Processor`
* :code:`Event`

Any :code:`Source` defines how events appear in your system. It may be any external source, e.g.: queue, database or clocks. Rule of thumb: if you want to *create* an :code:`event` or series of events "from nowhere", you need a :code:`source`.

Any :code:`Processor` should be treated as function (maybe with some internal state), which receives specific events, handles them and, optionally, spawn new events. Rule of thumb: if you want to *do* something with the :code:`event`, or pass the information to the downstream, you need a :code:`processor`.

Any :code:`Event` is just any input and output, which circulates between computing nodes (:code:`processors`). You may also recognize them as *messages*. So, if you need to define, what exactly you want to pass through the system, you need the :code:`event`.

Both sources and processors are essentially *blocks*, from which you can build your computational *graph*. If you heard about graph theory, you may easily imagine *blocks* as nodes (vertices, points) and *events* as edges (links, lines).
