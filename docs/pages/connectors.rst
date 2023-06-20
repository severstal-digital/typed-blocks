Connectors
==========

Kafka
-----

Topics
^^^^^^

.. autoclass:: blocks.kafka.topics.InputTopic
   :members:
   :special-members:
   :exclude-members: __weakref__

.. autoclass:: blocks.kafka.topics.OutputTopic
   :members:
   :special-members:
   :exclude-members: __weakref__

.. autoclass:: blocks.kafka.topics.CommitChoices
   :members:

Events
^^^^^^

.. automodule:: blocks.kafka.events
   :members:

App
^^^

.. autoclass:: blocks.kafka.app.KafkaApp
   :members:
   :inherited-members:
   :special-members:
   :exclude-members: __weakref__, run_async

Sources
^^^^^^^

.. automodule:: blocks.kafka.sources
   :members:
   :special-members:
   :exclude-members: __weakref__

Processors
^^^^^^^^^^

.. automodule:: blocks.kafka.processors
   :members:
   :special-members:
   :exclude-members: __weakref__

Redis Streams
-------------

App
^^^

.. autoclass:: blocks.redis.app.RedisStreamsApp
   :members:
   :inherited-members:
   :special-members:
   :exclude-members: __weakref__, run_async

Sources
^^^^^^^

.. automodule:: blocks.redis.sources
   :members:

Processors
^^^^^^^^^^

.. automodule:: blocks.redis.processors
   :members:

Databases-specific events
-------------------------

.. automodule:: blocks.db.types
   :members:

Postgres
--------

App
^^^

.. autoclass:: blocks.postgres.app.PostgresApp
   :members:
   :inherited-members:
   :special-members:
   :exclude-members: __weakref__, run_async

Sources
^^^^^^^

.. automodule:: blocks.postgres.sources
   :members:
   :special-members:
   :exclude-members: __weakref__

Processors
^^^^^^^^^^

.. automodule:: blocks.postgres.processors
   :members:
   :special-members:
   :exclude-members: __weakref__

Hive
--------

Sources
^^^^^^^

.. automodule:: blocks.hive.sources
   :members:
   :special-members:
   :exclude-members: __weakref__

Processors
^^^^^^^^^^

.. automodule:: blocks.hive.processors
   :members:
   :special-members:
   :exclude-members: __weakref__
