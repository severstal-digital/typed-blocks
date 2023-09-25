Installation
============
In minimal installation typed-blocks requires no dependencies, so you may just use: ::

$ pip install typed-blocks

But you may want to install typed-blocks with various features. In that case you need to pass desired extras: ::

$ pip install typed-blocks[<extra_name_1>, <extra_name_2>]

Available extras
----------------

.. list-table::
   :widths: 10 90
   :header-rows: 1

   * - extra
     - comment
   * - full
     - all possible dependencies will be installed
   * - schedule
     - dependencies for :code:`blocks.sources.schedule.Scheduler`
   * - kafka
     - dependencies for :code:`blocks.kafka.KafkaApp`
   * - redis
     - dependencies for :code:`blocks.redis.RedisStreamsApp`
   * - postgres
     - dependencies for :code:`blocks.postgres.PostgresApp`
   * - mssql
     - dependencies for :code:`blocks.mssql.MssqlApp`
   * - hive
     - dependencies for :code:`blocks.hive.HiveReader` and :code:`blocks.hive.HiveWriter`
   * - visualization
     - dependencies for :code:`blocks.graph.Graph`
   * - graphviz
     - dependencies for :code:`blocks.graph.Graph`
   * - parallel
     - dependencies for :code:`@parallel_processor` or for marking :code:`Processor` as separate process
   * - size-metric
     - dependencies for :code:`blocks.runners.Runner` to collect object size metrics
