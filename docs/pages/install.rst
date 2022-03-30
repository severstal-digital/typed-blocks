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
     - all dependencies will be installed
   * - schedule
     - dependencies for blocks.sources.schedule.Scheduler
   * - kafka
     - dependencies for blocks.kafka.KafkaApp
   * - redis
     - dependencies for blocks.redis.RedisStreamsApp
   * - postgres
     - dependencies for blocks.postgres.PostgresApp
