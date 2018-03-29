.. _ksql-server-config:

Configuring KSQL Server
=======================

.. toctree:: Contents
    :maxdepth: 1

    security
    ccloud-ksql
    avro-schema

Configuration parameters can be set for KSQL queries, the Kafka Admin Client, the KSQL Server, the Kafka Streams API, the Kafka Producer, and the Kafka Consumer. 

These can all be configured via a properties file. 

.. important:: KSQL Server configuration settings take precedence over those set in the KSQL CLI. For example, if a value
               for ``ksql.streams.replication.factor`` is set in both the KSQL Server and KSQL CLI, the KSQL Server value is used.

General KSQL parameters
    General KSQL configurations have the required ``ksql`` prefix. For example, ``ksql.service.id`` and ``ksql.persistent.prefix``. A full list of configurations can be found in our `source file`_.
    .. _source file: https://github.com/confluentinc/ksql/blob/4.1.x/ksql-common/src/main/java/io/confluent/ksql/util/KsqlConfig.java#L86

KSQL Server
    These configurations control the general behavior of the KSQL Server. For example, ``ksql.command.topic.suffix`` and ``ui.enabled``

Kafka Streams and Kafka Clients
    These configurations control how Kafka Streams executes queries. They also control the behavior of the underlying Kafka clients, viz. the producer, the consumer, and the admin client. These configs have the optional ``ksql.streams`` prefix. For example,  ``ksql.streams.auto.offset.reset`` and ``ksql.streams.cache.max.bytes.buffering``.

--------------------
KSQL Properties File
--------------------

By default the KSQL properties file is located at ``<path-to-confluent>/etc/ksql/ksql-server.properties``. The properties
file syntax follows Java conventions.

.. code:: bash

    <property-name>=<property-value>

For example:

.. code:: bash

    bootstrap.servers=localhost:9092
    listeners=http://localhost:8080
    ui.enabled=true

After you have configured your properties file, you can start KSQL with your properties file specified.

.. code:: bash

    $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

-----------
JMX Metrics
-----------

.. include:: ../../includes/ksql-includes.rst
    :start-line: 320
    :end-line: 327

.. _configuring-ksql:

------------------------------------
Common KSQL Configuration Parameters
------------------------------------

Here are some common configuration properties that you might want to customize.

.. _ksql-auto-offset-reset:

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ksql.streams.auto.offset.reset
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Determines what to do when a KSQL query has not committed any offsets and has thus not marked progress on its 
input Kafka topic. The default value in KSQL is ``latest``, which means KSQL queries will read Kafka topics 
from the latest available offset when they are first started. This means they will only process data produced 
after the query starts. To process data in input topics from the first available message, set the value to 
``earliest``  as follows:

.. code:: bash

    ksql> SET 'ksql.streams.auto.offset.reset'='earliest';

For more information, see :ref:`kafka_consumer` and the :cp-javadoc:`Javadoc|clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html#AUTO_OFFSET_RESET_CONFIG`.

.. _ksql-bootstrap-servers:

^^^^^^^^^^^^^^^^^
bootstrap.servers
^^^^^^^^^^^^^^^^^

A comma-separated list of host and port pairs that is used for connecting with a Kafka cluster. This list should be
in the form ``host1:port1,host2:port2,...`` The default value in KSQL is ``localhost:9092``. For example, to change it to ``9095``
by using the KSQL command line:

.. code:: bash

    ksql> SET 'bootstrap.servers'='localhost:9095';

For more information, see :ref:`Streams parameter reference <streams_developer-guide_required-configs>` and the :cp-javadoc:`Javadoc|clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html#BOOTSTRAP_SERVERS_CONFIG`.

.. _ksql-commit-interval-ms:

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ksql.streams.commit.interval.ms
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The frequency to save the state of a KSQL query.  The default value in KSQL is ``2000``, which means that KSQL will commit offsets (and thus mark progress), flush producer buffers (and hence materialize outputs to downstream consumers), and flush local state every 2 seconds.  A longer duration will allow more batching and thus higher throughput, at the cost of higher end-to-end processing latency. A lower value will have lower end-to-end processing latency but also lower throughput. Here is an example to commit offsets every 5 seconds:

.. code:: bash

    ksql> SET 'commit.interval.ms'='5000';

For more information, see the :ref:`Streams parameter reference <streams_developer-guide_optional-configs>` and the :cp-javadoc:`Javadoc|streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#COMMIT_INTERVAL_MS_CONFIG`,

.. _ksql-cache-max-bytes-buffering:

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ksql.streams.cache.max.bytes.buffering
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is a size based version of ``ksql.streams.commit.interval.ms``. It controls the amount of data to cache before flushing local state in the streams app. A higher value will allow more throughput at the cost of higher end-to-end processing latency. A lower value will enable lower processing latency at the cost of lower throughput. The default value in KSQL is ``10000000`` (~ 10 MB).
Here is an example to change the value to ``20000000`` by using the KSQL command line:

.. code:: bash

    ksql> SET 'cache.max.bytes.buffering'='20000000';

For more information, see the :ref:`Streams parameter reference <streams_developer-guide_optional-configs>` and :cp-javadoc:`Javadoc|streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#CACHE_MAX_BYTES_BUFFERING_CONFIG`.

.. _ksql-fail-on-deserialization-error:

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
fail.on.deserialization.error
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Indicates whether to fail if corrupt messages are read. KSQL decodes messages at runtime when reading from a Kafka topic. The
decoding that KSQL uses depends on what's defined in STREAM's or TABLE's data definition as the data format for the
topic. If a message in the topic can't be decoded according to that data format, KSQL considers this message to be
corrupt. For example, a message is corrupt if KSQL expects message values to be in JSON format, but they are in
DELIMITED format. The default value in KSQL is ``true``. For example, to ignore corrupt messages, add this to your
properties file:

.. code:: java

    fail.on.deserialization.error=false

.. _ksql-schema-registry-url:

^^^^^^^^^^^^^^^^^^^^^^^^
ksql.schema.registry.url
^^^^^^^^^^^^^^^^^^^^^^^^

The Schema Registry URL path to connect KSQL to.

.. _ksql-service-id:

^^^^^^^^^^^^^^^
ksql.service.id
^^^^^^^^^^^^^^^

The service ID of the KSQL server. By default, the service ID of KSQL servers is ``default_``. This determines the prefix of
the internal topics created by KSQL. With the default value for ``ksql.service.id``, the prefix for KSQL internal topics will be 
``_confluent-ksql-default_``. One example of an internal topic is the command topic, which distributes queries across a KSQL 
server pool. With the default ``ksql.service.id``, the command topic would be ``_confluent-ksql-default__command_topic``. On 
the other hand, if you set ``ksql.service.id`` to ``production_deployment_``, the KSQL command topic will 
be ``_confluent-ksql-production_deployment__command_topic``.

You can configure ``ksql.service.id`` so that your environment has isolated pools of KSQL servers sharing the same underlying 
Kafka cluster.

.. _ksql-queries-file:

^^^^^^^^^^^^^^^^^
ksql.queries.file
^^^^^^^^^^^^^^^^^

A file that specifies a predefined set of queries for the KSQL Server, KSQL, and its underlying Kafka Streams instances.
For an example, see :ref:`restrict-ksql-interactive`.

.. _ksql.sink.partitions:

^^^^^^^^^^^^^^^^^^^^
ksql.sink.partitions
^^^^^^^^^^^^^^^^^^^^

The default number of partitions for the topics created by KSQL. The default is four.

.. _ksql.sink.replicas:

^^^^^^^^^^^^^^^^^^
ksql.sink.replicas
^^^^^^^^^^^^^^^^^^

The default number of replicas for the topics created by KSQL. The default is one. 

.. _ksql-listeners:

^^^^^^^^^
listeners
^^^^^^^^^

Set the port for the KSQL Server to listen on. This defaults to http://localhost:8088. To listen for on port 80, update it accordingly
in your ``ksql-server.properties`` file.

.. code:: bash

    # Set which port to listen on.
    listeners=http://0.0.0.0:80 
    
.. _restrict-ksql-interactive:

----------------------------------
Restricting Interactive KSQL Usage
----------------------------------

KSQL supports locked-down deployment scenarios where you can restrict interactive use of the KSQL cluster.

You can prevent interactive use of a KSQL cluster. For example, you want to allow a team of users to develop and verify
their queries on a shared testing KSQL cluster. But when putting those queries to production you prefer to lock-down access
to KSQL Servers, version-control the exact queries and storing them in a .sql file, and prevent users from interacting
directly with the production KSQL cluster.

You can configure servers to run a predefined script (.sql file) via the ``--queries-file`` command line argument, or the
``ksql.queries.file`` setting in the :ref:`KSQL configuration file <common-configs>`. If a server is running a predefined
script, it will automatically disable its REST endpoint and interactive use.

.. tip::
If the ``ksql.queries.file`` property and the ``--queries-file`` argument are present, the ``--queries-file`` argument will take precedence.

Start the KSQL Server in via the command line argument
    #. Create a predefined script and save as an ``.sql`` file.

    #. Start the KSQL with the predefined script specified via the ``--queries-file`` argument.

       .. code:: bash

            $ <path-to-confluent>/bin/ksql-start-server <path-to-confluent>/etc/ksql/ksql-server.properties \
              --queries-file <path-to-queries-file>.sql

Start the KSQL Server in via the ``ksql-server.properties`` file
   #. Configure the ``ksql-server.properties`` file.  The ``bootstrap.servers`` and ``ksql.queries.file``
      are required. For more information about configuration, see :ref:`common-configs`.

      .. code:: bash

          # Inform the KSQL Server where the Kafka cluster can be found:
          bootstrap.servers=localhost:9092

          # Define the location of the queries file to execute
          ksql.queries.file=path/to/queries.sql

   #. Start the KSQL with the properties file specified.
      .. code:: bash

            $ <path-to-confluent>/bin/ksql-start-server <path-to-confluent>/etc/ksql/ksql-server.properties

-----------------------------
Production Settings
-----------------------------

When deploying KSQL to production, the following settings are recommended in your ``/etc/ksql/ksql-server.properties`` file:

.. code:: bash

    # Set the retries to Integer.MAX_VALUE to ensure that transient failures
    # will not result in data loss.
    producer.retries=2147483647

    # Set the batch expiry to Long.MAX_VALUE to ensure that queries will not
    # terminate if the underlying Kafka cluster is unavailable for a period of
    # time.  
    producer.confluent.batch.expiry.ms=9223372036854775807

    # Allows more frequent retries of requests when there are failures,
    # enabling quicker recovery.
    producer.request.timeout.ms=300000

    # Set the maximum allowable time for the producer to block to
    # Long.MAX_VALUE. This allows KSQL to pause processing if the underlying
    # Kafka cluster is unavailable.  
    producer.max.block.ms=9223372036854775807

    # Set the replication factor for internal topics, the command topic, and
    # output topics to be 3 for better fault tolerance and durability. Note:
    # the value 3 requires at least 3 brokers in your Kafka cluster.
    ksql.streams.replication.factor=3
    ksql.sink.replicas=3

    # Set the storage directory for stateful operations like aggregations and
    # joins to be at a durable location. By default, they are stored in /tmp.
    ksql.streams.state.dir=/some/non-temporary-storage-path/
    
    # Bump the number of replicas for state storage for stateful operations
    # like aggregations and joins. By having two replicas (one main and one
    # standby) recovery from node failures is quicker since the state doesn't
    # have to be rebuilt from scratch.
    ksql.streams.num.standby.replicas=1

