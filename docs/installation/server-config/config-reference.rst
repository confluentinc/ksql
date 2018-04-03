.. _ksql-param-reference:

KSQL Configuration Parameter Reference
======================================

Here are some common configuration properties that you can customize.

.. contents::
    :local:

Kafka Streams and Kafka Client Settings
---------------------------------------

These configurations control how Kafka Streams executes queries. These configurations can be specified via the
``ksql-server.properties`` file or via ``SET`` in a KSQL CLI. These can be provided with the optional ``ksql.streams.`` prefix.

.. important:: Although you can use either prefixed (``ksql.streams.``) or un-prefixed settings, it is recommended that
               you use prefixed settings.

.. _ksql-auto-offset-reset:

------------------------------
ksql.streams.auto.offset.reset
------------------------------

Determines what to do when there is no initial offset in Kafka or if the current offset does not exist on the server. The
default value in KSQL is ``latest``, which means all Kafka topics are read from the latest available offset. For example,
to change it to earliest by using the KSQL command line:

.. code:: bash

    ksql> SET 'auto.offset.reset'='earliest';

For more information, see :ref:`kafka_consumer` and the :cp-javadoc:`Javadoc|clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html#AUTO_OFFSET_RESET_CONFIG`.

.. _ksql-bootstrap-servers:

------------------------------
ksql.streams.bootstrap.servers
------------------------------

A list of host and port pairs that is used for establishing the initial connection to the Kafka cluster. This list should be
in the form ``host1:port1,host2:port2,...`` The default value in KSQL is ``localhost:9092``. For example, to change it to ``9095``
by using the KSQL command line:

.. code:: bash

    ksql> SET 'bootstrap.servers'='localhost:9095';

For more information, see :ref:`Streams parameter reference <streams_developer-guide_required-configs>` and the :cp-javadoc:`Javadoc|clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html#BOOTSTRAP_SERVERS_CONFIG`.

.. _ksql-commit-interval-ms:

-------------------------------
ksql.streams.commit.interval.ms
-------------------------------

The frequency to save the position of the processor. The default value in KSQL is ``2000``. Here is an example to change
the value to ``5000`` by using the KSQL command line:

.. code:: bash

    ksql> SET 'commit.interval.ms'='5000';

For more information, see the :ref:`Streams parameter reference <streams_developer-guide_optional-configs>` and the :cp-javadoc:`Javadoc|streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#COMMIT_INTERVAL_MS_CONFIG`,

.. _ksql-cache-max-bytes-buffering:

--------------------------------------
ksql.streams.cache.max.bytes.buffering
--------------------------------------

The maximum number of memory bytes to be used for buffering across all threads. The default value in KSQL is ``10000000`` (~ 10 MB).
Here is an example to change the value to ``20000000`` by using the KSQL command line:

.. code:: bash

    ksql> SET 'cache.max.bytes.buffering'='20000000';

For more information, see the :ref:`Streams parameter reference <streams_developer-guide_optional-configs>` and :cp-javadoc:`Javadoc|streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#CACHE_MAX_BYTES_BUFFERING_CONFIG`.

-------------------------------
ksql.streams.num.stream.threads
-------------------------------

This number of stream threads in an instance of the Kafka Streams application. The stream processing code runs in these
threads. For more information about Kafka Streams threading model, see :ref:`streams_architecture_threads`.


KSQL Query Settings
------------------

These configurations control how KSQL executes queries. These configurations can be specified via the ``ksql-server.properties``
file or via ``SET`` in a KSQL CLI. For example, ``ksql.service.id`` and ``ksql.persistent.prefix``.


.. _ksql-fail-on-deserialization-error:

----------------------------------
ksql.fail.on.deserialization.error
----------------------------------

Indicates whether to fail if corrupt messages are read. KSQL decodes messages at runtime when reading from a Kafka topic. The
decoding that KSQL uses depends on what's defined in STREAM's or TABLE's data definition as the data format for the
topic. If a message in the topic can't be decoded according to that data format, KSQL considers this message to be
corrupt. For example, a message is corrupt if KSQL expects message values to be in JSON format, but they are in
DELIMITED format. The default value in KSQL is ``true``. For example, to ignore corrupt messages, add this to your
properties file:

.. code:: java

    fail.on.deserialization.error=false

.. _ksql-schema-registry-url:

------------------------
ksql.schema.registry.url
------------------------

The Schema Registry URL path to connect KSQL to.

.. _ksql-service-id:

---------------
ksql.service.id
---------------

The service ID of the KSQL server. By default, the service ID of KSQL servers is ``default_``. The service ID is also used as
the prefix for the internal topics created by KSQL. Using the default value ``ksql.service.id``, the KSQL internal topics
will be prefixed as ``_confluent-ksql-default_`` (e.g. ``_command_topic`` becomes ``_confluent-ksql-default__command_topic``).

.. _ksql.sink.partitions:

--------------------
ksql.sink.partitions
--------------------

The default number of partitions for the topics created by KSQL. The default is four.

.. _ksql.sink.replicas:

------------------
ksql.sink.replicas
------------------

The default number of replicas for the topics created by KSQL. The default is one.

KSQL Server Settings
--------------------

These configurations control the general behavior of the KSQL server. These configurations can only be specified via the
``ksql-server.properties`` file.

.. important:: KSQL server configuration settings take precedence over those set in the KSQL CLI. For example, if a value
               for ``ksql.streams.replication.factor`` is set in both the KSQL server and KSQL CLI, the KSQL server value is used.

.. _ksql-queries-file:

-----------------
ksql.queries.file
-----------------

A file that specifies a predefined set of queries for the KSQL and KSQL server.
For an example, see :ref:`restrict-ksql-interactive`.

.. _ksql-listeners:

--------------
ksql.listeners
--------------

Comma-separated list of URIs (including protocol) that the broker will listen on. Specify hostname as ``0.0.0.0`` to bind
to all interfaces or leave it empty to bind to the default interface. For example:

.. code:: bash

    ksql.listeners=PLAINTEXT://myhost:9092

Example Production Settings
---------------------------

When deploying KSQL to production, the following settings are recommended in your ``/etc/ksql/ksql-server.properties`` file:

.. code:: bash

    # Set the retries to Integer.MAX_VALUE to ensure that transient failures
    # will not result in data loss.
    producer.retries=2147483647

    # Set the batch expiry to Long.MAX_VALUE to ensure that queries will not
    # terminate if the underlying Kafka cluster is unavailable for a period of
    # time.
    kafka.streams.producer.confluent.batch.expiry.ms=9223372036854775807

    # Allows more frequent retries of requests when there are failures,
    # enabling quicker recovery.
    kafka.streams.producer.request.timeout.ms=300000

    # Set the maximum allowable time for the producer to block to
    # Long.MAX_VALUE. This allows KSQL to pause processing if the underlying
    # Kafka cluster is unavailable.
    kafka.streams.producer.max.block.ms=9223372036854775807

    # Set the replication factor for internal topics, the command topic, and
    # output topics to be 3 for better fault tolerance and durability. Note:
    # the value 3 requires at least 3 brokers in your Kafka cluster.
    ksql.streams.replication.factor=3
    kafka.streams.ksql.sink.replicas=3

    # Set the storage directory for stateful operations like aggregations and
    # joins to be at a durable location. By default, they are stored in /tmp.
    ksql.streams.state.dir=/some/non-temporary-storage-path/

    # Bump the number of replicas for state storage for stateful operations
    # like aggregations and joins. By having two replicas (one main and one
    # standby) recovery from node failures is quicker since the state doesn't
    # have to be rebuilt from scratch.
    ksql.streams.num.standby.replicas=1
