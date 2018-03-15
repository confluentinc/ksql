.. _ksql-server-config-ref:

KSQL Config Reference
---------------------

Here are some common configuration properties that you might want to change from their default values.

auto.offset.reset
   Determines what to do when there is no initial offset in Kafka or if the current offset does not exist on the server. The
   default value in KSQL is ``latest``, which means all Kafka topics are read from the latest available offset. For example,
   to change it to earliest by using the KSQL command line:

   .. code:: bash

    ksql> SET 'auto.offset.reset'='earliest';

   For more information, see :ref:`kafka_consumer` and the :cp-javadoc:`Javadoc|clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html#AUTO_OFFSET_RESET_CONFIG`.

bootstrap.servers
   A list of host and port pairs that is used for establishing the initial connection to the Kafka cluster. This list should be
   in the form ``host1:port1,host2:port2,...`` The default value in KSQL is ``localhost:9092``. For example, to change it to ``9095``
   by using the KSQL command line:

   .. code:: bash

        ksql> SET 'bootstrap.servers'='localhost:9095';

   For more information, see :ref:`Streams parameter reference <streams_developer-guide_required-configs>` and the :cp-javadoc:`Javadoc|clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html#BOOTSTRAP_SERVERS_CONFIG`.

commit.interval.ms
   The frequency to save the position of the processor. The default value in KSQL is ``2000``. Here is an example to change
   the value to ``5000`` by using the KSQL command line:

   .. code:: bash

        ksql> SET 'commit.interval.ms'='5000';

   For more information, see the :ref:`Streams parameter reference <streams_developer-guide_optional-configs>` and the :cp-javadoc:`Javadoc|streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#COMMIT_INTERVAL_MS_CONFIG`,

cache.max.bytes.buffering
   The maximum number of memory bytes to be used for buffering across all threads. The default value in KSQL is ``10000000`` (~ 10 MB).
   Here is an example to change the value to ``20000000`` by using the KSQL command line:

   .. code:: bash

        ksql> SET 'cache.max.bytes.buffering'='20000000';

   For more information, see the :ref:`Streams parameter reference <streams_developer-guide_optional-configs>` and :cp-javadoc:`Javadoc|streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#CACHE_MAX_BYTES_BUFFERING_CONFIG`.

fail.on.deserialization.error
    Indicates whether to fail if corrupt messages are read. KSQL decodes messages at runtime when reading from a Kafka topic. The
    decoding that KSQL uses depends on what's defined in STREAM's or TABLE's data definition as the data format for the
    topic. If a message in the topic can't be decoded according to that data format, KSQL considers this message to be
    corrupt. For example, a message is corrupt if KSQL expects message values to be in JSON format, but they are in
    DELIMITED format. The default value in KSQL is ``true``. For example, to ignore corrupt messages, add this to your
    properties file:

    .. code:: java

        fail.on.deserialization.error=false

ksql.command.topic.suffix
    The KSQL server command topic determines the resource pool. By default, KSQL servers use the ``ksql__commands`` command topic.
    To assign a server to a different pool, change the ``ksql.command.topic.suffix`` setting. For example, if you change to ``'production_commands'``, in the ``ksql-server.properties`` file, the command topic will be named ``ksql__production_commands``.

    .. code:: java

        ksql.command.topic.suffix=production_commands

ksql.schema.registry.url
    The Schema Registry URL path to connect KSQL to.

ksql.queries.file
    A file that specifies a predefined set of queries for the KSQL Server, KSQL, and its underlying Kafka Streams instances.
    For an example, see :ref:`<install_ksql-standalone>`.

listeners
   The maximum number of memory bytes to be used for buffering across all threads. The default value in KSQL is ``10000000`` (~ 10 MB).
   Here is an example to change the value to ``20000000`` by using the KSQL command line:

   .. code:: bash

    ksql> SET 'cache.max.bytes.buffering'='20000000';

   For more information, see the :cp-javadoc:`Javadoc|streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#CACHE_MAX_BYTES_BUFFERING_CONFIG`.


    
    

