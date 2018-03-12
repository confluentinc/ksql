.. _configuring-ksql:

Configuring KSQL
================

You can set the default properties for KSQL, :cp-javadoc:`Kafka’s Streams |streams/javadocs/index.html`, Kafka’s
:cp-javadoc:`producer client |clients/javadocs/org/apache/kafka/clients/producer/ProducerConfig.html` and
:cp-javadoc:`consumer client |clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html`, and admin client properties in the ``/etc/ksql/ksql-server.properties`` file.

You can set per-session properties for :cp-javadoc:`Kafka’s Streams |streams/javadocs/index.html`, Kafka’s
:cp-javadoc:`producer client |clients/javadocs/org/apache/kafka/clients/producer/ProducerConfig.html` and
:cp-javadoc:`consumer client |clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html`, and admin client in
the SET statement in the CLI.

To view the current configuration settings, use the ``SHOW PROPERTIES`` KSQL command.

.. code:: sql

        ksql> SHOW PROPERTIES;

Setting Default Properties
--------------------------
Configure KSQL with the ``/etc/ksql/ksql-server.properties`` file. A property remains in effect for the remainder of the KSQL
CLI session, or until you issue another SET statement to change it. The syntax of properties files follow Java conventions.
Here is the basic syntax.

.. code:: java

    <property-name>=<property-value>

Here is an example ``ksql-server.properties`` file:

.. code:: java

        bootstrap.servers=localhost:9092
        ksql.command.topic.suffix=commands
        listeners=http://localhost:8080

After you have configured your properties file, start KSQL with your properties file specified.

.. code:: bash

    $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

.. tip:: The KSQL server command topic determines the resource pool. By default, KSQL servers use the ``ksql__commands`` command topic. To assign a server to a different pool, change the ``ksql.command.topic.suffix`` setting. For example, if you change to ``ksql.command.topic.suffix = production_commands``, the command topic will be named ``ksql__production_commands``.

Setting Per-Session Properties
------------------------------

Configure KSQL with the ``/etc/ksql/ksql-server.properties`` file. A property remains in effect for the remainder of the KSQL
CLI session, or until you issue another SET statement to change it. The syntax of properties files follow Java conventions.
Here is the basic syntax.

.. code:: java

    <property-name>=<property-value>

Here is an example ``ksql-server.properties`` file:

.. code:: java

        bootstrap.servers=localhost:9092
        ksql.command.topic.suffix=commands
        listeners=http://localhost:8080

After you have configured your properties file, start KSQL with your properties file specified.

.. code:: bash

    $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

.. tip:: The KSQL server command topic determines the resource pool. By default, KSQL servers use the ``ksql__commands`` command topic. To assign a server to a different pool, change the ``ksql.command.topic.suffix`` setting. For example, if you change to ``ksql.command.topic.suffix = production_commands``, the command topic will be named ``ksql__production_commands``.

For more information, see :ref:`<install_ksql-standalone>`.

Setting Per-Session Properties
------------------------------

Here is the basic syntax is for setting properties.


.. important:: The property name (``'<property-name>``) and value (``'<property-value>'``) must be enclosed in single quotes (``'``).

.. code:: sql

        ksql> SET '<property-name>'='<property-value>';

.. _common-configs:

Common Configurations
---------------------

Here are some common configuration properties that you might want to change from their default values.

:cp-javadoc:`auto.offset.reset |clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html#AUTO_OFFSET_RESET_CONFIG`
   Determines what to do when there is no initial offset in Kafka or if the current offset does not exist on the server. The
   default value in KSQL is ``latest``, which means all Kafka topics are read from the latest available offset. For example,
   to change it to earliest by using the KSQL command line:

   .. code:: bash

    ksql> SET 'auto.offset.reset'='earliest';

:cp-javadoc:`bootstrap.servers |clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html#BOOTSTRAP_SERVERS_CONFIG`
   A list of host and port pairs that is used for establishing the initial connection to the Kafka cluster. This list should be
   in the form ``host1:port1,host2:port2,...`` The default value in KSQL is ``localhost:9092``. For example, to change it to ``9095``
   by using the KSQL command line:

   .. code:: bash

    ksql> SET 'bootstrap.servers'='localhost:9095';


:cp-javadoc:`commit.interval.ms |streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#COMMIT_INTERVAL_MS_CONFIG`
   The frequency to save the position of the processor. The default value in KSQL is ``2000``. Here is an example to change
   the value to ``5000`` by using the KSQL command line:

   .. code:: bash

    ksql> SET 'commit.interval.ms'='5000';

:cp-javadoc:`cache.max.bytes.buffering |streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#CACHE_MAX_BYTES_BUFFERING_CONFIG`
   The maximum number of memory bytes to be used for buffering across all threads. The default value in KSQL is ``10000000`` (~ 10 MB).
   Here is an example to change the value to ``20000000`` by using the KSQL command line:

   .. code:: bash

    ksql> SET 'cache.max.bytes.buffering'='20000000';

ksql.command.topic.suffix
    The KSQL server command topic determines the resource pool. By default, KSQL servers use the ``ksql__commands`` command topic.
    To assign a server to a different pool, change the ``ksql.command.topic.suffix`` setting. For example, if you change to ``'production_commands'``, in the ``ksqlserver.properties`` file, the command topic will be named ``ksql__production_commands``.

    .. code:: java

        ksql.command.topic.suffix=production_commands

ksql.schema.registry.url
    The Schema Registry URL path to connect KSQL to.

ksql.queries.file
    A file that specifies a predefined set of queries for the KSQL Server, KSQL, and its underlying Kafka Streams instances.
    For an example, see :ref:`<install_ksql-standalone>`.

:cp-javadoc:`listeners |streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#CACHE_MAX_BYTES_BUFFERING_CONFIG`
   The maximum number of memory bytes to be used for buffering across all threads. The default value in KSQL is ``10000000`` (~ 10 MB).
   Here is an example to change the value to ``20000000`` by using the KSQL command line:

   .. code:: bash

    ksql> SET 'cache.max.bytes.buffering'='20000000';

