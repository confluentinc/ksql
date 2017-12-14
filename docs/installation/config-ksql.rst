.. _configuring-ksql:

Configuring KSQL
================

You can set configuration properties for KSQL and your queries with the
SET statement. This includes :cp-javadoc:`settings for Kafka’s Streams
API |streams/javadocs/index.html` (e.g.,
``cache.max.bytes.buffering``) as well as settings for Kafka’s :cp-javadoc:`producer
client |clients/javadocs/org/apache/kafka/clients/producer/ProducerConfig.html` and
:cp-javadoc:`consumer
client |clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html`
(e.g., ``auto.offset.reset``).

The basic syntax is:

.. code:: sql

    SET '<property-name>'='<property-value>';

The property name and value must be enclosed in single quotes.

**Tip:** You can view the current settings with the ``SHOW PROPERTIES`` command.

After a property has been set, it remains in effect for the remainder
of the KSQL CLI session, or until you issue another SET statement to change
it.

.. caution::
    When using KSQL in Docker, the properties file must be available inside the Docker container. If you
    don’t want to customize your Docker setup so that it contains an appropriate properties file, you can use the SET statement.

Examples
--------

Common configuration properties that you might want to change from their
default values include:

-  :cp-javadoc:`auto.offset.reset |clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html#AUTO_OFFSET_RESET_CONFIG`:
   The default value in KSQL is ``latest`` meaning all the Kafka topics
   will be read from the current offset (aka latest available data). You
   can change it using the following statement:

   .. code:: bash

    ksql> SET 'auto.offset.reset'='earliest';

-  :cp-javadoc:`commit.interval.ms |streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#COMMIT_INTERVAL_MS_CONFIG`:
   The default value in KSQL is ``2000``. Here is an example to change
   the value to ``5000``:

   .. code:: bash

    ksql> SET 'commit.interval.ms'='5000';

-  :cp-javadoc:`cache.max.bytes.buffering |streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#CACHE_MAX_BYTES_BUFFERING_CONFIG`:
   The default value in KSQL is ``10000000`` (~ 10 MB). Here is an example to change the value to ``20000000``:

   .. code:: bash

    ksql> SET 'cache.max.bytes.buffering'='20000000';


You can also use a properties file instead of the SET statement. The
syntax of properties files follow Java conventions, which are slightly
different to the syntax of the SET statement above.

.. code:: bash

    # Show the example contents of a properties file
    $ cat ksql.properties
    auto.offset.reset=earliest

    # Start KSQL in standalone mode with the custom properties above
    $ ksql-cli local --properties-file ./ksql.properties

    # Start a KSQL server node (for client-server mode) with the custom properties above
    $ ksql-server-start ./ksql.properties

