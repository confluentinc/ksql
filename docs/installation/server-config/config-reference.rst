.. _ksql-param-reference:

KSQL Configuration Parameter Reference
======================================

Here are some common configuration properties that you can customize. Refer to
:ref:`ksql-server-config` and :ref:`install_cli-config` for details of how to set properties.

.. tip::

   Each property has a corresponding environment variable in the Docker image
   for `KSQL Server <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__.
   The environment variable name is constructed from the configuration property
   name by converting to uppercase, replacing periods with underscores, and
   prepending with ``KSQL_``. For example, the name of the ``ksql.service.id``
   environment variable is ``KSQL_KSQL_SERVICE_ID``. For more information, see
   :ref:`install-ksql-with-docker`.

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

Determines what to do when there is no initial offset in |ak-tm| or if the current offset does not exist on the server. The
default value in KSQL is ``latest``, which means all Kafka topics are read from the latest available offset. For example,
to change it to earliest by using the KSQL command line:

.. code:: sql

    SET 'auto.offset.reset'='earliest';

For more information, see :ref:`kafka_consumer` and the :cp-javadoc:`Javadoc|clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html#AUTO_OFFSET_RESET_CONFIG`.

The corresponding environment variable in the
`KSQL Server image <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__ is
``KSQL_KSQL_STREAMS_AUTO_OFFSET_RESET``.

.. _ksql-bootstrap-servers:

------------------------------
ksql.streams.bootstrap.servers
------------------------------

A list of host and port pairs that is used for establishing the initial connection to the Kafka cluster. This list should be
in the form ``host1:port1,host2:port2,...`` The default value in KSQL is ``localhost:9092``. For example, to change it to ``9095``
by using the KSQL command line:

.. code:: sql

    SET 'bootstrap.servers'='localhost:9095';

For more information, see :ref:`Streams parameter reference <streams_developer-guide_required-configs>` and the :cp-javadoc:`Javadoc|clients/javadocs/org/apache/kafka/clients/consumer/ConsumerConfig.html#BOOTSTRAP_SERVERS_CONFIG`.

The corresponding environment variable in the
`KSQL Server image <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__ is
``KSQL_KSQL_STREAMS_BOOTSTRAP_SERVERS`` or ``KSQL_BOOTSTRAP_SERVERS``.
For more information, see :ref:`install-ksql-with-docker`.

.. _ksql-commit-interval-ms:

-------------------------------
ksql.streams.commit.interval.ms
-------------------------------

The frequency to save the position of the processor. The default value in KSQL is ``2000``. Here is an example to change
the value to ``5000`` by using the KSQL command line:

.. code:: sql

    SET 'commit.interval.ms'='5000';

For more information, see the :ref:`Streams parameter reference <streams_developer-guide_optional-configs>` and the :cp-javadoc:`Javadoc|streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#COMMIT_INTERVAL_MS_CONFIG`,

The corresponding environment variable in the
`KSQL Server image <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__ is
``KSQL_KSQL_STREAMS_COMMIT_INTERVAL_MS``.

.. _ksql-cache-max-bytes-buffering:

--------------------------------------
ksql.streams.cache.max.bytes.buffering
--------------------------------------

The maximum number of memory bytes to be used for buffering across all threads. The default value in KSQL is ``10000000`` (~ 10 MB).
Here is an example to change the value to ``20000000`` by using the KSQL command line:

.. code:: sql

    SET 'cache.max.bytes.buffering'='20000000';

For more information, see the :ref:`Streams parameter reference <streams_developer-guide_optional-configs>` and :cp-javadoc:`Javadoc|streams/javadocs/org/apache/kafka/streams/StreamsConfig.html#CACHE_MAX_BYTES_BUFFERING_CONFIG`.

The corresponding environment variable in the
`KSQL Server image <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__ is
``KSQL_KSQL_STREAMS_CACHE_MAX_BYTES_BUFFERING``.

.. _ksql-streams-num-streams-threads:

-------------------------------
ksql.streams.num.stream.threads
-------------------------------

This number of stream threads in an instance of the Kafka Streams application. The stream processing code runs in these
threads. For more information about Kafka Streams threading model, see :ref:`streams_architecture_threads`.

The corresponding environment variable in the
`KSQL Server image <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__ is
``KSQL_KSQL_STREAMS_NUM_STREAM_THREADS``.

-----------------------------
ksql.output.topic.name.prefix
-----------------------------

The default prefix for automatically created topic names. Unless a user
defines an explicit topic name in a KSQL statement, KSQL prepends the value of
``ksql.output.topic.name.prefix`` to the names of automatically created output
topics. For example, you might use "ksql-interactive-" to name output topics
in a KSQL Server cluster that's deployed in interactive mode. For more information, see
:ref:`Configuring Security for KSQL <config-security-ksql-acl-interactive_post_ak_2_0>`.

The corresponding environment variable in the
`KSQL Server image <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__ is
``KSQL_KSQL_OUTPUT_TOPIC_NAME_PREFIX``.

KSQL Query Settings
-------------------

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
DELIMITED format. The default value in KSQL is ``false``, which means a corrupt message will result in a log entry,
and KSQL will continue processing. To change this default behavior and instead have Kafka Streams threads shut down when
corrupt messages are encountered, add this to your properties file:

::

    ksql.fail.on.deserialization.error=true

.. _ksql-fail-on-production-error:

-----------------------------
ksql.fail.on.production.error
-----------------------------

Indicates whether to fail if KSQL fails to publish a record to an output topic due to a Kafka producer exception.
The default value in KSQL is ``true``, which means if a producer error occurs, then the Kafka Streams thread that
encountered the error will shut down. To log the error message to the
:ref:`ksql_processing_log` and have KSQL continue processing as normal, add this to your properties file:

::

    ksql.fail.on.production.error=false

The corresponding environment variable in the
`KSQL Server image <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__ is
``KSQL_KSQL_FAIL_ON_DESERIALIZATION_ERROR``.

.. _ksql-schema-registry-url:

------------------------
ksql.schema.registry.url
------------------------

The |sr| URL path to connect KSQL to. To communicate with |sr| over a secure
connection, see :ref:`config-security-ksql-sr`.

The corresponding environment variable in the
`KSQL Server image <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__ is
``KSQL_KSQL_SCHEMA_REGISTRY_URL``.

.. _ksql-service-id:

---------------
ksql.service.id
---------------

The service ID of the KSQL server. This is used to define the KSQL cluster membership of a KSQL server instance. If multiple KSQL
servers connect to the same Kafka cluster (i.e. the same ``bootstrap.servers``) *and* have the same ``ksql.service.id`` they will form a KSQL cluster and share the workload. 

By default, the service ID of KSQL servers is ``default_``. The service ID is also used as
the prefix for the internal topics created by KSQL. Using the default value ``ksql.service.id``, the KSQL internal topics
will be prefixed as ``_confluent-ksql-default_`` (e.g. ``_command_topic`` becomes ``_confluent-ksql-default__command_topic``).

By convention, the ``ksql.service.id`` property should end with a separator character of some form,
for example a dash or underscore, as this makes the internal topic names easier to read.

.. _ksql-internal-topic-replicas:

----------------------------
ksql.internal.topic.replicas
----------------------------

The number of replicas for the internal topics created by KSQL Server. The default is 1.
This configuration parameter works in KSQL 5.3 and later.
Replicas for the record processing log topic should be configured separately.
For more information, see :ref:`KSQL Processing Log <ksql_processing_log>`.

The corresponding environment variable in the
`KSQL Server image <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__ is
``KSQL_KSQL_INTERNAL_TOPIC_REPLICAS``.

.. _ksql-sink-partitions:

---------------------------------
ksql.sink.partitions (Deprecated)
---------------------------------

The default number of partitions for the topics created by KSQL. The default is four.
This property has been deprecated since 5.3 release. For more info see the WITH clause properties in :ref:`CREATE STREAM AS SELECT <create-stream-as-select>` and :ref:`CREATE TABLE AS SELECT <create-table-as-select>`.

The corresponding environment variable in the
`KSQL Server image <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__ is
``KSQL_KSQL_SINK_PARTITIONS``.

.. _ksql-sink-replicas:

-------------------------------
ksql.sink.replicas (Deprecated)
-------------------------------

The default number of replicas for the topics created by KSQL. The default is one.
This property has been deprecated since 5.3 release. For more info see the WITH clause properties in :ref:`CREATE STREAM AS SELECT <create-stream-as-select>` and :ref:`CREATE TABLE AS SELECT <create-table-as-select>`.

------------------------------------
ksql.functions.substring.legacy.args
------------------------------------

Controls the semantics of the SUBSTRING UDF. Refer to the SUBSTRING documentation in the :ref:`function <functions>` guide for details.

When upgrading headless mode KSQL applications from versions 5.0.x or earlier without updating your queries that use SUBSTRING to match 
the new 5.1 behavior, you must set this config to ``true`` to enforce the previous SUBSTRING behavior. If possible, however, we recommend
that you update your queries accordingly instead of enabling this configuration setting.

The corresponding environment variable in the
`KSQL Server image <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__ is
``KSQL_KSQL_FUNCTIONS_SUBSTRING_LEGACY_ARGS``.

.. _ksql_persistence_wrap_single_values:

-----------------------------------
ksql.persistence.wrap.single.values
-----------------------------------

Sets the default value for the ``WRAP_SINGLE_VALUE`` property if one is
not supplied explicitly in :ref:`CREATE TABLE <create-table>`,
:ref:`CREATE STREAM <create-stream>`, :ref:`CREATE TABLE <create-table-as-select>`
or :ref:`CREATE STREAM AS SELECT <create-stream-as-select>` statements.

When set to the default value, ``true``, KSQL serializes the column value nested with a JSON object or
an Avro record, depending on the format in use. When set to ``false``, KSQL persists the column
value without any nesting.

For example, consider the statement:

.. code:: sql

    CREATE STREAM y AS SELECT f0 FROM x;

The statement selects a single field as the value of stream ``y``. If ``f0`` has the
integer value ``10``,
with ``ksql.persistence.wrap.single.values`` set to ``true``, the JSON format persists
the value within a JSON object, as it would if the value had more fields:

.. code:: json

    {
       "F0": 10
    }

With ``ksql.persistence.wrap.single.values`` set to ``false``, the JSON format
persists the single field's value as a JSON number: ``10``.

.. code:: json

    10

The ``AVRO`` format supports the same properties. The properties control whether or not the field's
value is written as a named field within an Avro record or as an anonymous value.

This setting can be toggled using the `SET` command

 .. code:: sql
     SET 'ksql.persistence.wrap.single.values'='false';

For more information, refer to the :ref:`CREATE TABLE <create-table>`,
:ref:`CREATE STREAM <create-stream>`, :ref:`CREATE TABLE <create-table-as-select>`
or :ref:`CREATE STREAM AS SELECT <create-stream-as-select>` statements.

.. note:: The ``DELIMITED`` format is  not affected by the `ksql.persistence.ensure.value.is.struct`` setting,
          because it has no concept of an outer record or structure.

KSQL Server Settings
--------------------

These configurations control the general behavior of the KSQL server. These configurations can only be specified via the
``ksql-server.properties`` file.

.. important:: KSQL server configuration settings take precedence over those set in the KSQL CLI. For example, if a value
               for ``ksql.streams.replication.factor`` is set in both the KSQL server and KSQL CLI, the KSQL server value is used.

.. _ksql.query.persistent.active.limit:

----------------------------------
ksql.query.persistent.active.limit
----------------------------------

The maximum number of persistent queries that may be running at any given time. Applies to interactive mode only.
Once the limit is reached, commands that try to start additional persistent queries will be rejected.
Users may terminate existing queries before attempting to start new ones to avoid hitting the limit.
The default is no limit.

When setting up KSQL servers, it may be desirable to configure this limit to prevent users from overloading the server
with too many queries, since throughput suffers as more queries are run simultaneously,
and also because there is some small CPU overhead associated with starting each new query.
See :ref:`KSQL Sizing Recommendations <important-sizing-factors>` for more details.

.. _ksql-queries-file:

-----------------
ksql.queries.file
-----------------

A file that specifies a predefined set of queries for the KSQL and KSQL server.
For an example, see :ref:`restrict-ksql-interactive`.

The corresponding environment variable in the
`KSQL Server image <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__ is
``KSQL_KSQL_QUERIES_FILE``.

.. _ksql-listeners:

---------
listeners
---------

The ``listeners`` setting controls the REST API endpoint for the KSQL server.
For more info, see :ref:`ksql-rest-api`.

The default hostname is ``0.0.0.0`` which binds to all interfaces. Update this
to a specific interface to bind only to a single interface. For example:

::

    # Bind to all interfaces.
    listeners=http://0.0.0.0:8088

    # Bind only to localhost.
    listeners=http://localhost:8088

You can configure KSQL Server to use HTTPS. For more information, see
:ref:`config-ksql-for-https`.

The corresponding environment variable in the
`KSQL Server image <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__ is
``KSQL_LISTENERS``.

.. _ksql-metrics-tags-custom:

------------------------
ksql.metrics.tags.custom
------------------------

A list of tags to be included with emitted :ref:`JMX metrics <ksql-monitoring-and-metrics>`,
formatted as a string of ``key:value`` pairs separated by commas.
For example, ``key1:value1,key2:value2``.

.. _ksql-c3-settings:

|c3| Settings
-------------

You can access KSQL Server by using |c3|. For more information, see
:ref:`controlcenter_ksql_settings`.

.. _ksql-cloud-settings:

|ccloud| Settings
-----------------

You can connect KSQL Server to |ccloud|. For more information, see
:ref:`install_ksql-ccloud`.

KSQL Processing Log Settings
----------------------------

These configurations control the behavior of the :ref:`KSQL processing log <ksql_processing_log>`.

.. _ksql-processing-log-topic-auto-create:

-----------------------------------------
ksql.logging.processing.topic.auto.create
-----------------------------------------

Toggles automatic processing log topic creation. If set to true, then KSQL will automatically try
to create a processing log topic at startup. The name of the topic is the value of the
:ref:`ksql-processing-log-topic-name` property. The number of partitions is taken from the
:ref:`ksql-processing-log-topic-partitions` property , and the replication factor is taken from the
:ref:`ksql-processing-log-topic-replication-factor` property. By default, this property has the value
``false``.

.. _ksql-processing-log-topic-name:

----------------------------------
ksql.logging.processing.topic.name
----------------------------------

If automatic processing log topic creation is enabled, KSQL sets the name of the topic to the value of
this property. If automatic processing log stream creation is enabled, KSQL uses this topic to back the
stream. By default, this property has the value ``<service id>ksql_processing_log``, where ``<service id>``
is the value of the :ref:`ksql-service-id` property.

.. _ksql-processing-log-topic-partitions:

----------------------------------------
ksql.logging.processing.topic.partitions
----------------------------------------

If automatic processing log topic creation is enabled, KSQL creates the topic with number of partitions set
to the value of this property. By default, this property has the value ``1``.

.. _ksql-processing-log-topic-replication-factor:

------------------------------------------------
ksql.logging.processing.topic.replication.factor
------------------------------------------------

If automatic processing log topic creation is enabled, KSQL creates the topic with  number of replicas set
to the value of this property. By default, this property has the value ``1``.

.. _ksql-processing-log-stream-auto-create:

------------------------------------------
ksql.logging.processing.stream.auto.create
------------------------------------------

Toggles automatic processing log stream creation. If set to true, and KSQL is running in interactive mode on a new cluster,
KSQL automatically creates a processing log stream when it starts up. The name for the stream is the
value of the :ref:`ksql-processing-log-stream-name` property. The stream is created over the topic set in
the :ref:`ksql-processing-log-topic-name` property. By default, this property has the value ``false``.

.. _ksql-processing-log-stream-name:

-----------------------------------
ksql.logging.processing.stream.name
-----------------------------------

If automatic processing log stream creation is enabled, KSQL sets the name of the stream to the value of this
property. By default, this property has the value ``KSQL_PROCESSING_LOG``.

.. _ksql-processing-log-include-rows:

------------------------------------
ksql.logging.processing.rows.include
------------------------------------

Toggles whether or not the processing log should include rows in log messages. By default, this property has the
value ``false``.

.. _ksql-production-settings:

Recommended KSQL Production Settings
------------------------------------

When deploying KSQL to production, the following settings are recommended in your ``/etc/ksql/ksql-server.properties`` file:

::

    # Set the batch expiry to Integer.MAX_VALUE to ensure that queries will not
    # terminate if the underlying Kafka cluster is unavailable for a period of
    # time.
    ksql.streams.producer.delivery.timeout.ms=2147483647

    # Set the maximum allowable time for the producer to block to
    # Long.MAX_VALUE. This allows KSQL to pause processing if the underlying
    # Kafka cluster is unavailable.
    ksql.streams.producer.max.block.ms=9223372036854775807

    # For better fault tolerance and durability, set the replication factor for the KSQL
    # Server's internal topics. Note: the value 3 requires at least 3 brokers in your Kafka cluster.
    ksql.internal.topic.replicas=3

    # For better fault tolerance and durability, set the replication factor for
    # the internal topics that Kafka Streams creates for some queries.
    # Note: the value 3 requires at least 3 brokers in your Kafka cluster.
    ksql.streams.replication.factor=3

    # Set the storage directory for stateful operations like aggregations and
    # joins to be at a durable location. By default, they are stored in /tmp.
    ksql.streams.state.dir=/some/non-temporary-storage-path/

    # Bump the number of replicas for state storage for stateful operations
    # like aggregations and joins. By having two replicas (one main and one
    # standby) recovery from node failures is quicker since the state doesn't
    # have to be rebuilt from scratch.
    ksql.streams.num.standby.replicas=1
