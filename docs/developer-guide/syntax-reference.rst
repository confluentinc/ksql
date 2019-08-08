.. _ksql_syntax_reference:

KSQL Syntax Reference
=====================

KSQL has similar semantics to SQL:

- Terminate KSQL statements with a semicolon ``;``.
- Escape single-quote characters (``'``) inside string literals by using two successive
  single quotes (``''``). For example, to escape ``'T'``, write ``''T''``.

===========
Terminology
===========

When using KSQL, the following terminology is used.

Stream
------

A stream is an unbounded sequence of structured data (“facts”). For example, we could have a stream of financial transactions
such as “Alice sent $100 to Bob, then Charlie sent $50 to Bob”. Facts in a stream are immutable, which means new facts can
be inserted to a stream, but existing facts can never be updated or deleted. Streams can be created from an |ak-tm| topic or
derived from an existing stream. A stream’s underlying data is durably stored (persisted) within a Kafka topic on the Kafka
brokers.

Table
-----

A table is a view of a stream, or another table, and represents a collection of evolving facts. For example, we could have
a table that contains the latest financial information such as “Bob’s current account balance is $150”. It is the equivalent
of a traditional database table but enriched by streaming semantics such as windowing. Facts in a table are mutable, which
means new facts can be inserted to the table, and existing facts can be updated or deleted. Tables can be created from a
Kafka topic or derived from existing streams and tables. In both cases, a table’s underlying data is durably stored (persisted)
within a Kafka topic on the Kafka brokers.

.. _struct_overview:

STRUCT
------

In KSQL 5.0 and higher, you can read nested data, in Avro and JSON formats,
by using the ``STRUCT`` type in CREATE STREAM and CREATE TABLE statements.
You can use the ``STRUCT`` type in these KSQL statements: 

- CREATE STREAM/TABLE (from a topic)
- CREATE STREAM/TABLE AS SELECT (from existing streams/tables)
- SELECT (non-persistent query)

Use the following syntax to declare nested data: 

.. code:: sql

   STRUCT<FieldName FieldType, ...>

The ``STRUCT`` type requires you to specify a list of fields. For each field, you
specify the field name and field type. The field type can be any of the
supported KSQL types, including the complex types ``MAP``, ``ARRAY``, and
``STRUCT``.

.. note::

    ``Properties`` is not a valid field name.

Here's an example CREATE STREAM statement that uses a ``STRUCT`` to
encapsulate a street address and a postal code:

.. code:: sql

   CREATE STREAM orders (
     orderId BIGINT,
     address STRUCT<street VARCHAR, zip INTEGER>) WITH (...);

Access the fields in a ``STRUCT`` by using the dereference operator (``->``):

.. code:: sql

   SELECT address->city, address->zip FROM orders;

For more info, see :ref:`operators`.

.. note:: You can’t create new nested ``STRUCT`` data as the result of a query,
   but you can copy existing ``STRUCT`` fields as-is.

.. _ksql-time-units:

KSQL Time Units
---------------

The following list shows valid time units for the SIZE, ADVANCE BY, SESSION, and
WITHIN clauses.

* DAY, DAYS
* HOUR, HOURS
* MINUTE, MINUTES
* SECOND, SECONDS
* MILLISECOND, MILLISECONDS

For more information, see :ref:`windows_in_ksql_queries`.

.. _ksql-timestamp-formats:

KSQL Timestamp Formats
----------------------

Time-based operations, like windowing, process records according to the
timestamp in ``ROWTIME``. By default, the implicit ``ROWTIME`` column is the
timestamp of a message in a Kafka topic. Timestamps have an accuracy of
one millisecond.

Use the TIMESTAMP property to override ``ROWTIME`` with the contents of the
specified column. Define the format of a record's timestamp by using the
TIMESTAMP_FORMAT property.

If you use the TIMESTAMP property but don't set TIMESTAMP_FORMAT, KSQL assumes
that the timestamp field is a ``bigint``. If you set TIMESTAMP_FORMAT, the
TIMESTAMP field must be of type ``varchar`` and have a format that the
``DateTimeFormatter`` Java class can parse.

If your timestamp format has embedded single quotes, you can escape them by
using two successive single quotes, ``''``. For example, to escape ``'T'``,
write ``''T''``. The following examples show how to escape the ``'`` character
in KSQL statements.

.. code:: sql

    -- Example timestamp format: yyyy-MM-dd'T'HH:mm:ssX
    CREATE STREAM TEST (ID bigint, event_timestamp VARCHAR)
      WITH (kafka_topic='test_topic',
            value_format='JSON',
            timestamp='event_timestamp',
            timestamp_format='yyyy-MM-dd''T''HH:mm:ssX');

    -- Example timestamp format: yyyy.MM.dd G 'at' HH:mm:ss z
    CREATE STREAM TEST (ID bigint, event_timestamp VARCHAR)
      WITH (kafka_topic='test_topic',
            value_format='JSON',
            timestamp='event_timestamp',
            timestamp_format='yyyy.MM.dd G ''at'' HH:mm:ss z');

    -- Example timestamp format: hh 'o'clock' a, zzzz
    CREATE STREAM TEST (ID bigint, event_timestamp VARCHAR)
      WITH (kafka_topic='test_topic',
            value_format='JSON',
            timestamp='event_timestamp',
            timestamp_format='hh ''o''clock'' a, zzzz');

For more information on timestamp formats, see `DateTimeFormatter <https://cnfl.io/java-dtf>`__.

=================
KSQL CLI Commands
=================

The KSQL CLI commands can be run after :ref:`starting the KSQL CLI <install_ksql-cli>`. You can view the KSQL CLI help by
running ``<path-to-confluent>/bin/ksql --help``.

**Tip:** You can search and browse your command history in the KSQL CLI with ``Ctrl-R``. After pressing ``Ctrl-R``, start
typing the command or any part of the command to show an auto-complete of past commands.

.. code:: bash

    NAME
            ksql - KSQL CLI

    SYNOPSIS
            ksql [ --config-file <configFile> ] [ {-h | --help} ]
                    [ --output <outputFormat> ]
                    [ --query-row-limit <streamedQueryRowLimit> ]
                    [ --query-timeout <streamedQueryTimeoutMs> ] [--] <server>

    OPTIONS
            --config-file <configFile>
                A file specifying configs for Ksql and its underlying Kafka Streams
                instance(s). Refer to KSQL documentation for a list of available
                configs.

            -h, --help
                Display help information

            --output <outputFormat>
                The output format to use (either 'JSON' or 'TABULAR'; can be changed
                during REPL as well; defaults to TABULAR)

            --query-row-limit <streamedQueryRowLimit>
                An optional maximum number of rows to read from streamed queries

                This options value must fall in the following range: value >= 1


            --query-timeout <streamedQueryTimeoutMs>
                An optional time limit (in milliseconds) for streamed queries

                This options value must fall in the following range: value >= 1


            --
                This option can be used to separate command-line options from the
                list of arguments (useful when arguments might be mistaken for
                command-line options)

            <server>
                The address of the Ksql server to connect to (ex:
                http://confluent.io:9098)

                This option may occur a maximum of 1 times

RUN SCRIPT
----------

You can run a list of predefined queries and commands from in a file by using the RUN SCRIPT command.

Example:

.. code:: sql

    RUN SCRIPT '/local/path/to/queries.sql';

The RUN SCRIPT command supports a subset of KSQL statements:

- Persistent queries: :ref:`create-stream`, :ref:`create-table`, :ref:`create-stream-as-select`, :ref:`create-table-as-select`
- :ref:`drop-stream` and :ref:`drop-table`
- SET, UNSET statements
- INSERT INTO statement

It does not support statements such as:

- SHOW TOPICS and SHOW STREAMS etc
- TERMINATE
- Non-persistent queries: SELECT etc

RUN SCRIPT can also be used from the command line, for instance when writing shell scripts.
For more information, see :ref:`running-ksql-command-line`.


.. _data-types:

===============
KSQL data types
===============

KSQL supports the following data types:

Primitive Types
---------------

KSQL supports the following primitive data types:

-  ``BOOLEAN``
-  ``INTEGER`` or [``INT``]
-  ``BIGINT``
-  ``DOUBLE``
-  ``VARCHAR`` (or ``STRING``)


Array
-----

``ARRAY<ElementType>``

.. note:: The ``DELIMITED`` format doesn't support arrays.

KSQL supports fields that are arrays of another type. All the elements in the array must be of the
same type. The element type can be any valid KSQL type.

You can define arrays within a ``CREATE TABLE`` or ``CREATE STREAM`` statement by using the syntax
``ARRAY<ElementType>``. For example, ``ARRAY<INT>`` defines an array of integers.

The elements of an array are zero-indexed and can be accessed by using the ``[]`` operator passing
in the index. For example, ``SOME_ARRAY[0]`` retrieves the first element from the array.
For more information, see :ref:`operators`.

Map
---

``MAP<KeyType, ValueType>``

.. note:: The ``DELIMITED`` format doesn't support maps.

KSQL supports fields that are maps. A map has a key and value type. All of the keys must be of the
same type, and all of the values must be also be of the same type. Currently only ``STRING`` keys
are supported. The value type can be any valid KSQL type.

You can define maps within a ``CREATE TABLE`` or ``CREATE STREAM`` statement by using the syntax
``MAP<KeyType, ValueType>``. For example, ``MAP<STRING, INT>`` defines a map with string keys and
integer values.

Access the values of a map by using the ``[]`` operator and passing in the key. For example,
``SOME_MAP['cost']`` retrieves the value for the entry with key ``cost``, or ``null``
For more information, see :ref:`operators`.

Struct
------

``STRUCT<FieldName FieldType, ...>``

.. note:: The ``DELIMITED`` format doesn't support structs.

KSQL supports fields that are structs. A struct represents strongly typed structured data. A struct
is an ordered collection of named fields that have a specific type. The field types can be any valid
KSQL type.

You can define a structs within a ``CREATE TABLE`` or ``CREATE STREAM`` statement by using the syntax
``STRUCT<FieldName FieldType, ...>``. For example, ``STRUCT<ID BIGINT, NAME STRING, AGE INT>``
defines a struct with three fields, with the supplied name and type.

Access the fields of a struct by using the ``->`` operator. For example, ``SOME_STRUCT->ID``
retrieves the value of the struct's ``ID`` field. For more information, see :ref:`operators`.


===============
KSQL statements
===============

.. tip::

    - KSQL statements must be terminated with a semicolon (``;``).
    - Statements can be spread over multiple lines.
    - The hyphen character, ``-``, isn't supported in names for streams, tables,
      topics, and columns.
    - Don't use quotes around stream names or table names when you CREATE them.

.. _create-stream:

CREATE STREAM
-------------

**Synopsis**

.. code:: sql

    CREATE STREAM stream_name ( { column_name data_type } [, ...] )
      WITH ( property_name = expression [, ...] );

**Description**

Create a new stream with the specified columns and properties. Columns can be any of the
:ref:`data types <data-types>` supported by KSQL.

KSQL adds the implicit columns ``ROWTIME`` and ``ROWKEY`` to every
stream and table, which represent the corresponding Kafka message
timestamp and message key, respectively. The timestamp has milliseconds accuracy.

The WITH clause supports the following properties:

+-------------------------+--------------------------------------------------------------------------------------------+
| Property                | Description                                                                                |
+=========================+============================================================================================+
| KAFKA_TOPIC (required)  | The name of the Kafka topic that backs this source. The topic must either already exist in |
|                         | Kafka, or PARTITIONS must be specified to create the topic. Command will fail if the topic |
|                         | exists with different partition/replica counts.                                            |
+-------------------------+--------------------------------------------------------------------------------------------+
| VALUE_FORMAT (required) | Specifies the serialization format of the message value in the topic. Supported formats:   |
|                         | ``JSON``, ``DELIMITED`` (comma-separated value), ``AVRO`` and ``KAFKA``.                   |
|                         | For more information, see :ref:`ksql_formats`.                                             |
+-------------------------+--------------------------------------------------------------------------------------------+
| PARTITIONS              | The number of partitions in the backing topic. This property must be set if creating a     |
|                         | STREAM without an existing topic (the command will fail if the topic does not exist).      |
+-------------------------+--------------------------------------------------------------------------------------------+
| REPLICAS                | The number of replicas in the backing topic. If this property is not set but PARTITIONS is |
|                         | set, then the default Kafka cluster configuration for replicas will be used for creating a |
|                         | new topic.                                                                                 |
+-------------------------+--------------------------------------------------------------------------------------------+
| KEY                     | Optimization hint: If the Kafka message key is also present as a field/column in the Kafka |
|                         | message value, you may set this property to associate the corresponding field/column with  |
|                         | the implicit ``ROWKEY`` column (message key).                                              |
|                         | If set, KSQL uses it as an optimization hint to determine if repartitioning can be avoided |
|                         | when performing aggregations and joins.                                                    |
|                         | You can only use this if the key format in Kafka is ``VARCHAR`` or ``STRING``. Do not use  |
|                         | this hint if the message key format in Kafka is ``AVRO`` or ``JSON``.                      |
|                         | See :ref:`ksql_key_requirements` for more information.                                     |
+-------------------------+--------------------------------------------------------------------------------------------+
| TIMESTAMP               | By default, the implicit ``ROWTIME`` column is the timestamp of the message in the Kafka   |
|                         | topic. The TIMESTAMP property can be used to override ``ROWTIME`` with the contents of the |
|                         | specified field/column within the Kafka message value (similar to timestamp extractors     |
|                         | in Kafka's Streams API). Timestamps have a millisecond accuracy. Time-based operations,    |
|                         | such as windowing, will process a record according to the timestamp in ``ROWTIME``.        |
+-------------------------+--------------------------------------------------------------------------------------------+
| TIMESTAMP_FORMAT        | Used in conjunction with TIMESTAMP. If not set will assume that the timestamp field is a   |
|                         | bigint. If it is set, then the TIMESTAMP field must be of type varchar and have a format   |
|                         | that can be parsed with the java ``DateTimeFormatter``. If your timestamp format has       |
|                         | characters requiring single quotes, you can escape them with successive single quotes,     |
|                         | ``''``, for example: ``'yyyy-MM-dd''T''HH:mm:ssX'``. For more information on timestamp     |
|                         | formats, see `DateTimeFormatter <https://cnfl.io/java-dtf>`__.                             |
+-------------------------+--------------------------------------------------------------------------------------------+
| WRAP_SINGLE_VALUE       | Controls how values are deserialized where the value schema contains only a single field.  |
|                         |                                                                                            |
|                         | The setting controls how KSQL will deserialize the value of the records in the supplied    |
|                         | ``KAFKA_TOPIC`` that contain only a single field.                                          |
|                         | If set to ``true``, KSQL expects the field to have been serialized as a named field        |
|                         | within a record.                                                                           |
|                         | If set to ``false``, KSQL expects the field to have been serialized as an anonymous value. |
|                         | If not supplied, the system default, defined by :ref:`ksql-persistence-wrap-single-values` |
|                         | and defaulting to ``true```, is used.                                                      |
|                         |                                                                                            |
|                         | Note: ``null`` values have special meaning in KSQL. Care should be taken when dealing with |
|                         | single-field schemas where the value can be ``null`. For more information, see             |
|                         | :ref:`ksql_single_field_wrapping`.                                                         |
|                         |                                                                                            |
|                         | Note: Supplying this property for formats that do not support wrapping, for example        |
|                         | ``DELIMITED``, or when the value schema has multiple fields, will result in an error.      |
+-------------------------+--------------------------------------------------------------------------------------------+
| WINDOW_TYPE             | By default, the topic is assumed to contain non-windowed data. If the data is windowed,    |
|                         | i.e., was created using KSQL using a query that contains a ``WINDOW`` clause, then the     |
|                         | ``WINDOW_TYPE`` property can be used to provide the window type. Valid values are          |
|                         | ``SESSION``, ``HOPPING`, and ``TUMBLING``.                                                 |
+-------------------------+--------------------------------------------------------------------------------------------+
| WINDOW_SIZE             | By default, the topic is assumed to contain non-windowed data. If the data is windowed,    |
|                         | i.e., was created using KSQL using a query that contains a ``WINDOW`` clause, and the      |
|                         | ``WINDOW_TYPE`` property is TUMBLING or HOPPING, then the WINDOW_SIZE property should be   |
|                         | set. The property is a string with two literals, window size (a number) and window size    |
|                         | unit (a time unit). For example: '10 SECONDS'.                                             |
+-------------------------+--------------------------------------------------------------------------------------------+

For more information on timestamp formats, see
`DateTimeFormatter <https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html>`__.

.. include:: ../includes/ksql-includes.rst
    :start-after: Avro_note_start
    :end-before: Avro_note_end

Example:

.. code:: sql

    CREATE STREAM pageviews (viewtime BIGINT, user_id VARCHAR, page_id VARCHAR)
      WITH (VALUE_FORMAT = 'JSON',
            KAFKA_TOPIC = 'my-pageviews-topic');

If the name of a column in your source topic is one of the reserved words in KSQL you can use back quotes to define the column.
The same applies to the field names in a STRUCT type.
For instance, if in the above example we had another field called ``Properties``, which is a reserved word in KSQL, you can
use the following statement to declare your stream:

.. code:: sql

    CREATE STREAM pageviews (viewtime BIGINT, user_id VARCHAR, page_id VARCHAR, `Properties` VARCHAR)
      WITH (VALUE_FORMAT = 'JSON',
            KAFKA_TOPIC = 'my-pageviews-topic');

.. _create-table:

CREATE TABLE
------------

**Synopsis**

.. code:: sql

    CREATE TABLE table_name ( { column_name data_type } [, ...] )
      WITH ( property_name = expression [, ...] );

**Description**

Create a new table with the specified columns and properties. Columns can be any of the
:ref:`data types <data-types>` supported by KSQL.

KSQL adds the implicit columns ``ROWTIME`` and ``ROWKEY`` to every
stream and table, which represent the corresponding Kafka message
timestamp and message key, respectively. The timestamp has milliseconds accuracy.

When creating a table from a Kafka topic, KSQL requries the message key to be a ``VARCHAR`` aka ``STRING``. If the message
key is not of this type follow the instructions in :ref:`ksql_key_requirements`.

The WITH clause supports the following properties:

+-------------------------+--------------------------------------------------------------------------------------------+
| Property                | Description                                                                                |
+=========================+============================================================================================+
| KAFKA_TOPIC (required)  | The name of the Kafka topic that backs this source. The topic must either already exist in |
|                         | Kafka, or PARTITIONS must be specified to create the topic. Command will fail if the topic |
|                         | exists with different partition/replica counts.                                            |
+-------------------------+--------------------------------------------------------------------------------------------+
| VALUE_FORMAT (required) | Specifies the serialization format of message values in the topic. Supported formats:      |
|                         | ``JSON``, ``DELIMITED`` (comma-separated value), ``AVRO`` and ``KAFKA``.                   |
|                         | For more information, see :ref:`ksql_formats`.                                             |
+-------------------------+--------------------------------------------------------------------------------------------+
| PARTITIONS              | The number of partitions in the backing topic. This property must be set if creating a     |
|                         | TABLE without an existing topic (the command will fail if the topic does not exist).       |
+-------------------------+--------------------------------------------------------------------------------------------+
| REPLICAS                | The number of replicas in the backing topic. If this property is not set but PARTITIONS is |
|                         | set, then the default Kafka cluster configuration for replicas will be used for creating a |
|                         | new topic.                                                                                 |
+-------------------------+--------------------------------------------------------------------------------------------+
| KEY                     | Optimization hint: If the Kafka message key is also present as a field/column in the Kafka |
|                         | message value, you may set this property to associate the corresponding field/column with  |
|                         | the implicit ``ROWKEY`` column (message key).                                              |
|                         | If set, KSQL uses it as an optimization hint to determine if repartitioning can be avoided |
|                         | when performing aggregations and joins.                                                    |
|                         | You can only use this if the key format in kafka is ``VARCHAR`` or ``STRING``. Do not use  |
|                         | this hint if the message key format in kafka is AVRO or JSON.                              |
|                         | For more information, see :ref:`ksql_key_requirements`.                                    |
+-------------------------+--------------------------------------------------------------------------------------------+
| TIMESTAMP               | By default, the implicit ``ROWTIME`` column is the timestamp of the message in the Kafka   |
|                         | topic. The TIMESTAMP property can be used to override ``ROWTIME`` with the contents of the |
|                         | specified field/column within the Kafka message value (similar to timestamp extractors in  |
|                         | Kafka's Streams API). Timestamps have a millisecond accuracy. Time-based operations, such  |
|                         | as windowing, will process a record according to the timestamp in ``ROWTIME``.             |
+-------------------------+--------------------------------------------------------------------------------------------+
| TIMESTAMP_FORMAT        | Used in conjunction with TIMESTAMP. If not set will assume that the timestamp field is a   |
|                         | bigint. If it is set, then the TIMESTAMP field must be of type varchar and have a format   |
|                         | that can be parsed with the Java ``DateTimeFormatter``. If your timestamp format has       |
|                         | characters requiring single quotes, you can escape them with two successive single quotes, |
|                         | ``''``, for example: ``'yyyy-MM-dd''T''HH:mm:ssX'``. For more information on timestamp     |
|                         | formats, see `DateTimeFormatter <https://cnfl.io/java-dtf>`__.                             |
+-------------------------+--------------------------------------------------------------------------------------------+
| WRAP_SINGLE_VALUE       | Controls how values are deserialized where the values schema contains only a single field. |
|                         |                                                                                            |
|                         | The setting controls how KSQL will deserialize the value of the records in the supplied    |
|                         | ``KAFKA_TOPIC`` that contain only a single field.                                          |
|                         | If set to ``true``, KSQL expects the field to have been serialized as named field          |
|                         | within a record.                                                                           |
|                         | If set to ``false``, KSQL expects the field to have been serialized as an anonymous value. |
|                         | If not supplied, the system default, defined by :ref:`ksql-persistence-wrap-single-values` |
|                         | and defaulting to ``true```, is used.                                                      |
|                         |                                                                                            |
|                         | Note: ``null`` values have special meaning in KSQL. Care should be taken when dealing with |
|                         | single-field schemas where the value can be ``null`. For more information, see             |
|                         | :ref:`ksql_single_field_wrapping`.                                                         |
|                         |                                                                                            |
|                         | Note: Supplying this property for formats that do not support wrapping, for example        |
|                         | ``DELIMITED``, or when the value schema has multiple fields, will result in an error.      |
+-------------------------+--------------------------------------------------------------------------------------------+
| WINDOW_TYPE             | By default, the topic is assumed to contain non-windowed data. If the data is windowed,    |
|                         | i.e. was created using KSQL using a query that contains a ``WINDOW`` clause, then the      |
|                         | ``WINDOW_TYPE`` property can be used to provide the window type. Valid values are          |
|                         | ``SESSION``, ``HOPPING`, and ``TUMBLING``.                                                 |
+-------------------------+--------------------------------------------------------------------------------------------+
| WINDOW_SIZE             | By default, the topic is assumed to contain non-windowed data. If the data is windowed,    |
|                         | i.e., was created using KSQL using a query that contains a ``WINDOW`` clause, and the      |
|                         | ``WINDOW_TYPE`` property is TUMBLING or HOPPING, then the WINDOW_SIZE property should be   |
|                         | set. The property is a string with two literals, window size (a number) and window size    |
|                         | unit (a time unit). For example: '10 SECONDS'.                                             |
+-------------------------+--------------------------------------------------------------------------------------------+

.. include:: ../includes/ksql-includes.rst
    :start-after: Avro_note_start
    :end-before: Avro_note_end

Example:

.. code:: sql

    CREATE TABLE users (usertimestamp BIGINT, user_id VARCHAR, gender VARCHAR, region_id VARCHAR) WITH (
        KAFKA_TOPIC = 'my-users-topic',
        KEY = 'user_id');

If the name of a column in your source topic is one of the reserved words in KSQL you can use back quotes to define the column.
The same applies to the field names in a STRUCT type.
For instance, if in the above example we had another field called ``Properties``, which is a reserved word in KSQL, you can
use the following statement to declare your table:

.. code:: sql

    CREATE TABLE users (usertimestamp BIGINT, user_id VARCHAR, gender VARCHAR, region_id VARCHAR, `Properties` VARCHAR) WITH (
            KAFKA_TOPIC = 'my-users-topic',
            KEY = 'user_id');

.. _create-stream-as-select:

CREATE STREAM AS SELECT
-----------------------

**Synopsis**

.. code:: sql

    CREATE STREAM stream_name
      [WITH ( property_name = expression [, ...] )]
      AS SELECT  select_expr [, ...]
      FROM from_stream
      [ LEFT | FULL | INNER ] JOIN [join_table | join_stream] [ WITHIN [(before TIMEUNIT, after TIMEUNIT) | N TIMEUNIT] ] ON join_criteria 
      [ WHERE condition ]
      [PARTITION BY column_name];

**Description**

Create a new stream along with the corresponding Kafka topic, and
continuously write the result of the SELECT query into the stream and
its corresponding topic.

If the PARTITION BY clause is present, then the resulting stream will
have the specified column as its key. The `column_name` must be present
in the `select_expr`. For more information, see :ref:`partition-data-to-enable-joins`.

For joins, the key of the resulting stream will be the value from the column
from the left stream that was used in the join criteria. This column will be
registered as the key of the resulting stream if included in the selected
columns.

For stream-table joins, the column used in the join criteria for the table
must be the table key.

For stream-stream joins, you can specify an optional WITHIN clause for matching
records that both occur within a specified time interval. For valid time units,
see :ref:`ksql-time-units`.

For more information, see :ref:`join-streams-and-tables`.

The WITH clause for the result supports the following properties:

+-------------------------+------------------------------------------------------------------------------------------------------+
| Property                | Description                                                                                          |
+=========================+======================================================================================================+
| KAFKA_TOPIC             | The name of the Kafka topic that backs this stream. If this property is not set, then the            |
|                         | name of the stream in upper case will be used as default.                                            |
+-------------------------+------------------------------------------------------------------------------------------------------+
| VALUE_FORMAT            | Specifies the serialization format of the message value in the topic. Supported formats:             |
|                         | ``JSON``, ``DELIMITED`` (comma-separated value), ``AVRO`` and ``KAFKA``.                             |
|                         | If this property is not set, then the format of the input stream/table is used.                      |
|                         | For more information, see :ref:`ksql_formats`.                                                       |
+-------------------------+------------------------------------------------------------------------------------------------------+
| PARTITIONS              | The number of partitions in the backing topic. If this property is not set, then the number          |
|                         | of partitions of the input stream/table will be used. In join queries, the property values are taken |
|                         | from the left-side stream or table.                                                                  |
|                         | For KSQL 5.2 and earlier, if the property is not set, the value of the ``ksql.sink.partitions``      |
|                         | property, which defaults to four partitions, will be used. The ``ksql.sink.partitions`` property can |
|                         | be set in the properties file the KSQL server is started with, or by using the ``SET`` statement.    |
+-------------------------+------------------------------------------------------------------------------------------------------+
| REPLICAS                | The replication factor for the topic. If this property is not set, then the number of                |
|                         | replicas of the input stream or table will be used. In join queries, the property values are taken   |
|                         | from the left-side stream or table.                                                                  |
|                         | For KSQL 5.2 and earlier, if the REPLICAS is not set, the value of the ``ksql.sink.replicas``        |
|                         | property, which defaults to one replica, will be used. The ``ksql.sink.replicas`` property can       |
|                         | be set in the properties file the KSQL server is started with, or by using the ``SET`` statement.    |
+-------------------------+------------------------------------------------------------------------------------------------------+
| TIMESTAMP               | Sets a field within this stream's schema to be used as the default source of ``ROWTIME`` for         |
|                         | any downstream queries. Downstream queries that use time-based operations, such as windowing,        |
|                         | will process records in this stream based on the timestamp in this field. By default,                |
|                         | such queries will also use this field to set the timestamp on any records emitted to Kafka.          |
|                         | Timestamps have a millisecond accuracy.                                                              |
|                         |                                                                                                      |
|                         | If not supplied, the ``ROWTIME`` of the source stream will be used.                                  |
|                         |                                                                                                      |
|                         | **Note**: This doesn't affect the processing of the query that populates this stream.                |
|                         | For example, given the following statement:                                                          |
|                         |                                                                                                      |
|                         | .. literalinclude:: ../includes/csas-snippet.sql                                                     |
|                         |    :language: sql                                                                                    |
|                         |                                                                                                      |
|                         | The window into which each row of ``bar`` is placed is determined by bar's ``ROWTIME``, not ``t2``.  |
+-------------------------+------------------------------------------------------------------------------------------------------+
| TIMESTAMP_FORMAT        | Used in conjunction with TIMESTAMP. If not set will assume that the timestamp field is a             |
|                         | bigint. If it is set, then the TIMESTAMP field must be of type varchar and have a format             |
|                         | that can be parsed with the Java ``DateTimeFormatter``. If your timestamp format has                 |
|                         | characters requiring single quotes, you can escape them with two successive single quotes,           |
|                         | ``''``, for example: ``'yyyy-MM-dd''T''HH:mm:ssX'``. For more information on timestamp               |
|                         | formats, see `DateTimeFormatter <https://cnfl.io/java-dtf>`__.                                       |
+-------------------------+------------------------------------------------------------------------------------------------------+
| WRAP_SINGLE_VALUE       | Controls how values are serialized where the values schema contains only a single field.             |
|                         |                                                                                                      |
|                         | The setting controls how the query will serialize values with a single-field schema.                 |
|                         | If set to ``true``, KSQL will serialize the field as a named field within a record.                  |
|                         | If set to ``false`` KSQL, KSQL will serialize the field as an anonymous value.                       |
|                         | If not supplied, the system default, defined by :ref:`ksql-persistence-wrap-single-values` and       |
|                         | defaulting to ``true```, is used.                                                                    |
|                         |                                                                                                      |
|                         | Note: ``null`` values have special meaning in KSQL. Care should be taken when dealing with           |
|                         | single-field schemas where the value can be ``null`. For more information, see                       |
|                         | :ref:`ksql_single_field_wrapping`.                                                                   |
|                         |                                                                                                      |
|                         | Note: Supplying this property for formats that do not support wrapping, for example                  |
|                         | ``DELIMITED``, or when the value schema has multiple fields, will result in an error.                |
+-------------------------+------------------------------------------------------------------------------------------------------+

.. include:: ../includes/ksql-includes.rst
    :start-after: Avro_note_start
    :end-before: Avro_note_end

.. note:: The ``KEY`` property is not supported – use PARTITION BY instead.

.. _create-table-as-select:

CREATE TABLE AS SELECT
----------------------

**Synopsis**

.. code:: sql

    CREATE TABLE table_name
      [WITH ( property_name = expression [, ...] )]
      AS SELECT  select_expr [, ...]
      FROM from_item
      [ LEFT | FULL | INNER ] JOIN join_table ON join_criteria 
      [ WINDOW window_expression ]
      [ WHERE condition ]
      [ GROUP BY grouping_expression ]
      [ HAVING having_expression ];

**Description**

Create a new KSQL table along with the corresponding Kafka topic and
stream the result of the SELECT query as a changelog into the topic.
Note that the WINDOW clause can only be used if the ``from_item`` is a stream.

For joins, the key of the resulting table will be the value from the column
from the left table that was used in the join criteria. This column will be
registered as the key of the resulting table if included in the selected
columns.

For joins, the columns used in the join criteria must be the keys of the tables
being joined.

For more information, see :ref:`join-streams-and-tables`.

The WITH clause supports the following properties:

+-------------------------+------------------------------------------------------------------------------------------------------+
| Property                | Description                                                                                          |
+=========================+======================================================================================================+
| KAFKA_TOPIC             | The name of the Kafka topic that backs this table. If this property is not set, then the             |
|                         | name of the table will be used as default.                                                           |
+-------------------------+------------------------------------------------------------------------------------------------------+
| VALUE_FORMAT            | Specifies the serialization format of the message value in the topic. Supported formats:             |
|                         | ``JSON``, ``DELIMITED`` (comma-separated value), ``AVRO`` and ``KAFKA``.                             |
|                         | If this property is not set, then the format of the input stream/table is used.                      |
|                         | For more information, see :ref:`ksql_formats`.                                                       |
+-------------------------+------------------------------------------------------------------------------------------------------+
| PARTITIONS              | The number of partitions in the backing topic. If this property is not set, then the number          |
|                         | of partitions of the input stream/table will be used. In join queries, the property values are taken |
|                         | from the left-side stream or table.                                                                  |
|                         | For KSQL 5.2 and earlier, if the property is not set, the value of the ``ksql.sink.partitions``      |
|                         | property, which defaults to four partitions, will be used. The ``ksql.sink.partitions`` property can |
|                         | be set in the properties file the KSQL server is started with, or by using the ``SET`` statement.    |
+-------------------------+------------------------------------------------------------------------------------------------------+
| REPLICAS                | The replication factor for the topic. If this property is not set, then the number of                |
|                         | replicas of the input stream or table will be used. In join queries, the property values are taken   |
|                         | from the left-side stream or table.                                                                  |
|                         | For KSQL 5.2 and earlier, if the REPLICAS is not set, the value of the ``ksql.sink.replicas``        |
|                         | property, which defaults to one replica, will be used. The ``ksql.sink.replicas`` property can       |
|                         | be set in the properties file the KSQL server is started with, or by using the ``SET`` statement.    |
+-------------------------+------------------------------------------------------------------------------------------------------+
| TIMESTAMP               | Sets a field within this tables's schema to be used as the default source of ``ROWTIME`` for         |
|                         | any downstream queries. Downstream queries that use time-based operations, such as windowing,        |
|                         | will process records in this stream based on the timestamp in this field.                            |
|                         | Timestamps have a millisecond accuracy.                                                              |
|                         |                                                                                                      |
|                         | If not supplied, the ``ROWTIME`` of the source stream will be used.                                  |
|                         |                                                                                                      |
|                         | **Note**: This doesn't affect the processing of the query that populates this table.                 |
|                         | For example, given the following statement:                                                          |
|                         |                                                                                                      |
|                         | .. literalinclude:: ../includes/ctas-snippet.sql                                                     |
|                         |    :language: sql                                                                                    |
|                         |                                                                                                      |
|                         | The window into which each row of ``bar`` is placed is determined by bar's ``ROWTIME``, not ``t2``.  |
+-------------------------+------------------------------------------------------------------------------------------------------+
| TIMESTAMP_FORMAT        | Used in conjunction with TIMESTAMP. If not set will assume that the timestamp field is a             |
|                         | bigint. If it is set, then the TIMESTAMP field must be of type varchar and have a format             |
|                         | that can be parsed with the Java ``DateTimeFormatter``. If your timestamp format has                 |
|                         | characters requiring single quotes, you can escape them with two successive single quotes,           |
|                         | ``''``, for example: ``'yyyy-MM-dd''T''HH:mm:ssX'``. For more information on timestamp               |
|                         | formats, see `DateTimeFormatter <https://cnfl.io/java-dtf>`__.                                       |
+-------------------------+------------------------------------------------------------------------------------------------------+
| WRAP_SINGLE_VALUE       | Controls how values are serialized where the values schema contains only a single field.             |
|                         |                                                                                                      |
|                         | The setting controls how the query will serialize values with a single-field schema.                 |
|                         | If set to ``true``, KSQL will serialize the field as a named field within a record.                  |
|                         | If set to ``false`` KSQL, KSQL will serialize the field as an anonymous value.                       |
|                         | If not supplied, the system default, defined by :ref:`ksql-persistence-wrap-single-values` and       |
|                         | defaulting to ``true```, is used.                                                                    |
|                         |                                                                                                      |
|                         | Note: ``null`` values have special meaning in KSQL. Care should be taken when dealing with           |
|                         | single-field schemas where the value can be ``null`. For more information, see                       |
|                         | :ref:`ksql_single_field_wrapping`.                                                                   |
|                         |                                                                                                      |
|                         | Note: Supplying this property for formats that do not support wrapping, for example                  |
|                         | ``DELIMITED``, or when the value schema has multiple fields, will result in an error.                |
+-------------------------+------------------------------------------------------------------------------------------------------+

.. include:: ../includes/ksql-includes.rst
    :start-after: Avro_note_start
    :end-before: Avro_note_end

.. _insert-into:

INSERT INTO
-----------

**Synopsis**

.. code:: sql

    INSERT INTO stream_name
      SELECT select_expr [., ...]
      FROM from_stream
      [ WHERE condition ]
      [ PARTITION BY column_name ];

**Description**

Stream the result of the SELECT query into an existing stream and its underlying topic.

The schema and partitioning column produced by the query must match the stream's schema
and key, respectively. If the schema and partitioning column are incompatible with the
stream, then the statement will return an error.

stream_name and from_item must both refer to a Stream. Tables are not supported.

Records written into the stream are not timestamp-ordered with respect to other queries.
Therefore, the topic partitions of the output stream may contain out-of-order records even
if the source stream for the query is ordered by timestamp.


INSERT VALUES
-------------

**Synopsis**

.. code:: sql

    INSERT INTO <stream_name|table_name> [(column_name [, ...]])]
      VALUES (value [,...]);

**Description**

Produce a row into an existing stream or table and its underlying topic based on
explicitly specified values. The first ``column_name`` of every schema is ``ROWKEY``, which
defines the corresponding Kafka key. If the source specifies a ``key`` and that column is present
in the column names for this INSERT statement then that value and the ``ROWKEY`` value are expected
to match, otherwise the value from ``ROWKEY`` will be copied into the value of the key column (or
conversely from the key column into the ``ROWKEY`` column).

Any column not explicitly given a value is set to ``null``.  If no columns are specified, a value
for every column is expected in the same order as the schema with ``ROWKEY`` as the first column.
If columns are specified, the order does not matter.

.. note:: ``ROWTIME`` may be specified as an explicit column, but is not required when omitting the
  column specifications.

For example, the statements below would all be valid for a source with schema
``<KEY_COL VARCHAR, COL_A VARCHAR>`` with ``KEY=KEY_COL``:

  .. code:: sql

      // inserts (1234, "key", "key", "A")
      INSERT INTO foo (ROWTIME, ROWKEY, KEY_COL, COL_A) VALUES (1234, 'key', 'key', 'A');

      // inserts (current_time(), "key", "key", "A")
      INSERT INTO foo VALUES ('key', 'key', 'A');

      // inserts (current_time(), "key", "key", "A")
      INSERT INTO foo (KEY_COL, COL_A) VALUES ('key', 'A');

      // inserts (current_time(), "key", "key", null)
      INSERT INTO foo (KEY_COL) VALUES ('key');

The values will serialize using the ``value_format`` specified in the original ``CREATE`` statement.
The key will always be serialized as a String.

.. _ksql-syntax-describe:

DESCRIBE
--------

**Synopsis**

.. code:: sql

    DESCRIBE [EXTENDED] (stream_name|table_name);

**Description**

* DESCRIBE: List the columns in a stream or table along with their data type and other attributes.
* DESCRIBE EXTENDED: Display DESCRIBE information with additional runtime statistics, Kafka topic details, and the
  set of queries that populate the table or stream.

Extended descriptions provide the following metrics for the topic backing the source being described.

+------------------------------+------------------------------------------------------------------------------------------------------+
| KSQL Metric                  | Description                                                                                          |
+==============================+======================================================================================================+
| consumer-failed-messages     | Total number of failures during message consumption on the server.                                   |
+------------------------------+------------------------------------------------------------------------------------------------------+
| consumer-messages-per-sec    | The number of messages consumed per second from the topic by the server.                             |
+------------------------------+------------------------------------------------------------------------------------------------------+
| consumer-total-message-bytes | Total number of bytes consumed from the topic by the server.                                         |
+------------------------------+------------------------------------------------------------------------------------------------------+
| consumer-total-messages      | Total number of messages consumed from the topic by the server.                                      |
+------------------------------+------------------------------------------------------------------------------------------------------+
| failed-messages-per-sec      | Number of failures during message consumption (for example, deserialization failures)                |
|                              | per second on the server.                                                                            |
+------------------------------+------------------------------------------------------------------------------------------------------+
| last-failed                  | Time that the last failure occured when a message was consumed from the topic by the server.         |
+------------------------------+------------------------------------------------------------------------------------------------------+
| last-message                 | Time that the last message was produced to or consumed from the topic by the server.                 |
+------------------------------+------------------------------------------------------------------------------------------------------+
| messages-per-sec             | Number of messages produced per second into the topic by the server.                                 |
+------------------------------+------------------------------------------------------------------------------------------------------+
| total-messages               | Total number of messages produced into the topic by the server.                                      |
+------------------------------+------------------------------------------------------------------------------------------------------+
| total-message-bytes          | Total number of bytes produced into the topic by the server.                                         |
+------------------------------+------------------------------------------------------------------------------------------------------+

Example of describing a table:

.. code:: sql

    DESCRIBE ip_sum;

Your output should resemble:

::

     Field   | Type
    -------------------------------------
     ROWTIME | BIGINT           (system)
     ROWKEY  | VARCHAR(STRING)  (system)
     IP      | VARCHAR(STRING)  (key)
     KBYTES  | BIGINT
    -------------------------------------
    For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>

Example of describing a table with extended information:

.. code:: bash

    DESCRIBE EXTENDED ip_sum;

Your output should resemble:

::

    Type                 : TABLE
    Key field            : CLICKSTREAM.IP
    Timestamp field      : Not set - using <ROWTIME>
    Key format           : STRING
    Value format         : JSON
    Kafka output topic   : IP_SUM (partitions: 4, replication: 1)

     Field   | Type
    -------------------------------------
     ROWTIME | BIGINT           (system)
     ROWKEY  | VARCHAR(STRING)  (system)
     IP      | VARCHAR(STRING)  (key)
     KBYTES  | BIGINT
    -------------------------------------

    Queries that write into this TABLE
    -----------------------------------
    id:CTAS_IP_SUM - CREATE TABLE IP_SUM as SELECT ip,  sum(bytes)/1024 as kbytes FROM CLICKSTREAM window SESSION (300 second) GROUP BY ip;

    For query topology and execution plan run: EXPLAIN <QueryId>; for more information

    Local runtime statistics
    ------------------------
    messages-per-sec:      4.41   total-messages:       486     last-message: 12/14/17 4:32:23 PM GMT
     failed-messages:         0      last-failed:       n/a
    (Statistics of the local KSQL server interaction with the Kafka topic IP_SUM)

DESCRIBE FUNCTION
-----------------

**Synopsis**

.. code:: sql

    DESCRIBE FUNCTION function_name;

**Description**

Provides a description of a function including input parameters and the return type.

.. _ksql-syntax-explain:

EXPLAIN
-------

**Synopsis**

.. code:: sql

    EXPLAIN (sql_expression|query_id);

**Description**

Show the execution plan for a SQL expression or, given the ID of a running query, show the execution plan plus
additional runtime information and metrics. Statements such as DESCRIBE EXTENDED, for example, show the IDs of
queries related to a stream or table.

Example of explaining a running query:

.. code:: sql

    EXPLAIN ctas_ip_sum;

Your output should resemble:

::

    Type                 : QUERY
    SQL                  : CREATE TABLE IP_SUM as SELECT ip,  sum(bytes)/1024 as kbytes FROM CLICKSTREAM window SESSION (300 second) GROUP BY ip;


    Local runtime statistics
    ------------------------
    messages-per-sec:     104.38   total-messages:       14238     last-message: 12/14/17 4:30:42 PM GMT
     failed-messages:          0      last-failed:         n/a
    (Statistics of the local Ksql Server interaction with the Kafka topic IP_SUM)

    Execution plan
    --------------
     > [ PROJECT ] Schema: [IP : STRING , KBYTES : INT64].
             > [ AGGREGATE ] Schema: [CLICKSTREAM.IP : STRING , CLICKSTREAM.BYTES : INT64 , KSQL_AGG_VARIABLE_0 : INT64].
                     > [ PROJECT ] Schema: [CLICKSTREAM.IP : STRING , CLICKSTREAM.BYTES : INT64].
                             > [ REKEY ] Schema: [CLICKSTREAM.ROWTIME : INT64 , CLICKSTREAM.ROWKEY : STRING , CLICKSTREAM._TIME : INT64 , CLICKSTREAM.TIME : STRING , CLICKSTREAM.IP : STRING , CLICKSTREAM.REQUEST : STRING , CLICKSTREAM.STATUS : INT32 , CLICKSTREAM.USERID : INT32 , CLICKSTREAM.BYTES : INT64 , CLICKSTREAM.AGENT : STRING].
                                     > [ SOURCE ] Schema: [CLICKSTREAM.ROWTIME : INT64 , CLICKSTREAM.ROWKEY : STRING , CLICKSTREAM._TIME : INT64 , CLICKSTREAM.TIME : STRING , CLICKSTREAM.IP : STRING , CLICKSTREAM.REQUEST : STRING , CLICKSTREAM.STATUS : INT32 , CLICKSTREAM.USERID : INT32 , CLICKSTREAM.BYTES : INT64 , CLICKSTREAM.AGENT : STRING].


    Processing topology
    -------------------
    Sub-topologies:
      Sub-topology: 0
        Source: KSTREAM-SOURCE-0000000000 (topics: [clickstream])
          --> KSTREAM-MAP-0000000001
        Processor: KSTREAM-MAP-0000000001 (stores: [])
          --> KSTREAM-TRANSFORMVALUES-0000000002
          <-- KSTREAM-SOURCE-0000000000

.. _drop-stream:

DROP STREAM [IF EXISTS] [DELETE TOPIC];
---------------------------------------

**Synopsis**

.. code:: sql

    DROP STREAM [IF EXISTS] stream_name [DELETE TOPIC];

**Description**

Drops an existing stream.

If the DELETE TOPIC clause is present, the corresponding Kafka topic is marked
for deletion, and if the topic format is AVRO, the corresponding Avro schema is
deleted, too. Topic deletion is asynchronous, and actual removal from brokers
may take some time to complete.

.. note:: DELETE TOPIC will not necessarily work if your kafka cluster is configured
  to create topics automatically with ``auto.create.topics.enable=true``. We
  recommended checking after a few minutes to ensure that the topic was
  deleted.

If the IF EXISTS clause is present, the statement doesn't fail if the table
doesn't exist.

.. _drop-table:

DROP TABLE [IF EXISTS] [DELETE TOPIC];
--------------------------------------

**Synopsis**

.. code:: sql

    DROP TABLE [IF EXISTS] table_name [DELETE TOPIC];

**Description**

Drops an existing table.

If the DELETE TOPIC clause is present, the corresponding Kafka topic is marked
for deletion and if the topic format is AVRO, the corresponding Avro schema is
deleted in the schema registry. Topic deletion is asynchronous, and actual removal from brokers
may take some time to complete.

.. note:: DELETE TOPIC will not necessarily work if your kafka cluster is configured
  to create topics automatically with ``auto.create.topics.enable=true``. We
  recommended checking after a few minutes to ensure that the topic was
  deleted.

If the IF EXISTS clause is present, the statement doesn't fail if the table
doesn't exist.

PRINT
-----

.. code:: sql

    PRINT qualifiedName [FROM BEGINNING] [INTERVAL interval] [LIMIT limit]

**Description**

Print the contents of Kafka topics to the KSQL CLI.

.. important:: SQL grammar defaults to uppercase formatting. You can use quotations (``"``) to print topics that contain lowercase characters.

The PRINT statement supports the following properties:

+-------------------------+------------------------------------------------------------------------------------------------------------------+
| Property                | Description                                                                                                      |
+=========================+==================================================================================================================+
| FROM BEGINNING          | Print starting with the first message in the topic. If not specified, PRINT starts with the most recent message. |
+-------------------------+------------------------------------------------------------------------------------------------------------------+
| INTERVAL interval       | Print every ``interval`` th message. The default is 1, meaning that every message is printed.                    |
+-------------------------+------------------------------------------------------------------------------------------------------------------+
| LIMIT limit             | Stop printing after ``limit`` messages. The default value is unlimited, requiring Ctrl+C to terminate the query. |
+-------------------------+------------------------------------------------------------------------------------------------------------------+

For example:

.. code:: sql

    PRINT 'ksql__commands' FROM BEGINNING;

Your output should resemble:

::

    Format:JSON
    {"ROWTIME":1516010696273,"ROWKEY":"\"stream/CLICKSTREAM/create\"","statement":"CREATE STREAM clickstream (_time bigint,time varchar, ip varchar, request varchar, status int, userid int, bytes bigint, agent varchar) with (kafka_topic = 'clickstream', value_format = 'json');","streamsProperties":{}}
    {"ROWTIME":1516010709492,"ROWKEY":"\"table/EVENTS_PER_MIN/create\"","statement":"create table events_per_min as select userid, count(*) as events from clickstream window  TUMBLING (size 10 second) group by userid;","streamsProperties":{}}
    ^CTopic printing ceased

SELECT
------

**Synopsis**

.. code:: sql

    SELECT select_expr [, ...]
      FROM from_item
      [ LEFT JOIN join_table ON join_criteria ]
      [ WINDOW window_expression ]
      [ WHERE condition ]
      [ GROUP BY grouping_expression ]
      [ HAVING having_expression ]
      [ LIMIT count ];

**Description**

Selects rows from a KSQL stream or table. The result of this statement
will not be persisted in a Kafka topic and will only be printed out in
the console. To stop the continuous query in the CLI press ``Ctrl-C``.
Note that the WINDOW  clause can only be used if the ``from_item`` is a stream.

In the above statements from_item is one of the following:

-  ``stream_name [ alias ]``
-  ``table_name [ alias ]``
-  ``from_item LEFT JOIN from_item ON join_condition``

The WHERE clause can refer to any column defined for a stream or table,
including the two implicit columns ``ROWTIME`` and ``ROWKEY``.

Example:

.. code:: sql

    SELECT * FROM pageviews
      WHERE ROWTIME >= 1510923225000
        AND ROWTIME <= 1510923228000;

When writing logical expressions using ``ROWTIME``, ISO-8601 formatted datestrings can also be used to represent dates.
For example, the above query is equivalent to the following:

.. code:: sql

    SELECT * FROM pageviews
          WHERE ROWTIME >= '2017-11-17T04:53:45'
            AND ROWTIME <= '2017-11-17T04:53:48';

If the datestring is inexact, the rest of the timestamp is assumed to be padded with 0's.
For example, ``ROWTIME = '2019-07-30T11:00'`` is equivalent to ``ROWTIME = '2019-07-30T11:00:00.0000'``.

Timezones can be specified within the datestring. For example, `2017-11-17T04:53:45-0330` is in the Newfoundland time
zone. If no timezone is specified within the datestring, then timestamps are interperted in the UTC timezone.

A ``LIMIT`` can be used to limit the number of rows returned. Once the limit is reached the query will terminate.

Example:

.. code:: sql

    SELECT * FROM pageviews LIMIT 5;

If no limit is supplied the query will run until terminated, streaming back all results to the console.

**Tip:** If you want to select older data, you can configure KSQL to query the stream from the beginning.  You must
run this configuration before running the query:

.. code:: sql

    SET 'auto.offset.reset' = 'earliest';

The WINDOW clause lets you control how to *group input records that have
the same key* into so-called *windows* for operations such as
aggregations or joins. Windows are tracked per record key. KSQL supports
the following WINDOW types:

-  **TUMBLING**: Tumbling windows group input records into fixed-sized,
   non-overlapping windows based on the records’ timestamps. You must
   specify the *window size* for tumbling windows. Note: Tumbling
   windows are a special case of hopping windows where the window size
   is equal to the advance interval.

   Example:

   .. code:: sql

       SELECT item_id, SUM(quantity)
         FROM orders
         WINDOW TUMBLING (SIZE 20 SECONDS)
         GROUP BY item_id;

-  **HOPPING**: Hopping windows group input records into fixed-sized,
   (possibly) overlapping windows based on the records’ timestamps. You
   must specify the *window size* and the *advance interval* for hopping
   windows.

   Example:

   .. code:: sql

       SELECT item_id, SUM(quantity)
         FROM orders
         WINDOW HOPPING (SIZE 20 SECONDS, ADVANCE BY 5 SECONDS)
         GROUP BY item_id;

-  **SESSION**: Session windows group input records into so-called
   sessions. You must specify the *session inactivity gap* parameter for
   session windows. For example, imagine you set the inactivity gap to 5
   minutes. If, for a given record key such as “alice”, no new input
   data arrives for more than 5 minutes, then the current session for
   “alice” is closed, and any newly arriving data for “alice” in the
   future will mark the beginning of a new session.

   Example:

   .. code:: sql

       SELECT item_id, SUM(quantity)
         FROM orders
         WINDOW SESSION (20 SECONDS)
         GROUP BY item_id;

CAST
~~~~

**Synopsis**

.. code:: sql

    CAST (expression AS data_type);

You can cast an expression’s type to a new type using CAST. Here is an
example of converting a BIGINT into a VARCHAR type:

.. code:: sql

    -- This query converts the numerical count into a suffixed string; e.g., 5 becomes '5_HELLO'
    SELECT page_id, CONCAT(CAST(COUNT(*) AS VARCHAR), '_HELLO')
      FROM pageviews_enriched
      WINDOW TUMBLING (SIZE 20 SECONDS)
      GROUP BY page_id;

CASE
~~~~

**Synopsis**

.. code:: sql

    CASE
       WHEN condition THEN result
       [ WHEN ... THEN ... ]
       …
       [ WHEN … THEN … ]
       [ ELSE result ]
    END

Currently, KSQL supports a ``searched`` form of CASE expression. In this form,
CASE evaluates each boolean ``condition`` in WHEN clauses, from left to right.
If a condition is true, CASE returns the corresponding result. If none of
the conditions is true, CASE returns the result from the ELSE clause. If none
of the conditions is true and there is no ELSE clause, CASE returns null.

The schema for all results must be the same, otherwise, KSQL rejects the
statement. Here's an example of a CASE expression:

.. code:: sql

    SELECT
     CASE
       WHEN orderunits < 2.0 THEN 'small'
       WHEN orderunits < 4.0 THEN 'medium'
       ELSE 'large'
     END AS case_result
    FROM orders;

LIKE
~~~~

**Synopsis**

.. code:: sql

    column_name LIKE pattern;

The LIKE operator is used for prefix or suffix matching. Currently KSQL
supports ``%``, which represents zero or more characters.

Example:

.. code:: sql

    SELECT user_id
      FROM users
      WHERE user_id LIKE 'santa%';

BETWEEN
~~~~~~~

**Synopsis**

.. code:: sql

    WHERE expression [NOT] BETWEEN start_expression AND end_expression;

The BETWEEN operator is used to indicate that a certain value must lie within
a specified range, inclusive of boundaries. Currently, KSQL supports any expression
that resolves to a numeric or string value for comparison.

Example:

.. code:: sql

  SELECT event
    FROM events
    WHERE event_id BETWEEN 10 AND 20

SHOW FUNCTIONS
--------------

**Synopsis**

.. code:: sql

    SHOW | LIST FUNCTIONS;

**Description**

List the available scalar and aggregate functions available.

.. _show-topics:

SHOW TOPICS
-----------

**Synopsis**

.. code:: sql

    SHOW | LIST TOPICS [EXTENDED];

**Description**

SHOW TOPICS lists the available topics in the Kafka cluster that KSQL is configured
to connect to (default setting for ``bootstrap.servers``:
``localhost:9092``). SHOW TOPICS EXTENDED also displays consumer groups and their active consumer
counts.

.. _show-streams:

SHOW STREAMS
------------

**Synopsis**

.. code:: sql

    SHOW | LIST STREAMS;

**Description**

List the defined streams.

SHOW TABLES
-----------

**Synopsis**

.. code:: sql

    SHOW | LIST TABLES;

**Description**

List the defined tables.

SHOW QUERIES
------------

**Synopsis**

.. code:: sql

    SHOW QUERIES;

**Description**

List the running persistent queries.

SHOW PROPERTIES
---------------

**Synopsis**

.. code:: sql

    SHOW PROPERTIES;

**Description**

List the :ref:`configuration settings <ksql-param-reference>` that are
currently in effect.

SPOOL
_____
**Synopsis**

.. code:: sql

    SPOOL <file_name|OFF>

**Description**

Stores issued commands and their results into a file. Only one spool may be active at a time and can
be closed by issuing ``SPOOL OFF`` . Commands are prefixed with ``ksql>`` to differentiate from
output.


.. _ksql-terminate:

TERMINATE
---------

**Synopsis**

.. code:: sql

    TERMINATE query_id;

**Description**

Terminate a persistent query. Persistent queries run continuously until
they are explicitly terminated.

-  In client-server mode, exiting the CLI will not stop persistent
   queries because the KSQL server(s) will continue to process the
   queries.

(To terminate a non-persistent query use ``Ctrl-C`` in the CLI.)

.. _operators:

=========
Operators
=========

KSQL supports the following operators in value expressions.

The explanation for each operator includes a supporting example based on the following table:

.. code:: sql

  CREATE TABLE USERS (
      USERID BIGINT
      FIRST_NAME STRING,
      LAST_NAME STRING,
      NICKNAMES ARRAY<STRING>,
      ADDRESS STRUCT<STREET_NAME STRING, NUMBER INTEGER>
  ) WITH (KAFKA_TOPIC='users', VALUE_FORMAT='AVRO', KEY='USERID');

- Arithmetic (``+,-,/,*,%``) The usual arithmetic operators may be applied to numeric types
  (INT, BIGINT, DOUBLE)

.. code:: sql

  SELECT LEN(FIRST_NAME) + LEN(LAST_NAME) AS NAME_LENGTH FROM USERS;

- Concatenation (``+,||``) The concatenation operator can be used to concatenate STRING values.

.. code:: sql

  SELECT FIRST_NAME + LAST_NAME AS FULL_NAME FROM USERS;

- You can use the ``+`` operator for multi-part concatenation, for example:

.. code:: sql

    SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss') +
            ': :heavy_exclamation_mark: On ' +
            HOST +
            ' there were ' +
            CAST(INVALID_LOGIN_COUNT AS VARCHAR) +
            ' attempts in the last minute (threshold is >=4)'
    FROM INVALID_USERS_LOGINS_PER_HOST
    WHERE INVALID_LOGIN_COUNT>=4;

- Source Dereference (``.``) The source dereference operator can be used to specify columns
  by dereferencing the source stream or table.

.. code:: sql

  SELECT USERS.FIRST_NAME FROM USERS;

- Subscript (``[subscript_expr]``) The subscript operator is used to reference the value at
  an array index or a map key.

.. code:: sql

  SELECT NICKNAMES[0] FROM USERS;

- STRUCT dereference (``->``) Access nested data by declaring a STRUCT and using
  the dereference operator to access its fields:

.. code:: sql

   CREATE STREAM orders (
     orderId BIGINT,
     address STRUCT<street VARCHAR, zip INTEGER>) WITH (...);

   SELECT address->street, address->zip FROM orders;

- Combine `->` with `.` when using aliases:

.. code:: sql

   SELECT orders.address->street, o.address->zip FROM orders o;

.. _functions:

================
Scalar functions
================

+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| Function               | Example                                                                   | Description                                       |
+========================+===========================================================================+===================================================+
| ABS                    |  ``ABS(col1)``                                                            | The absolute value of a value                     |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| ARRAYCONTAINS          |  ``ARRAYCONTAINS('[1, 2, 3]', 3)``                                        | Given JSON or AVRO array checks if a search       |
|                        |                                                                           | value contains in it                              |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| AS_ARRAY               |  ``AS_ARRAY(col1, col2)```                                                | Construct an array from a variable number of      |
|                        |                                                                           | inputs.                                           |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| AS_MAP                 |  ``AS_MAP(keys, vals)```                                                  | Construct a map from a list of keys and a list of |
|                        |                                                                           | values.                                           |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| CEIL                   |  ``CEIL(col1)``                                                           | The ceiling of a value.                           |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| CONCAT                 |  ``CONCAT(col1, '_hello')``                                               | Concatenate two strings.                          |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| UNIX_DATE              |  ``UNIX_DATE()``                                                          | Gets an integer representing days since epoch.    |
|                        |                                                                           | The returned timestamp may differ depending on    |
|                        |                                                                           | the local time of different KSQL Server instances.|
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| UNIX_TIMESTAMP         |  ``UNIX_TIMESTAMP()``                                                     | Gets the Unix timestamp in milliseconds,          |
|                        |                                                                           | represented as a BIGINT.                          |
|                        |                                                                           | The returned timestamp may differ depending on    |
|                        |                                                                           | the local time of different KSQL Server instances.|
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| DATETOSTRING           |  ``DATETOSTRING(START_DATE, 'yyyy-MM-dd')``                               | Converts an integer representation of a date into |
|                        |                                                                           | a string representing the date in                 |
|                        |                                                                           | the given format. Single quotes in the            |
|                        |                                                                           | timestamp format can be escaped with two          |
|                        |                                                                           | successive single quotes, ``''``, for example:    |
|                        |                                                                           | ``'yyyy-MM-dd''T'''``.                            |
|                        |                                                                           | The integer represents days since epoch           |
|                        |                                                                           | matching the encoding used by Kafka Connect dates.|
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| ELT                    | ``ELT(n INTEGER, args VARCHAR[])``                                        | Returns element ``n`` in the ``args`` list of     |
|                        |                                                                           | strings, or NULL if ``n`` is less than 1 or       |
|                        |                                                                           | greater than the number of arguments. This        |
|                        |                                                                           | function is 1-indexed. ELT is the complement to   |
|                        |                                                                           | FIELD.                                            |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| EXTRACTJSONFIELD       |  ``EXTRACTJSONFIELD(message, '$.log.cloud')``                             | Given a string column in JSON format, extract     |
|                        |                                                                           | the field that matches.                           |
|                        |                                                                           |                                                   |
|                        |                                                                           | Example where EXTRACTJSONFIELD is needed:         |
|                        |                                                                           |                                                   |
|                        |                                                                           | ``{"foo": \"{\"bar\": \"quux\"}\"}``              |
|                        |                                                                           |                                                   |
|                        |                                                                           | However, in cases where the column is really an   |
|                        |                                                                           | object but declared as a STRING you can use the   |
|                        |                                                                           | ``STRUCT`` type, which is easier to work with.    |
|                        |                                                                           |                                                   |
|                        |                                                                           | Example where ``STRUCT`` will work:               |
|                        |                                                                           |                                                   |
|                        |                                                                           | ``{"foo": {"bar": "quux"}}``                      |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| EXP                    |  ``EXP(col1)``                                                            | The exponential of a value.                       |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| FIELD                  | ``FIELD(str VARCHAR, args VARCHAR[])``                                    | Returns the 1-indexed position of ``str`` in      |
|                        |                                                                           | ``args``, or 0 if not found. If ``str`` is NULL,  |
|                        |                                                                           | the return value is 0, because NULL is not        |
|                        |                                                                           | considered to be equal to any value. FIELD is the |
|                        |                                                                           | complement to ELT.                                |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| FLOOR                  |  ``FLOOR(col1)``                                                          | The floor of a value.                             |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| GEO_DISTANCE           |  ``GEO_DISTANCE(lat1, lon1, lat2, lon2, unit)``                           | The great-circle distance between two lat-long    |
|                        |                                                                           | points, both specified in decimal degrees. An     |
|                        |                                                                           | optional final parameter specifies ``KM``         |
|                        |                                                                           | (the default) or ``miles``.                       |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| IFNULL                 |  ``IFNULL(col1, retval)``                                                 | If the provided VARCHAR is NULL, return           |
|                        |                                                                           | ``retval``, otherwise, return the value. Only     |
|                        |                                                                           | VARCHAR values are supported for the input. The   |
|                        |                                                                           | return value must be a VARCHAR.                   |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| LCASE                  |  ``LCASE(col1)``                                                          | Convert a string to lowercase.                    |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| LEN                    |  ``LEN(col1)``                                                            | The length of a string.                           |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| LN                     |  ``LN(col1)``                                                             | The natural logarithm of a value.                 |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| MASK                   |  ``MASK(col1, 'X', 'x', 'n', '-')``                                       | Convert a string to a masked or obfuscated        |
|                        |                                                                           | version of itself. The optional arguments         |
|                        |                                                                           | following the input string to be masked are the   |
|                        |                                                                           | characters to be substituted for upper-case,      |
|                        |                                                                           | lower-case, numeric and other characters of the   |
|                        |                                                                           | input, respectively. If the mask characters are   |
|                        |                                                                           | omitted then the default values, illustrated in   |
|                        |                                                                           | the example to the left, will be applied.         |
|                        |                                                                           | Set a given mask character to NULL to prevent any |
|                        |                                                                           | masking of that character type.                   |
|                        |                                                                           | For example: ``MASK("My Test $123")`` will return |
|                        |                                                                           | ``Xx-Xxxx--nnn``, applying all default masks.     |
|                        |                                                                           | ``MASK("My Test $123", '*', NULL, '1', NULL)``    |
|                        |                                                                           | will yield ``*y *est $111``.                      |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| MASK_KEEP_LEFT         |  ``MASK_KEEP_LEFT(col1, numChars, 'X', 'x', 'n', '-')``                   | Similar to the ``MASK`` function above, except    |
|                        |                                                                           | that the first or left-most ``numChars``          |
|                        |                                                                           | characters will not be masked in any way.         |
|                        |                                                                           | For example: ``MASK_KEEP_LEFT("My Test $123", 4)``|
|                        |                                                                           | will return ``My Txxx--nnn``.                     |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| MASK_KEEP_RIGHT        |  ``MASK_KEEP_RIGHT(col1, numChars, 'X', 'x', 'n', '-')``                  | Similar to the ``MASK`` function above, except    |
|                        |                                                                           | that the last or right-most ``numChars``          |
|                        |                                                                           | characters will not be masked in any way.         |
|                        |                                                                           | For example:``MASK_KEEP_RIGHT("My Test $123", 4)``|
|                        |                                                                           | will return ``Xx-Xxxx-$123``.                     |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| MASK_LEFT              |  ``MASK_LEFT(col1, numChars, 'X', 'x', 'n', '-')``                        | Similar to the ``MASK`` function above, except    |
|                        |                                                                           | that only the first or left-most ``numChars``     |
|                        |                                                                           | characters will have any masking applied to them. |
|                        |                                                                           | For example: ``MASK_LEFT("My Test $123", 4)``     |
|                        |                                                                           | will return ``Xx-Xest $123``.                     |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| MASK_RIGHT             |  ``MASK_RIGHT(col1, numChars, 'X', 'x', 'n', '-')``                       | Similar to the ``MASK`` function above, except    |
|                        |                                                                           | that only the last or right-most ``numChars``     |
|                        |                                                                           | characters will have any masking applied to them. |
|                        |                                                                           | For example: ``MASK_RIGHT("My Test $123", 4)``    |
|                        |                                                                           | will return ``My Test -nnn``.                     |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| RANDOM                 |  ``RANDOM()``                                                             | Return a random DOUBLE value between 0.0 and 1.0. |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| ROUND                  |  ``ROUND(col1)``                                                          | Round a value to the nearest BIGINT value.        |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| SQRT                   |  ``SQRT(col1)``                                                           | The square root of a value.                       |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| SLICE                  |  ``SLICE(col1, from, to)``                                                | Slices a list based on the supplied indices. The  |
|                        |                                                                           | indices start at 1 and include both endpoints.    |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| SPLIT                  |  ``SPLIT(col1, delimiter)``                                               | Splits a string into an array of substrings based |
|                        |                                                                           | on a delimiter. If the delimiter is not found,    |
|                        |                                                                           | then the original string is returned as the only  |
|                        |                                                                           | element in the array. If the delimiter is empty,  |
|                        |                                                                           | then all characters in the string are split.      |
|                        |                                                                           | If either, string or delimiter, are NULL, then a  |
|                        |                                                                           | NULL value is returned.                           |
|                        |                                                                           |                                                   |
|                        |                                                                           | If the delimiter is found at the beginning or end |
|                        |                                                                           | of the string, or there are contiguous delimiters,|
|                        |                                                                           | then an empty space is added to the array.        |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| STRINGTODATE           |  ``STRINGTODATE(col1, 'yyyy-MM-dd')``                                     | Converts a string representation of a date in the |
|                        |                                                                           | given format into an integer representing days    |
|                        |                                                                           | since epoch. Single quotes in the timestamp       |
|                        |                                                                           | format can be escaped with two successive single  |
|                        |                                                                           | quotes, ``''``, for example:                      |
|                        |                                                                           | ``'yyyy-MM-dd''T'''``.                            |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| STRINGTOTIMESTAMP      |  ``STRINGTOTIMESTAMP(col1, 'yyyy-MM-dd HH:mm:ss.SSS' [, TIMEZONE])``      | Converts a string value in the given              |
|                        |                                                                           | format into the BIGINT value                      |
|                        |                                                                           | that represents the millisecond timestamp. Single |
|                        |                                                                           | quotes in the timestamp format can be escaped with|
|                        |                                                                           | two successive single quotes, ``''``, for         |
|                        |                                                                           | example: ``'yyyy-MM-dd''T''HH:mm:ssX'``.          |
|                        |                                                                           | TIMEZONE is an optional parameter and it is a     |
|                        |                                                                           | java.util.TimeZone ID format, for example: "UTC", |
|                        |                                                                           | "America/Los_Angeles", "PDT", "Europe/London". For|
|                        |                                                                           | more information on timestamp formats, see        |
|                        |                                                                           | `DateTimeFormatter <https://cnfl.io/java-dtf>`__. |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| SUBSTRING              |  ``SUBSTRING(col1, 2, 5)``                                                | ``SUBSTRING(str, pos, [len]``.                    |
|                        |                                                                           | Returns a substring of ``str`` that starts at     |
|                        |                                                                           | ``pos`` (first character is at position 1) and    |
|                        |                                                                           | has length ``len``, or continues to the end of    |
|                        |                                                                           | the string.                                       |
|                        |                                                                           | For example, ``SUBSTRING("stream", 1, 4)``        |
|                        |                                                                           | returns "stre".                                   |
|                        |                                                                           |                                                   |
|                        |                                                                           | NOTE: Prior to v5.1 of KSQL the syntax was:       |
|                        |                                                                           | ``SUBSTRING(str, start, [end])``, where ``start`` |
|                        |                                                                           | and ``end`` positions where base-zero indexes     |
|                        |                                                                           | (first character at position 0) to start          |
|                        |                                                                           | (inclusive) and end (exclusive) the substring,    |
|                        |                                                                           | respectively.                                     |
|                        |                                                                           | For example, ``SUBSTRING("stream", 1, 4)`` would  |
|                        |                                                                           | return "tre".                                     |
|                        |                                                                           | It is possible to switch back to this legacy mode |
|                        |                                                                           | by setting                                        |
|                        |                                                                           | ``ksql.functions.substring.legacy.args`` to       |
|                        |                                                                           | ``true``. We recommend against enabling this      |
|                        |                                                                           | setting. Instead, update your queries             |
|                        |                                                                           | accordingly.                                      |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| TIMESTAMPTOSTRING      |  ``TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS' [, TIMEZONE])``   | Converts a BIGINT millisecond timestamp value into|
|                        |                                                                           | the string representation of the timestamp in     |
|                        |                                                                           | the given format. Single quotes in the            |
|                        |                                                                           | timestamp format can be escaped with two          |
|                        |                                                                           | successive single quotes, ``''``, for example:    |
|                        |                                                                           | ``'yyyy-MM-dd''T''HH:mm:ssX'``.                   |
|                        |                                                                           | TIMEZONE is an optional parameter and it is a     |
|                        |                                                                           | java.util.TimeZone ID format, for example: "UTC", |
|                        |                                                                           | "America/Los_Angeles", "PDT", "Europe/London". For|
|                        |                                                                           | more information on timestamp formats, see        |
|                        |                                                                           | `DateTimeFormatter <https://cnfl.io/java-dtf>`__. |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| TRIM                   |  ``TRIM(col1)``                                                           | Trim the spaces from the beginning and end of     |
|                        |                                                                           | a string.                                         |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| UCASE                  |  ``UCASE(col1)``                                                          | Convert a string to uppercase.                    |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| URL_DECODE_PARAM       |  ``URL_DECODE_PARAM(col1)``                                               | Unescapes the `URL-param-encoded`_ value in       |
|                        |                                                                           | ``col1`` This is the inverse of URL_ENCODE_PARAM  |
|                        |                                                                           | :superscript:`*`                                  |
|                        |                                                                           |                                                   |
|                        |                                                                           | Input: ``'url%20encoded``                         |
|                        |                                                                           | Output: ``url encoded``                           |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| URL_ENCODE_PARAM       |  ``URL_ENCODE_PARAM(col1)``                                               | Escapes the value of ``col1`` such that it can    |
|                        |                                                                           | safely be used in URL query parameters. Note that |
|                        |                                                                           | this is not the same as encoding a value for use  |
|                        |                                                                           | in the path portion of a URL.                     |
|                        |                                                                           |                                                   |
|                        |                                                                           | Input: ``url encoded``                            |
|                        |                                                                           | Output: ``'url%20encoded``                        |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| URL_EXTRACT_FRAGMENT   |  ``URL_EXTRACT_FRAGMENT(url)``                                            | Extract the fragment portion of the specified     |
|                        |                                                                           | value. Returns NULL if ``url`` is not a valid URL |
|                        |                                                                           | or if the fragment does not exist. Any encoded    |
|                        |                                                                           | value will be decoded.                            |
|                        |                                                                           |                                                   |
|                        |                                                                           | Input: ``http://test.com#frag``,                  |
|                        |                                                                           | Output: ``frag``                                  |
|                        |                                                                           | Input: ``http://test.com#frag%20space``,          |
|                        |                                                                           | Output: ``frag space``                            |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| URL_EXTRACT_HOST       |  ``URL_EXTRACT_HOST(url)``                                                | Extract the host-name portion of the specified    |
|                        |                                                                           | value. Returns NULL if the ``url`` is not a valid |
|                        |                                                                           | URI according to RFC-2396.                        |
|                        |                                                                           |                                                   |
|                        |                                                                           | Input: ``http://test.com:8080/path``,             |
|                        |                                                                           | Output: ``test.com``                              |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| URL_EXTRACT_PARAMETER  |  ``URL_EXTRACT_PARAMETER(url, parameter_name)``                           | Extract the value of the requested parameter from |
|                        |                                                                           | the query-string of ``url``. Returns NULL         |
|                        |                                                                           | if the parameter is not present, has no value     |
|                        |                                                                           | specified for it in the query-string, or ``url``  |
|                        |                                                                           | is not a valid URI. Encodes the param and decodes |
|                        |                                                                           | the output (see examples).                        |
|                        |                                                                           |                                                   |
|                        |                                                                           | To get all of the parameter values from a         |
|                        |                                                                           | URL as a single string, see ``URL_EXTRACT_QUERY.``|
|                        |                                                                           |                                                   |
|                        |                                                                           | Input: ``http://test.com?a%20b=c%20d``, ``a b``   |
|                        |                                                                           | Output: ``c d``                                   |
|                        |                                                                           | Input: ``http://test.com?a=foo&b=bar``, `b`       |
|                        |                                                                           | Output: ``bar``                                   |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| URL_EXTRACT_PATH       |  ``URL_EXTRACT_PATH(url)``                                                | Extracts the path from ``url``.                   |
|                        |                                                                           | Returns NULL if ``url`` is not a valid URI but    |
|                        |                                                                           | returns an empty string if the path is empty.     |
|                        |                                                                           |                                                   |
|                        |                                                                           | Input: ``http://test.com/path/to#a``              |
|                        |                                                                           | Output: ``path/to``                               |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| URL_EXTRACT_PORT       |  ``URL_EXTRACT_PORT(url)``                                                | Extract the port number from ``url``.             |
|                        |                                                                           | Returns NULL if ``url`` is not a valid URI or does|
|                        |                                                                           | not contain an explicit port number.              |
|                        |                                                                           |                                                   |
|                        |                                                                           | Input: ``http://localhost:8080/path``             |
|                        |                                                                           | Output: ``8080``                                  |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| URL_EXTRACT_PROTOCOL   |  ``URL_EXTRACT_PROTOCOL(url)``                                            | Extract the protocol from ``url``. Returns NULL if|
|                        |                                                                           | ``url`` is an invalid URI or has no protocol.     |
|                        |                                                                           |                                                   |
|                        |                                                                           | Input: ``http://test.com?a=foo&b=bar``            |
|                        |                                                                           | Output: ``http``                                  |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+
| URL_EXTRACT_QUERY      |  ``URL_EXTRACT_QUERY(url)``                                               | Extract the decoded query-string portion of       |
|                        |                                                                           | ``url``. Returns NULL if no query-string is       |
|                        |                                                                           | present or ``url`` is not a valid URI.            |
|                        |                                                                           |                                                   |
|                        |                                                                           | Input: ``http://test.com?a=foo%20bar&b=baz``,     |
|                        |                                                                           | Output: ``a=foo bar&b=baz``                       |
+------------------------+---------------------------------------------------------------------------+---------------------------------------------------+

.. _URL-param-encoded:

:superscript:`*` All KSQL URL functions assume URI syntax defined in `RFC 39386`_.
For more information on the structure of a URI, including definitions of the various components,
see Section 3 of the RFC. For encoding/decoding, the ``application/x-www-form-urlencoded``
convention is followed.

.. _RFC 39386: https://tools.ietf.org/html/rfc3986

.. _ksql_aggregate_functions:

===================
Aggregate functions
===================

+------------------------+---------------------------+------------+---------------------------------------------------------------------+
| Function               | Example                   | Input Type | Description                                                         |
+========================+===========================+============+=====================================================================+
| COLLECT_LIST           | ``COLLECT_LIST(col1)``    | Stream,    | Return an array containing all the values of ``col1`` from each     |
|                        |                           | Table      | input row (for the specified grouping and time window, if any).     |
|                        |                           |            | Currently only works for simple types (not Map, Array, or Struct).  |
|                        |                           |            | This version limits the size of the result Array to a maximum of    |
|                        |                           |            | 1000 entries and any values beyond this limit are silently ignored. |
|                        |                           |            | When using with a window type of ``session``, it can sometimes      |
|                        |                           |            | happen that two session windows get merged together into one when a |
|                        |                           |            | late-arriving record with a timestamp between the two windows is    |
|                        |                           |            | processed. In this case the 1000 record limit is calculated by      |
|                        |                           |            | first considering all the records from the first window, then the   |
|                        |                           |            | late-arriving record, then the records from the second window in    |
|                        |                           |            | the order they were originally processed.                           |
+------------------------+---------------------------+------------+---------------------------------------------------------------------+
| COLLECT_SET            | ``COLLECT_SET(col1)``     | Stream     | Return an array containing the distinct values of ``col1`` from     |
|                        |                           |            | each input row (for the specified grouping and time window, if any).|
|                        |                           |            | Currently only works for simple types (not Map, Array, or Struct).  |
|                        |                           |            | This version limits the size of the result Array to a maximum of    |
|                        |                           |            | 1000 entries and any values beyond this limit are silently ignored. |
|                        |                           |            | When using with a window type of ``session``, it can sometimes      |
|                        |                           |            | happen that two session windows get merged together into one when a |
|                        |                           |            | late-arriving record with a timestamp between the two windows is    |
|                        |                           |            | processed. In this case the 1000 record limit is calculated by      |
|                        |                           |            | first considering all the records from the first window, then the   |
|                        |                           |            | late-arriving record, then the records from the second window in    |
|                        |                           |            | the order they were originally processed.                           |
+------------------------+---------------------------+------------+---------------------------------------------------------------------+
| COUNT                  | ``COUNT(col1)``,          | Stream,    | Count the number of rows. When ``col1`` is specified, the count     |
|                        | ``COUNT(*)``              | Table      | returned will be the number of rows where ``col1`` is non-null.     |
|                        |                           |            | When ``*`` is specified, the count returned will be the total       |
|                        |                           |            | number of rows.                                                     |
+------------------------+---------------------------+------------+---------------------------------------------------------------------+
| HISTOGRAM              | ``HISTOGRAM(col1)``       | Stream,    | Return a map containing the distinct String values of ``col1``      |
|                        |                           | Table      | mapped to the number of times each one occurs for the given window. |
|                        |                           |            | This version limits the number of distinct values which can be      |
|                        |                           |            | counted to 1000, beyond which any additional entries are ignored.   |
|                        |                           |            | When using with a window type of ``session``, it can sometimes      |
|                        |                           |            | happen that two session windows get merged together into one when a |
|                        |                           |            | late-arriving record with a timestamp between the two windows is    |
|                        |                           |            | processed. In this case the 1000 record limit is calculated by      |
|                        |                           |            | first considering all the records from the first window, then the   |
|                        |                           |            | late-arriving record, then the records from the second window in    |
|                        |                           |            | the order they were originally processed.                           |
+------------------------+---------------------------+------------+---------------------------------------------------------------------+
| MAX                    | ``MAX(col1)``             | Stream     | Return the maximum value for a given column and window.             |
|                        |                           |            | Note: rows where ``col1`` is null will be ignored.                  |
+------------------------+---------------------------+------------+---------------------------------------------------------------------+
| MIN                    | ``MIN(col1)``             | Stream     | Return the minimum value for a given column and window.             |
|                        |                           |            | Note: rows where ``col1`` is null will be ignored.                  |
+------------------------+---------------------------+------------+---------------------------------------------------------------------+
| SUM                    | ``SUM(col1)``             | Stream,    | Sums the column values                                              |
|                        |                           | Table      | Note: rows where ``col1`` is null will be ignored.                  |
+------------------------+---------------------------+------------+---------------------------------------------------------------------+
| TOPK                   | ``TOPK(col1, k)``         | Stream     | Return the Top *K* values for the given column and window           |
|                        |                           |            | Note: rows where ``col1`` is null will be ignored.                  |
+------------------------+---------------------------+------------+---------------------------------------------------------------------+
| TOPKDISTINCT           | ``TOPKDISTINCT(col1, k)`` | Stream     | Return the distinct Top *K* values for the given column and window  |
|                        |                           |            | Note: rows where ``col1`` is null will be ignored.                  |
+------------------------+---------------------------+------------+---------------------------------------------------------------------+
| WindowStart            | ``WindowStart()``         | Stream     | Extract the start time of the current window, in milliseconds.      |
|                        |                           | Table      | If the query is not windowed the function will return null.         |
+------------------------+---------------------------+------------+---------------------------------------------------------------------+
| WindowEnd              | ``WindowEnd()``           | Stream     | Extract the end time of the current window, in milliseconds.        |
|                        |                           | Table      | If the query is not windowed the function will return null.         |
+------------------------+---------------------------+------------+---------------------------------------------------------------------+

For more information, see :ref:`aggregate-streaming-data-with-ksql`.

.. _ksql_key_requirements:

================
Key Requirements
================

Message Keys
------------

The ``CREATE STREAM`` and ``CREATE TABLE`` statements, which read data from a Kafka topic into a stream or table,
allow you to specify a field/column in the Kafka message value that corresponds to the Kafka message key by setting the ``KEY`` property of the ``WITH`` clause.

Example:

.. code:: sql

    CREATE TABLE users (registertime BIGINT, gender VARCHAR, regionid VARCHAR, userid VARCHAR)
      WITH (KAFKA_TOPIC='users', VALUE_FORMAT='JSON', KEY = 'userid');


The ``KEY`` property is optional. KSQL uses it as an optimization hint to determine if repartitioning can be avoided when performing aggregations and joins.
  
  .. important::
     Don't set the KEY property, unless you have validated that your stream doesn't need to be re-partitioned for future joins.
     If you set the KEY property, you will need to re-partition explicitly if your record key doesn't meet partitioning requirements.
     For more information, see :ref:`partition-data-to-enable-joins`.

In either case, when setting ``KEY`` you must be sure that *both* of the following conditions are true:

1. For every record, the contents of the Kafka message key must be the same as the contents of the column set in ``KEY`` (which is derived from a field in the Kafka message value).
2. ``KEY`` must be set to a column of type ``VARCHAR`` aka ``STRING``.

If these conditions are not met, then the results of aggregations and joins may be incorrect. However, if your data doesn't meet these requirements, you can still use KSQL with a few extra steps. The following section explains how.

Table-table joins can be joined only on the ``KEY`` field, and one-to-many
(1:N) joins aren't supported.

What To Do If Your Key Is Not Set or Is In A Different Format
-------------------------------------------------------------

Streams
-------

For streams, just leave out the ``KEY`` property from the ``WITH`` clause. KSQL will take care of repartitioning the stream for you using the value(s) from the ``GROUP BY`` columns for aggregates, and the join predicate for joins.

Tables
------

For tables, you can still use KSQL if the message key is not also present in the Kafka message value or if it is not in
the required format as long as *one* of the following statements is true:

- The message key is a `unary function <https://en.wikipedia.org/wiki/Unary_function>`__ of the value in the desired key
  column.
- It is ok for the messages in the topic to be re-ordered before being inserted into the table.

First create a stream to have KSQL write the message key, and then declare the table on the output topic of this stream:

Example:

- Goal: You want to create a table from a topic, which is keyed by userid of type INT.
- Problem: The message key is present as a field/column (aptly named userid) in the message value but in the wrong
  format (INT instead of VARCHAR).

.. code:: sql

    -- Create a stream on the original topic
    CREATE STREAM users_with_wrong_key_format (userid INT, username VARCHAR, email VARCHAR)
      WITH (KAFKA_TOPIC='users', VALUE_FORMAT='JSON');

    -- Derive a new stream with the required key changes.
    -- 1) The CAST statement converts the key to the required format.
    -- 2) The PARTITION BY clause re-partitions the stream based on the new, converted key.
    CREATE STREAM users_with_proper_key
      WITH(KAFKA_TOPIC='users-with-proper-key') AS
      SELECT CAST(userid as VARCHAR) as userid_string, username, email
      FROM users_with_wrong_key_format
      PARTITION BY userid_string;

    -- Now you can create the table on the properly keyed stream.
    CREATE TABLE users_table (userid_string VARCHAR, username VARCHAR, email VARCHAR)
      WITH (KAFKA_TOPIC='users-with-proper-key',
            VALUE_FORMAT='JSON',
            KEY='userid_string');


Example:

- Goal: You want to create a table from a topic, which is keyed by userid of type INT.
- Problem: The message key is not present as a field/column in the topic's message values.

.. code:: sql

    -- Create a stream on the original topic.
    -- The topic is keyed by userid, which is available as the implicit column ROWKEY
    -- in the users_with_missing_key stream. Note how the explicit column definitions
    -- only define username and email but not userid.
    CREATE STREAM users_with_missing_key (username VARCHAR, email VARCHAR)
      WITH (KAFKA_TOPIC='users', VALUE_FORMAT='JSON');

    -- Derive a new stream with the required key changes.
    -- 1) The contents of ROWKEY (message key) are copied into the message value as the userid_string column,
    --    and the CAST statement converts the key to the required format.
    -- 2) The PARTITION BY clause re-partitions the stream based on the new, converted key.
    CREATE STREAM users_with_proper_key
      WITH(KAFKA_TOPIC='users-with-proper-key') AS
      SELECT CAST(ROWKEY as VARCHAR) as userid_string, username, email
      FROM users_with_missing_key
      PARTITION BY userid_string;

    -- Now you can create the table on the properly keyed stream.
    CREATE TABLE users_table (userid_string VARCHAR, username VARCHAR, email VARCHAR)
      WITH (KAFKA_TOPIC='users-with-proper-key',
            VALUE_FORMAT='JSON',
            KEY='userid_string');

For more information, see :ref:`partition-data-to-enable-joins`.
