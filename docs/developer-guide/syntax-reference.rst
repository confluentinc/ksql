.. _ksql_syntax_reference:

KSQL Syntax Reference
=====================

KSQL has similar semantics to SQL:

- Terminate KSQL statements with a semicolon ``;``
- Use a back-slash ``\`` to indicate continuation of a multi-line statement on the next line

.. contents:: Contents
    :local:
    :depth: 1


===========
Terminology
===========

When using KSQL, the following terminology is used.

Stream
------

A stream is an unbounded sequence of structured data (“facts”). For example, we could have a stream of financial transactions
such as “Alice sent $100 to Bob, then Charlie sent $50 to Bob”. Facts in a stream are immutable, which means new facts can
be inserted to a stream, but existing facts can never be updated or deleted. Streams can be created from a Kafka topic or
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

.. code:: bash

    ksql> RUN SCRIPT '/local/path/to/queries.sql';

The RUN SCRIPT command supports a subset of KSQL statements:

- Persistent queries: :ref:`create-stream`, :ref:`create-table`, :ref:`create-stream-as-select`, :ref:`create-table-as-select`
- :ref:`drop-stream` and :ref:`drop-table`
- SET, UNSET statements

It does not support statements such as:

- SHOW TOPICS and SHOW STREAMS etc
- TERMINATE
- Non-persistent queries: SELECT etc

RUN SCRIPT can also be used from the command line, for instance when writing shell scripts.
For more information, see :ref:`running-ksql-command-line`.


===============
KSQL statements
===============

.. tip::

    -  KSQL statements must be terminated with a semicolon (``;``).
    -  Multi-line statements:

       -  In the CLI you must use a backslash (``\``) to indicate
          continuation of a statement on the next line.
       -  Do not use ``\`` for multi-line statements in ``.sql`` files.


.. contents:: Available KSQL statements:
    :local:
    :depth: 1

.. _create-stream:

CREATE STREAM
-------------

**Synopsis**

.. code:: sql

    CREATE STREAM stream_name ( { column_name data_type } [, ...] )
      WITH ( property_name = expression [, ...] );

**Description**

Create a new stream with the specified columns and properties.

The supported column data types are:

-  ``BOOLEAN``
-  ``INTEGER``
-  ``BIGINT``
-  ``DOUBLE``
-  ``VARCHAR`` (or ``STRING``)
-  ``ARRAY<ArrayType>`` (JSON and AVRO only)
-  ``MAP<VARCHAR, ValueType>`` (JSON and AVRO only)

KSQL adds the implicit columns ``ROWTIME`` and ``ROWKEY`` to every
stream and table, which represent the corresponding Kafka message
timestamp and message key, respectively.

The WITH clause supports the following properties:

+-------------------------+--------------------------------------------------------------------------------------------+
| Property                | Description                                                                                |
+=========================+============================================================================================+
| KAFKA_TOPIC (required)  | The name of the Kafka topic that backs this stream. The topic must already exist in Kafka. |
+-------------------------+--------------------------------------------------------------------------------------------+
| VALUE_FORMAT (required) | Specifies the serialization format of the message value in the topic. Supported formats:   |
|                         | ``JSON``, ``DELIMITED`` (comma-separated value), and ``AVRO``.                             |
+-------------------------+--------------------------------------------------------------------------------------------+
| KEY                     | Optimization hint: If the Kafka message key is also present as a field/column in the Kafka |
|                         | message value, you may set this property to associate the corresponding field/column with  |
|                         | the implicit ``ROWKEY`` column (message key).                                              |
|                         | If set, KSQL uses it as an optimization hint to determine if repartitioning can be avoided |
|                         | when performing aggregations and joins.                                                    |
|                         | You can only use this if the key format in kafka is ``VARCHAR`` or ``STRING``. Do not use  |
|                         | this hint if the message key format in kafka is AVRO or JSON.                              |
|                         | See :ref:`ksql_key_requirements` for more information.                                     |
+-------------------------+--------------------------------------------------------------------------------------------+
| TIMESTAMP               | By default, the implicit ``ROWTIME`` column is the timestamp of the message in the Kafka   |
|                         | topic. The TIMESTAMP property can be used to override ``ROWTIME`` with the contents of the |
|                         | specified field/column within the Kafka message value (similar to timestamp extractors     |
|                         | in Kafka's Streams API). Time-based operations such as windowing will process a record     |
|                         | according to the timestamp in ``ROWTIME``.                                                 |
+-------------------------+--------------------------------------------------------------------------------------------+


.. include:: ../includes/ksql-includes.rst
    :start-line: 2
    :end-line: 6

Example:

.. code:: sql

    CREATE STREAM pageviews (viewtime BIGINT, user_id VARCHAR, page_id VARCHAR)
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

Create a new table with the specified columns and properties.

The supported column data types are:

-  ``BOOLEAN``
-  ``INTEGER``
-  ``BIGINT``
-  ``DOUBLE``
-  ``VARCHAR`` (or ``STRING``)
-  ``ARRAY<ArrayType>`` (JSON and AVRO only)
-  ``MAP<VARCHAR, ValueType>`` (JSON and AVRO only)

KSQL adds the implicit columns ``ROWTIME`` and ``ROWKEY`` to every
stream and table, which represent the corresponding Kafka message
timestamp and message key, respectively.

KSQL has currently the following requirements for creating a table from a Kafka topic:

1. The Kafka message key must also be present as a field/column in the Kafka message value. The ``KEY`` property (see
   below) must be defined to inform KSQL which field/column in the message value represents the key. If the message key
   is not present in the message value, follow the instructions in :ref:`ksql_key_requirements`.
2. The message key must be in ``VARCHAR`` aka ``STRING`` format. If the message key is not in this format, follow the
   instructions in :ref:`ksql_key_requirements`.

The WITH clause supports the following properties:

+-------------------------+--------------------------------------------------------------------------------------------+
| Property                | Description                                                                                |
+=========================+============================================================================================+
| KAFKA_TOPIC (required)  | The name of the Kafka topic that backs this table. The topic must already exist in Kafka.  |
+-------------------------+--------------------------------------------------------------------------------------------+
| VALUE_FORMAT (required) | Specifies the serialization format of message values in the topic. Supported formats:      |
|                         | ``JSON``, ``DELIMITED`` (comma-separated value), and ``AVRO``.                             |
+-------------------------+--------------------------------------------------------------------------------------------+
| KEY (required)          | Associates a field/column within the Kafka message value with the implicit ``ROWKEY``      |
|                         | column (message key) in the KSQL table.                                                    |
|                         |                                                                                            |
|                         | KSQL currently requires that the Kafka message key, which will be available as the         |
|                         | implicit ``ROWKEY`` column in the table, must also be present as a field/column in the     |
|                         | message value. You must set the KEY property to this corresponding field/column in the     |
|                         | message value, and this column must be in ``VARCHAR`` aka ``STRING`` format.               |
|                         | See :ref:`ksql_key_requirements` for more information.                                     |
+-------------------------+--------------------------------------------------------------------------------------------+
| TIMESTAMP               | By default, the implicit ``ROWTIME`` column is the timestamp of the message in the Kafka   |
|                         | topic. The TIMESTAMP property can be used to override ``ROWTIME`` with the contents of the |
|                         | specified field/column within the Kafka message value (similar to timestamp extractors in  |
|                         | Kafka's Streams API). Time-based operations such as windowing will process a record        |
|                         | according to the timestamp in ``ROWTIME``.                                                 |
+-------------------------+--------------------------------------------------------------------------------------------+

.. include:: ../includes/ksql-includes.rst
    :start-line: 2
    :end-line: 6

Example:

.. code:: sql

    CREATE TABLE users (usertimestamp BIGINT, user_id VARCHAR, gender VARCHAR, region_id VARCHAR)
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
      FROM from_item
      [ LEFT JOIN join_table ON join_criteria ]
      [ WHERE condition ]
      [PARTITION BY column_name];

**Description**

Create a new stream along with the corresponding Kafka topic, and
continuously write the result of the SELECT query into the stream and
its corresponding topic.

If the PARTITION BY clause is present, then the resulting stream will
have the specified column as its key.

The WITH clause for the result supports the following properties:

+---------------+------------------------------------------------------------------------------------------------------+
| Property      | Description                                                                                          |
+===============+======================================================================================================+
| KAFKA_TOPIC   | The name of the Kafka topic that backs this stream. If this property is not set, then the            |
|               | name of the stream in upper case will be used as default.                                            |
+---------------+------------------------------------------------------------------------------------------------------+
| VALUE_FORMAT  | Specifies the serialization format of the message value in the topic. Supported formats:             |
|               | ``JSON``, ``DELIMITED`` (comma-separated value), and ``AVRO``. If this property is not               |
|               | set, then the format of the input stream/table is used.                                              |
+---------------+------------------------------------------------------------------------------------------------------+
| PARTITIONS    | The number of partitions in the backing topic. If this property is not set, then the number          |
|               | of partitions is taken from the value of the ``ksql.sink.partitions`` property, which                |
|               | defaults to four partitions. The ``ksql.sink.partitions`` property can be set in the                 |
|               | properties file the KSQL server is started with, or by using the ``SET`` statement.                  |
+---------------+------------------------------------------------------------------------------------------------------+
| REPLICAS      | The replication factor for the topic. If this property is not set, then the number of                |
|               | replicas of the input stream or table will be used.                                                  |
+---------------+------------------------------------------------------------------------------------------------------+
| TIMESTAMP     | Sets a field within this stream's schema to be used as the default source of ``ROWTIME`` for         |
|               | any downstream queries. Downstream queries that use time-based operations, such as windowing,        |
|               | will process records in this stream based on the timestamp in this field. By default,                |
|               | such queries will also use this field to set the timestamp on any records emitted to Kafka.          |
|               |                                                                                                      |
|               | If not supplied, the ``ROWTIME`` of the source stream will be used.                                  |
|               |                                                                                                      |
|               | **NOTE**: This does _not_ affect the processing of the query that populates this stream,             |
|               | e.g. given the statement                                                                             |
|               | ``CREATE STEAM foo WITH (TIMESTAMP='t2') AS SELECT * FROM bar WINDOW TUMBLING (size 10 seconds);``,  |
|               | the window into which each row of ``bar`` is place is determined by bar's ``ROWTIME``, not ``t2``.   |
+---------------+------------------------------------------------------------------------------------------------------+

.. include:: ../includes/ksql-includes.rst
    :start-line: 2
    :end-line: 6

Note: The ``KEY`` property is not supported – use PARTITION BY instead.

.. _create-table-as-select:

CREATE TABLE AS SELECT
----------------------

**Synopsis**

.. code:: sql

    CREATE TABLE table_name
      [WITH ( property_name = expression [, ...] )]
      AS SELECT  select_expr [, ...]
      FROM from_item
      [ WINDOW window_expression ]
      [ WHERE condition ]
      [ GROUP BY grouping_expression ]
      [ HAVING having_expression ];

**Description**

Create a new KSQL table along with the corresponding Kafka topic and
stream the result of the SELECT query as a changelog into the topic.
Note that WINDOW, GROUP BY and HAVING clauses can only be used if the
``from_item`` is a stream.

The WITH clause supports the following properties:

+---------------+------------------------------------------------------------------------------------------------------+
| Property      | Description                                                                                          |
+===============+======================================================================================================+
| KAFKA_TOPIC   | The name of the Kafka topic that backs this table. If this property is not set, then the             |
|               | name of the table will be used as default.                                                           |
+---------------+------------------------------------------------------------------------------------------------------+
| VALUE_FORMAT  | Specifies the serialization format of the message value in the topic. Supported formats:             |
|               | ``JSON``, ``DELIMITED`` (comma-separated value), and ``AVRO``. If this property is not               |
|               | set, then the format of the input stream or table is used.                                           |
+---------------+------------------------------------------------------------------------------------------------------+
| PARTITIONS    | The number of partitions in the backing topic. If this property is not set, then the number          |
|               | of partitions is taken from the value of the ``ksql.sink.partitions`` property, which                |
|               | defaults to four partitions. The ``ksql.sink.partitions`` property can be set in the                 |
|               | properties file the KSQL server is started with, or by using the ``SET`` statement.                  |
+---------------+------------------------------------------------------------------------------------------------------+
| REPLICAS      | The replication factor for the topic. If this property is not set, then the number of                |
|               | replicas of the input stream or table will be used.                                                  |
+---------------+------------------------------------------------------------------------------------------------------+
| TIMESTAMP     | Sets a field within this tables's schema to be used as the default source of ``ROWTIME`` for         |
|               | any downstream queries. Downstream queries that use time-based operations, such as windowing,        |
|               | will process records in this stream based on the timestamp in this field.                            |
|               |                                                                                                      |
|               | If not supplied, the ``ROWTIME`` of the source stream will be used.                                  |
|               |                                                                                                      |
|               | **NOTE**: This does _not_ affect the processing of the query that populates this table,              |
|               | e.g. given the statement                                                                             |
|               |                                                                                                      |
|               | .. literalinclude:: includes/ctas-snippet.sql                                                        |
|               |    :language: sql                                                                                    |
|               |                                                                                                      |
|               | the window into which each row of ``bar`` is placed is determined by bar's ``ROWTIME``, not ``t2``.  |
+---------------+------------------------------------------------------------------------------------------------------+

.. include:: ../includes/ksql-includes.rst
    :start-line: 2
    :end-line: 6


DESCRIBE
--------

**Synopsis**

.. code:: sql

    DESCRIBE [EXTENDED] (stream_name|table_name);

**Description**

* DESCRIBE: List the columns in a stream or table along with their data type and other attributes.
* DESCRIBE EXTENDED: Display DESCRIBE information with additional runtime statistics, Kafka topic details, and the
  set of queries that populate the table or stream.

Example of describing a table:

.. code:: bash

    ksql> DESCRIBE ip_sum;

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

    ksql> DESCRIBE EXTENDED ip_sum;
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

.. code:: bash

    ksql> EXPLAIN ctas_ip_sum;

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

DROP STREAM
-----------

**Synopsis**

.. code:: sql

    DROP STREAM stream_name;

**Description**

Drops an existing stream.

.. _drop-table:

DROP TABLE
----------

**Synopsis**

.. code:: sql

    DROP TABLE table_name;

**Description**

Drops an existing table.

PRINT
-----

.. code:: sql

    PRINT qualifiedName [FROM BEGINNING] [INTERVAL]

**Description**

Print the contents of Kafka topics to the KSQL CLI.

.. important:: SQL grammar defaults to uppercase formatting. You can use quotations (``"``) to print topics that contain lowercase characters.

The PRINT statement supports the following properties:

+-------------------------+------------------------------------------------------------------------------------------------------------------+
| Property                | Description                                                                                                      |
+=========================+==================================================================================================================+
| FROM BEGINNING          | Print starting with the first message in the topic. If not specified, PRINT starts with the most recent message. |
+-------------------------+------------------------------------------------------------------------------------------------------------------+
| INTERVAL                | Print every nth message. The default is 1, meaning that every message is printed.                                |
+-------------------------+------------------------------------------------------------------------------------------------------------------+

For example:

.. code:: sql

    ksql> PRINT 'ksql__commands' FROM BEGINNING;
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
      [ HAVING having_expression ];

**Description**

Selects rows from a KSQL stream or table. The result of this statement
will not be persisted in a Kafka topic and will only be printed out in
the console. To stop the continuous query in the CLI press ``Ctrl-C``.
Note that WINDOW, GROUP BY and HAVING clauses can only be used if the
``from_item`` is a stream.

In the above statements from_item is one of the following:

-  ``stream_name [ [ AS ] alias]``
-  ``table_name [ [ AS ] alias]``
-  ``from_item LEFT JOIN from_item ON join_condition``

The WHERE clause can refer to any column defined for a stream or table,
including the two implicit columns ``ROWTIME`` and ``ROWKEY``.

Example:

.. code:: sql

    SELECT * FROM pageviews
      WHERE ROWTIME >= 1510923225000
        AND ROWTIME <= 1510923228000;

**Tip:** If you want to select older data, you can configure KSQL to query
the stream from the beginning.  You must run this configuration before
running the query:

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

.. _show-topics:

SHOW TOPICS
-----------

**Synopsis**

.. code:: sql

    SHOW | LIST TOPICS;

**Description**

List the available topics in the Kafka cluster that KSQL is configured
to connect to (default setting for ``bootstrap.servers``:
``localhost:9092``).

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

================
Scalar functions
================

+------------------------+------------------------------------------------------------+---------------------------------------------------+
| Function               | Example                                                    | Description                                       |
+========================+============================================================+===================================================+
| ABS                    |  ``ABS(col1)``                                             | The absolute value of a value                     |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| ARRAYCONTAINS          |  ``ARRAYCONTAINS('[1, 2, 3]', 3)``                         | Given JSON or AVRO array checks if a search       |
|                        |                                                            | value contains in it                              |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| CEIL                   |  ``CEIL(col1)``                                            | The ceiling of a value                            |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| CONCAT                 |  ``CONCAT(col1, '_hello')``                                | Concatenate two strings                           |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| EXTRACTJSONFIELD       |  ``EXTRACTJSONFIELD(message, '$.log.cloud')``              | Given a string column in JSON format, extract     |
|                        |                                                            | the field that matches                            |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| FLOOR                  |  ``FLOOR(col1)``                                           | The floor of a value                              |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| LCASE                  |  ``LCASE(col1)``                                           | Convert a string to lowercase                     |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| LEN                    |  ``LEN(col1)``                                             | The length of a string                            |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| RANDOM                 |  ``RANDOM()``                                              | Return a random DOUBLE value between 0.0 and 1.0  |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| ROUND                  |  ``ROUND(col1)``                                           | Round a value to the nearest BIGINT value         |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| STRINGTOTIMESTAMP      |  ``STRINGTOTIMESTAMP(col1, 'yyyy-MM-dd HH:mm:ss.SSS')``    | Converts a string value in the given              |
|                        |                                                            | format into the BIGINT value                      |
|                        |                                                            | that represents the timestamp.                    |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| SUBSTRING              |  ``SUBSTRING(col1, 2, 5)``                                 | Return the substring with the start and end       |
|                        |                                                            | indices                                           |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| TIMESTAMPTOSTRING      |  ``TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS')`` | Converts a BIGINT timestamp value into the        |
|                        |                                                            | string representation of the timestamp in         |
|                        |                                                            | the given format.                                 |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| TRIM                   |  ``TRIM(col1)``                                            | Trim the spaces from the beginning and end of     |
|                        |                                                            | a string                                          |
+------------------------+------------------------------------------------------------+---------------------------------------------------+
| UCASE                  |  ``UCASE(col1)``                                           | Convert a string to uppercase                     |
+------------------------+------------------------------------------------------------+---------------------------------------------------+

===================
Aggregate functions
===================

+------------------------+---------------------------+---------------------------------------------------------------------+
| Function               | Example                   | Description                                                         |
+========================+===========================+=====================================================================+
| COUNT                  | ``COUNT(col1)``           |  Count the number of rows                                           |
+------------------------+---------------------------+---------------------------------------------------------------------+
| MAX                    | ``MAX(col1)``             |  Return the maximum value for a given column and window             |
+------------------------+---------------------------+---------------------------------------------------------------------+
| MIN                    | ``MIN(col1)``             |  Return the minimum value for a given column and window             |
+------------------------+---------------------------+---------------------------------------------------------------------+
| SUM                    | ``SUM(col1)``             |  Sums the column values                                             |
+------------------------+---------------------------+---------------------------------------------------------------------+
| TOPK                   | ``TOPK(col1, k)``         |  Return the Top *K* values for the given column and window          |
+------------------------+---------------------------+---------------------------------------------------------------------+
| TOPKDISTINCT           | ``TOPKDISTINCT(col1, k)`` |  Return the distinct Top *K* values for the given column and window |
+------------------------+---------------------------+---------------------------------------------------------------------+


.. _ksql_key_requirements:

================
Key Requirements
================

Message Keys
------------

The ``CREATE STREAM`` and ``CREATE TABLE`` statements, which read data from a Kafka topic into a stream or table, allow you to specify a field/column in the Kafka message value that corresponds to the Kafka message key by setting the ``KEY`` property of the ``WITH`` clause.

Example:

.. code:: sql

    CREATE TABLE users (registertime BIGINT, gender VARCHAR, regionid VARCHAR, userid VARCHAR)
      WITH (KAFKA_TOPIC='users', VALUE_FORMAT='JSON', KEY = 'userid');


The ``KEY`` property is:

- Required for tables.
- Optional for streams. Here, KSQL uses it as an optimization hint to determine if repartitioning can be avoided when performing aggregations and joins.

In either case, when setting ``KEY`` you must be sure that *both* of the following conditions are true:

1. For every record, the contents of the Kafka message key must be the same as the contents of the columm set in ``KEY`` (which is derived from a field in the Kafka message value).
2. ``KEY`` must be set to a column of type ``VARCHAR`` aka ``STRING``.

If these conditions are not met, then the results of aggregations and joins may be incorrect. However, if your data doesn't meet these requirements, you can still use KSQL with a few extra steps. The following section explains how.

What To Do If Your Key Is Not Set or Is In A Different Format
-------------------------------------------------------------

-------
Streams
-------

For streams, just leave out the ``KEY`` property from the ``WITH`` clause. KSQL will take care of repartitioning the stream for you using the value(s) from the ``GROUP BY`` columns for aggregates, and the join predicate for joins.

------
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
    -- in the `users_with_missing_key` stream. Note how the explicit column definitions
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
