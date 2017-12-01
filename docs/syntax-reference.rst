.. _ksql_syntax_reference:

Syntax Reference
================

The KSQL CLI provides a terminal-based interactive shell for running
queries.

.. contents:: Contents
    :local:
    :depth: 1

=====================
CLI-specific commands
=====================

Unlike KSQL statements such as ``SELECT``, these commands are for
setting a KSQL configuration, exiting the CLI, etc. Run the CLI with
``--help`` to see the available options.

**Tip:** You can search and browse your command history in the KSQL CLI
with ``Ctrl-R``. After pressing ``Ctrl-R``, start typing the command or
any part of the command to show an auto-complete of past commands.

.. code:: bash

    Description:
      The KSQL CLI provides a terminal-based interactive shell for running queries.  Each command must be on a separate
      line. For KSQL command syntax, see the documentation at https://github.com/confluentinc/ksql/docs/.

    help:
      Show this message.

    clear:
      Clear the current terminal.

    output:
      View the current output format.

    output <format>:
      Set the output format to <format> (valid formats: 'JSON', 'TABULAR')
      For example: "output JSON"

    history:
      Show previous lines entered during the current CLI session. You can use up and down arrow keys to navigate to the
      previous lines too.

    version:
      Get the current KSQL version.

    exit:
      Exit the CLI.


    Default behavior:

        Lines are read one at a time and are sent to the server as KSQL unless one of the following is true:

        1. The line is empty or entirely whitespace. In this case, no request is made to the server.

        2. The line ends with backslash (`\`). In this case, lines are continuously read and stripped of their trailing
        newline and `\` until one is encountered that does not end with `\`; then, the concatenation of all lines read
        during this time is sent to the server as KSQL.

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

DESCRIBE
--------

**Synopsis**

.. code:: sql

    DESCRIBE (stream_name|table_name);

**Description**

List the columns in a stream or table along with their data type and
other attributes.

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
-  ``ARRAY<ArrayType>`` (JSON only)
-  ``MAP<VARCHAR, ValueType>`` (JSON only)

KSQL adds the implicit columns ``ROWTIME`` and ``ROWKEY`` to every
stream and table, which represent the corresponding Kafka message
timestamp and message key, respectively.

The WITH clause supports the following properties:

+--------------+-------------------------------------------------------+
| Property     | Description                                           |
+==============+=======================================================+
| KAFKA_TOPIC  | The name of the Kafka topic that backs this stream.   |
| (required)   | The topic must already exist in Kafka.                |
+--------------+-------------------------------------------------------+
| VALUE_FORMAT | Specifies the serialization format of the message     |
| (required)   | value in the topic. Supported formats: ``JSON``,      |
|              | ``DELIMITED``                                         |
+--------------+-------------------------------------------------------+
| KEY          | Associates the message key in the Kafka topic with a  |
|              | column in the KSQL stream.                            |
+--------------+-------------------------------------------------------+
| TIMESTAMP    | Associates the message timestamp in the Kafka topic   |
|              | with a column in the KSQL stream. Time-based          |
|              | operations such as windowing will process a record    |
|              | according to this timestamp.                          |
+--------------+-------------------------------------------------------+

Example:

.. code:: sql

    CREATE STREAM pageviews (viewtime BIGINT, user_id VARCHAR, page_id VARCHAR)
      WITH (VALUE_FORMAT = 'JSON',
            KAFKA_TOPIC = 'my-pageviews-topic');

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
-  ``ARRAY<ArrayType>`` (JSON only)
-  ``MAP<VARCHAR, ValueType>`` (JSON only)

KSQL adds the implicit columns ``ROWTIME`` and ``ROWKEY`` to every
stream and table, which represent the corresponding Kafka message
timestamp and message key, respectively.

The WITH clause supports the following properties:

+--------------+-------------------------------------------------------+
| Property     | Description                                           |
+==============+=======================================================+
| KAFKA_TOPIC  | The name of the Kafka topic that backs this table.    |
| (required)   | The topic must already exist in Kafka.                |
+--------------+-------------------------------------------------------+
| VALUE_FORMAT | Specifies the serialization format of the message     |
| (required)   | value in the topic. Supported formats: ``JSON``,      |
|              | ``DELIMITED``                                         |
+--------------+-------------------------------------------------------+
| KEY          | Associates the message key in the Kafka topic with a  |
|              | column in the KSQL table.                             |
+--------------+-------------------------------------------------------+
| TIMESTAMP    | Associates the message timestamp in the Kafka topic   |
|              | with a column in the KSQL table. Time-based           |
|              | operations such as windowing will process a record    |
|              | according to this timestamp.                          |
+--------------+-------------------------------------------------------+

Example:

.. code:: sql

    CREATE TABLE users (usertimestamp BIGINT, user_id VARCHAR, gender VARCHAR, region_id VARCHAR)
      WITH (VALUE_FORMAT = 'JSON',
            KAFKA_TOPIC = 'my-users-topic');

CREATE STREAM AS SELECT
-----------------------

**Synopsis**

.. code:: sql

    CREATE STREAM stream_name
      [WITH ( property_name = expression [, ...] )]
      AS SELECT  select_expr [, ...]
      FROM from_item [, ...]
      [ WHERE condition ]
      [PARTITION BY column_name];

**Description**

Create a new stream along with the corresponding Kafka topic, and
continuously write the result of the SELECT query into the stream and
its corresponding topic.

If the PARTITION BY clause is present, then the resulting stream will
have the specified column as its key.

The WITH clause supports the following properties:

+--------------+-------------------------------------------------------+
| Property     | Description                                           |
+==============+=======================================================+
| KAFKA_TOPIC  | The name of the Kafka topic that backs this stream.   |
|              | If this property is not set, then the name of the     |
|              | stream will be used as default.                       |
+--------------+-------------------------------------------------------+
| VALUE_FORMAT | Specifies the serialization format of the message     |
|              | value in the topic. Supported formats: ``JSON``,      |
|              | ``DELIMITED``. If this property is not set, then the  |
|              | format of the input stream/table will be used.        |
+--------------+-------------------------------------------------------+
| PARTITIONS   | The number of partitions in the topic. If this        |
|              | property is not set, then the number of partitions of |
|              | the input stream/table will be used.                  |
+--------------+-------------------------------------------------------+
| REPLICATIONS | The replication factor for the topic. If this         |
|              | property is not set, then the number of replicas of   |
|              | the input stream/table will be used.                  |
+--------------+-------------------------------------------------------+
| TIMESTAMP    | Associates the message timestamp in the Kafka topic   |
|              | with a column in the KSQL stream. Time-based          |
|              | operations such as windowing will process a record    |
|              | according to this timestamp.                          |
+--------------+-------------------------------------------------------+

Note: The ``KEY`` property is not supported – use PARTITION BY instead.

CREATE TABLE AS SELECT
----------------------

**Synopsis**

.. code:: sql

    CREATE TABLE stream_name
      [WITH ( property_name = expression [, ...] )]
      AS SELECT  select_expr [, ...]
      FROM from_item [, ...]
      [ WINDOW window_expression ]
      [ WHERE condition ]
      [ GROUP BY grouping_expression ]
      [ HAVING having_expression ];

**Description**

Create a new KSQL table along with the corresponding Kafka topic and
stream the result of the SELECT query as a changelog into the topic.

The WITH clause supports the following properties:

+--------------+-------------------------------------------------------+
| Property     | Description                                           |
+==============+=======================================================+
| KAFKA_TOPIC  | The name of the Kafka topic that backs this table. If |
|              | this property is not set, then the name of the table  |
|              | will be used as default.                              |
+--------------+-------------------------------------------------------+
| VALUE_FORMAT | Specifies the serialization format of the message     |
|              | value in the topic. Supported formats: ``JSON``,      |
|              | ``DELIMITED``. If this property is not set, then the  |
|              | format of the input stream/table will be used.        |
+--------------+-------------------------------------------------------+
| PARTITIONS   | The number of partitions in the topic. If this        |
|              | property is not set, then the number of partitions of |
|              | the input stream/table will be used.                  |
+--------------+-------------------------------------------------------+
| REPLICATIONS | The replication factor for the topic. If this         |
|              | property is not set, then the number of replicas of   |
|              | the input stream/table will be used.                  |
+--------------+-------------------------------------------------------+
| TIMESTAMP    | Associates the message timestamp in the Kafka topic   |
|              | with a column in the KSQL table. Time-based           |
|              | operations such as windowing will process a record    |
|              | according to this timestamp.                          |
+--------------+-------------------------------------------------------+

DROP STREAM
-----------

**Synopsis**

.. code:: sql

    DROP STREAM stream_name;

**Description**

Drops an existing stream.

DROP TABLE
----------

**Synopsis**

.. code:: sql

    DROP TABLE table_name;

**Description**

Drops an existing table.

SELECT
------

**Synopsis**

.. code:: sql

    SELECT select_expr [, ...]
      FROM from_item [, ...]
      [ WINDOW window_expression ]
      [ WHERE condition ]
      [ GROUP BY grouping_expression ]
      [ HAVING having_expression ];

**Description**

Selects rows from a KSQL stream or table. The result of this statement
will not be persisted in a Kafka topic and will only be printed out in
the console. To stop the continuous query in the CLI press ``Ctrl-C``.

In the above statements from_item is one of the following:

-  ``stream_name [ [ AS ] alias]``
-  ``table_name [ [ AS ] alias]``
-  ``from_item LEFT JOIN from_item ON join_condition``

The WHERE clause can refer to any column defined for a stream or table, including the two implicit columns `ROWTIME`
and `ROWKEY`.

Example:

.. code:: sql

    SELECT * FROM pageviews
      WHERE ROWTIME >= 1510923225000
        AND ROWTIME <= 1510923228000;

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

SHOW TOPICS
-----------

**Synopsis**

.. code:: sql

    SHOW | LIST TOPICS;

**Description**

List the available topics in the Kafka cluster that KSQL is configured
to connect to (default setting for ``bootstrap.servers``:
``localhost:9092``).

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

List the :ref:`configuration settings <configuring-ksql>` that are
currently in effect.

TERMINATE
---------

**Synopsis**

.. code:: sql

    TERMINATE query_id;

**Description**

Terminate a persistent query. Persistent queries run continuously until
they are explicitly terminated.

-  In standalone mode, exiting the CLI will stop (think: “pause”) any
   persistent queries because exiting the CLI will also stop the KSQL
   server. When the CLI is restarted, the server will be restarted, too,
   and any previously defined persistent queries will resume processing.
-  In client-server mode, exiting the CLI will not stop persistent
   queries because the KSQL server(s) will continue to process the
   queries.

(To terminate a non-persistent query use ``Ctrl-C`` in the CLI.)

================
Scalar functions
================

+------------+----------------------------------+----------------------+
| Function   | Example                          | Description          |
+============+==================================+======================+
| ABS        | ``ABS(col1)``                    | The absolute value   |
|            |                                  | of a value           |
+------------+----------------------------------+----------------------+
| CEIL       | ``CEIL(col1)``                   | The ceiling of a     |
|            |                                  | value                |
+------------+----------------------------------+----------------------+
| CONCAT     | ``CONCAT(col1, '_hello')``       | Concatenate two      |
|            |                                  | strings              |
+------------+----------------------------------+----------------------+
| EXTRACTJSO | ``EXTRACTJSONFIELD(message, '$.l | Given a string       |
| NFIELD     | og.cloud')``                     | column in JSON       |
|            |                                  | format, extract the  |
|            |                                  | field that matches   |
+------------+----------------------------------+----------------------+
| FLOOR      | ``FLOOR(col1)``                  | The floor of a value |
+------------+----------------------------------+----------------------+
| LCASE      | ``LCASE(col1)``                  | Convert a string to  |
|            |                                  | lowercase            |
+------------+----------------------------------+----------------------+
| LEN        | ``LEN(col1)``                    | The length of a      |
|            |                                  | string               |
+------------+----------------------------------+----------------------+
| RANDOM     | ``RANDOM()``                     | Return a random      |
|            |                                  | DOUBLE value between |
|            |                                  | 0 and 1.0            |
+------------+----------------------------------+----------------------+
| ROUND      | ``ROUND(col1)``                  | Round a value to the |
|            |                                  | nearest BIGINT value |
+------------+----------------------------------+----------------------+
| STRINGTOTI | ``STRINGTOTIMESTAMP(col1, 'yyyy- | Converts a string    |
| MESTAMP    | MM-dd HH:mm:ss.SSS')``           | value in the given   |
|            |                                  | format into the      |
|            |                                  | BIGINT value         |
|            |                                  | representing the     |
|            |                                  | timestamp.           |
+------------+----------------------------------+----------------------+
| SUBSTRING  | ``SUBSTRING(col1, 2, 5)``        | Return the substring |
|            |                                  | with the start and   |
|            |                                  | end indices          |
+------------+----------------------------------+----------------------+
| TIMESTAMPT | ``TIMESTAMPTOSTRING(ROWTIME, 'yy | Converts a BIGINT    |
| OSTRING    | yy-MM-dd HH:mm:ss.SSS')``        | timestamp value into |
|            |                                  | the string           |
|            |                                  | representation of    |
|            |                                  | the timestamp in the |
|            |                                  | given format.        |
+------------+----------------------------------+----------------------+
| TRIM       | ``TRIM(col1)``                   | Trim the spaces from |
|            |                                  | the beginning and    |
|            |                                  | end of a string      |
+------------+----------------------------------+----------------------+
| UCASE      | ``UCASE(col1)``                  | Convert a string to  |
|            |                                  | uppercase            |
+------------+----------------------------------+----------------------+

===================
Aggregate functions
===================

+-----------------------+-----------------------+-----------------------+
| Function              | Example               | Description           |
+=======================+=======================+=======================+
| COUNT                 | ``COUNT(col1)``       | Count the number of   |
|                       |                       | rows                  |
+-----------------------+-----------------------+-----------------------+
| MAX                   | ``MAX(col1)``         | Return the maximum    |
|                       |                       | value for a given     |
|                       |                       | column and window     |
+-----------------------+-----------------------+-----------------------+
| MIN                   | ``MIN(col1)``         | Return the minimum    |
|                       |                       | value for a given     |
|                       |                       | column and window     |
+-----------------------+-----------------------+-----------------------+
| SUM                   | ``SUM(col1)``         | Sums the column       |
|                       |                       | values                |
+-----------------------+-----------------------+-----------------------+

