.. _create-a-stream-with-ksql:

Create a KSQL Stream
####################

In KSQL, you create streams from Kafka topics, and you create streams of
query results from other streams.

* Use the CREATE STREAM statement to create a stream from a Kafka topic.
* Use the CREATE STREAM AS SELECT statement to create a query stream from an
  existing stream.

.. note::

   Creating tables is similar to creating streams. For more information, see
   :ref:`ksql_examples`.

Create a Stream from a Kafka topic
**********************************

Use the CREATE STREAM statement to create a stream from an underlying Kafka
topic. The Kafka topic must exist already in your Kafka cluster.

The following examples show how to create streams from a Kafka topic, named
``pageviews``. To see these examples in action, create the ``pageviews`` topic
by following the procedure in :ref:`ksql_quickstart-docker`.  

Create a Stream with Selected Columns
=====================================

The following example creates a stream that has three columns from the
``pageviews`` topic: ``viewtime``, ``userid``, and ``pageid``.

KSQL can't infer the topic's data format, so you must provide the format of the
values that are stored in the topic. In this example, the data format is
``DELIMITED``. Other options are ``Avro`` and ``JSON``.

In the KSQL CLI, paste the following CREATE STREAM statement: 

.. code:: sql

    CREATE STREAM pageviews \
      (viewtime BIGINT, \
       userid VARCHAR, \
       pageid VARCHAR) \
      WITH (KAFKA_TOPIC='pageviews', \
            VALUE_FORMAT='DELIMITED');

Your output should resemble:

.. code:: text

     Message
    ----------------
     Stream created
    ----------------

Inspect the stream by using the SHOW STREAMS and DESCRIBE statements:

.. code:: text

    ksql> SHOW STREAMS;

    Stream Name | Kafka Topic | Format
    ---------------------------------------
    PAGEVIEWS   | pageviews   | DELIMITED
    ---------------------------------------

    ksql> DESCRIBE PAGEVIEWS;

    Name                 : PAGEVIEWS
     Field    | Type
    --------------------------------------
     ROWTIME  | BIGINT           (system)
     ROWKEY   | VARCHAR(STRING)  (system)
     VIEWTIME | BIGINT
     USERID   | VARCHAR(STRING)
     PAGEID   | VARCHAR(STRING)
    --------------------------------------
    For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;

Create a Stream with a Specified Key 
====================================

The previous KSQL statement makes no assumptions about the Kafka message key
in the underlying Kafka topic. If the value of the message key in the topic
is the same as one of the columns defined in the stream, you can specify the
key in the WITH clause of the CREATE STREAM statement.

For example, if the Kafka message key has the same value as the ``pageid``
column, you can write the CREATE STREAM statement like this:

.. code:: sql

    CREATE STREAM pageviews_withkey \
      (viewtime BIGINT, \
       userid VARCHAR, \
       pageid VARCHAR) \
     WITH (KAFKA_TOPIC='pageviews', \
           VALUE_FORMAT='DELIMITED', \
           KEY='pageid');

Confirm that the KEY field in the new stream is ``pageid`` by using the
DESCRIBE EXTENDED statement:

.. code:: text

    ksql> DESCRIBE EXTENDED pageviews_withkey;

    Name                 : PAGEVIEWS_WITHKEY
    Type                 : STREAM
    Key field            : PAGEID
    Key format           : STRING
    Timestamp field      : Not set - using <ROWTIME>
    Value format         : DELIMITED
    Kafka topic          : pageviews (partitions: 1, replication: 1)
    [...]

Create a Stream with Timestamps 
===============================

In KSQL, message timestamps are used for window-based operations, like windowed
aggregations, and to support event-time processing.

If you want to use the value of one of the topic's columns as the Kafka message
timestamp, set the TIMESTAMP property in the WITH clause.

For example, if you want to use the value of the ``viewtime`` column as the
message timestamp, you can rewrite the previous CREATE STREAM AS SELECT statement
like this:

.. code:: sql

    CREATE STREAM pageviews_timestamped \
      (viewtime BIGINT, \
       userid VARCHAR, \
       pageid VARCHAR) \
      WITH (KAFKA_TOPIC='pageviews', \
            VALUE_FORMAT='DELIMITED', \
            KEY='pageid', \
            TIMESTAMP='viewtime');

Confirm that the TIMESTAMP field is ``viewtime`` by using the DESCRIBE EXTENDED
statement:

.. code:: text

    ksql> DESCRIBE EXTENDED pageviews_timestamped;

    Name                 : PAGEVIEWS_TIMESTAMPED
    Type                 : STREAM
    Key field            : PAGEID
    Key format           : STRING
    Timestamp field      : VIEWTIME
    Value format         : DELIMITED
    Kafka topic          : pageviews (partitions: 1, replication: 1)
    [...]

Create a Continuous Streaming Query from a Stream
*************************************************

Use the CREATE STREAM AS SELECT statement to create a query stream from an 
existing stream. 

CREATE STREAM AS SELECT creates a stream that contains the results from a
SELECT query. KSQL persists the SELECT query results into a corresponding new
topic. A stream created this way represents a persistent, continuous query,
which means that it runs until you stop it explicitly.

Use the SHOW QUERIES statement to list the persistent queries that are running
currently.

Use the PRINT statement to view the results of a persistent query in the KSQL CLI.
Press CTRL+C to stop printing records. When you stop printing, the query continues
to run.

Use the TERMINATE statement to stop a persistent query. Exiting the KSQL CLI
*does not stop* persistent queries. Your KSQL servers continue to process the
queries, and queries run continuously until you terminate them explicitly.

.. note::

   A SELECT statement by itself is a *non-persistent* continuous query. The result
   of a SELECT statement isn't persisted in a Kafka topic and is only printed in the
   KSQL console. Don't confuse persistent queries created by CREATE STREAM AS SELECT
   with the query result from a SELECT statement.

The following KSQL statement creates a ``pageviews_intro`` stream that contains
results from a persistent query that matches "introductory" pages that have a
``pageid`` value that's less than ``Page_20``:

.. code:: sql

    CREATE STREAM pageviews_intro AS \
          SELECT * FROM pageviews \
          WHERE pageid < 'Page_20';

Your output should resemble:

.. code:: text

     Message
    ----------------------------
     Stream created and running
    ----------------------------

To confirm that the ``pageviews_intro`` query is running continuously as a
stream, run the PRINT statement:

.. code:: text

    ksql> PRINT pageviews_intro;
    Format:STRING
    10/30/18 10:15:51 PM UTC , 294851 , 1540937751186,User_8,Page_12
    10/30/18 10:15:55 PM UTC , 295051 , 1540937755255,User_1,Page_15
    10/30/18 10:15:57 PM UTC , 295111 , 1540937757265,User_8,Page_10
    10/30/18 10:15:59 PM UTC , 295221 , 1540937759330,User_4,Page_15
    10/30/18 10:15:59 PM UTC , 295231 , 1540937759699,User_1,Page_12
    10/30/18 10:15:59 PM UTC , 295241 , 1540937759990,User_6,Page_15
    ^CTopic printing ceased

Press CTRL-C to stop printing the stream.

.. note:: 

   The query continues to run after you stop printing the stream. 

Terminate a Persistent Query
****************************

Use the TERMINATE statement to stop a persistent query. The TERMINATE statement
requires the ID of the query, which you get by using the SHOW QUERIES statement.

A persistent query that's created by the CREATE STREAM AS SELECT
statement has the string ``CSAS`` in its ID, for example, ``CSAS_PAGEVIEWS_INTRO_0``.

Run the SHOW QUERIES statement to see the ID of the ``pageviews_intro`` query:

.. code:: text

    ksql> SHOW QUERIES;

     Query ID               | Kafka Topic     | Query String
    --------------------------------------------------------------------------------------------------------------------------------------------
     CSAS_PAGEVIEWS_INTRO_0 | PAGEVIEWS_INTRO | CREATE STREAM pageviews_intro AS       SELECT * FROM pageviews       WHERE pageid < 'Page_20';
    --------------------------------------------------------------------------------------------------------------------------------------------
    For detailed information on a Query run: EXPLAIN <Query ID>;

When you have the Query ID, you can terminate the query:

.. code:: text

    ksql> TERMINATE CSAS_PAGEVIEWS_INTRO_0;

     Message
    -------------------
     Query terminated.
    -------------------

Delete a Persistent Query
*************************

Use the DROP STREAM statement to delete a persistent query. You must TERMINATE
the query before you can drop it.

.. code:: text

    ksql> DROP STREAM pageviews_intro;

     Message
    -------------------
     Source PAGEVIEWS_INTRO was dropped.
    -------------------

Next Steps
**********

* :ref:`join-streams-and-tables`
* :ref:`ksql_clickstream-docker`
