---
layout: page
title: Create a ksqlDB Stream
tagline: Create a Stream from a Kafka topic
description: Learn how to use the CREATE STREAM statement on a Kafka topic
---

In ksqlDB, you create streams from existing {{ site.aktm }} topics, create
streams that will create new {{ site.ak }} topics, or create streams of
query results from other streams.

-   Use the CREATE STREAM statement to create a stream from an existing Kafka
    topic, or a new Kafka topic.
-   Use the CREATE STREAM AS SELECT statement to create a query stream
    from an existing stream.

!!! note
      Creating tables is similar to creating streams. For more information,
      see [Create a ksqlDB Table](create-a-table.md).
      
ksqlDB can't infer the topic value's data format, so you must provide the
format of the values that are stored in the topic. In this example, the
data format is `DELIMITED`. 
For all supported formats, see [Serialization Formats](serialization.md#serialization-formats).

Create a Stream from an existing Kafka topic
--------------------------------------------

Use the [CREATE STREAM](./create-stream) statement to create a stream from an
existing underlying {{ site.ak }} topic.

The following examples show how to create streams from a {{ site.ak }} topic
named `pageviews`.

### Create a Stream with Selected Columns

The following example creates a stream that has three columns from the
`pageviews` topic: `viewtime`, `userid`, and `pageid`. All of these columns are loaded from the 
{{ site.ak }} topic message value. To access data in your message key, see 
[Create a Stream with a Specified Key](#create-a-stream-with-a-specified-key), below.

In the ksqlDB CLI, paste the following CREATE STREAM statement:

```sql
CREATE STREAM pageviews (
    viewtime BIGINT,
    userid VARCHAR,
    pageid VARCHAR
  ) WITH (
    KAFKA_TOPIC='pageviews',
    VALUE_FORMAT='DELIMITED'
  );
```

Your output should resemble:

```
 Message
----------------
 Stream created
----------------
```

Inspect the stream by using the SHOW STREAMS and DESCRIBE statements:

```sql
SHOW STREAMS;
```

Your output should resemble:

```
 Stream Name | Kafka Topic | Format
---------------------------------------
 PAGEVIEWS   | pageviews   | DELIMITED
---------------------------------------
```

Get the schema for the stream:

```sql
DESCRIBE PAGEVIEWS;
```

Your output should resemble:

```
Name                 : PAGEVIEWS
 Field    | Type
--------------------------------------
 VIEWTIME | BIGINT
 USERID   | VARCHAR(STRING)
 PAGEID   | VARCHAR(STRING)
--------------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
```

### Create a Stream with a Specified Key

The previous SQL statement doesn't define a column to represent the data in the
{{ site.ak }} message key in the underlying {{ site.ak }} topic. If the {{ site.ak }} message key 
is serialized in a key format that ksqlDB supports, you can specify the key in the column list of 
the CREATE STREAM statement.

ksqlDB requires keys to be serialized using {{ site.ak }}'s own serializers or compatible
serializers. For supported data types, see the [`KAFKA` format](./serialization.md#kafka). 
If the data in your {{ site.ak }} topics doesn't have a suitable key format, 
see [Key Requirements](syntax-reference.md#key-requirements).

For example, the {{ site.ak }}  message key of the `pageviews` topic is a `BIGINT` containing the 
`viewtime`, so you can write the CREATE STREAM statement like this:

```sql
CREATE STREAM pageviews_withkey (
    viewtime BIGINT KEY,
    userid VARCHAR,
    pageid VARCHAR
  ) WITH (
    KAFKA_TOPIC='pageviews',
    VALUE_FORMAT='DELIMITED'
  );
```

Confirm that the KEY column in the new stream is `pageid` by using the
`DESCRIBE EXTENDED` statement:

```sql
DESCRIBE EXTENDED pageviews_withkey;
```

Your output should resemble:

```
Name                 : PAGEVIEWS_WITHKEY
Type                 : STREAM
Timestamp field      : Not set - using <ROWTIME>
Key format           : KAFKA
Value format         : DELIMITED
Kafka topic          : pageviews (partitions: 1, replication: 1)

 Field    | Type
--------------------------------------
 VIEWTIME | BIGINT           (Key)
 USERID   | VARCHAR(STRING)
 PAGEID   | VARCHAR(STRING)
--------------------------------------
[...]
```

### Create a Stream with Timestamps

In ksqlDB, message timestamps are used for window-based operations, like
windowed aggregations, and to support event-time processing.

If you want to use the value of one of the topic's columns as the Kafka
message timestamp, set the TIMESTAMP property in the WITH clause.

For example, if you want to use the value of the `viewtime` column as
the message timestamp, you can rewrite the previous CREATE STREAM statement
like this:

```sql
CREATE STREAM pageviews_timestamped (
    viewtime BIGINT KEY,
    userid VARCHAR
    pageid VARCHAR
  ) WITH (
    KAFKA_TOPIC='pageviews',
    VALUE_FORMAT='DELIMITED',
    TIMESTAMP='viewtime'
  );
```

Confirm that the TIMESTAMP field is `viewtime` by using the DESCRIBE
EXTENDED statement:

```sql
DESCRIBE EXTENDED pageviews_timestamped;
```

Your output should resemble:

```
Name                 : PAGEVIEWS_TIMESTAMPED
Type                 : STREAM
Timestamp field      : VIEWTIME
Key format           : KAFKA
Value format         : DELIMITED
Kafka topic          : pageviews (partitions: 1, replication: 1)
[...]
```

### Creating a Table using Schema Inference

For supported [serialization formats](../developer-guide/serialization.md),
ksqlDB can integrate with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).
ksqlDB can use [Schema Inference](../concepts/schemas.md#schema-inference) to
spare you from defining columns manually in your `CREATE STREAM` statements.

The following example creates a stream over an existing topic, loading the
value column definitions from {{ site.sr }}.

```sql
CREATE STREAM pageviews WITH (
    KAFKA_TOPIC='pageviews',
    VALUE_FORMAT='PROTOBUF'
  );
```

For more information, see [Schema Inference](../concepts/schemas.md#schema-inference).

Create a Stream backed by a new Kafka Topic
-------------------------------------------

Use the CREATE STREAM statement to create a stream without a preexisting
topic by providing the PARTITIONS count, and optionally the REPLICA count,
in the WITH clause.

Taking the example of the pageviews table from above, but where the underlying
Kafka topic does not already exist, you can create the stream by pasting
the following CREATE STREAM statement into the CLI:

```sql
CREATE STREAM pageviews(
    viewtime BIGINT KEY,
    userid VARCHAR,
    pageid VARCHAR
  ) WITH (
    KAFKA_TOPIC='pageviews',
    VALUE_FORMAT='DELIMITED',
    PARTITIONS=4,
    REPLICAS=3
  );
```

This statement creates the `pageviews` topic for you with the supplied partition and replica count. The 
value of `viewTime` is stored in the {{ site.ak }} topic message key. To store
the column in the message value, simply remove `KEY` from the column definition.

Create a Persistent Streaming Query from a Stream
-------------------------------------------------

Use the CREATE STREAM AS SELECT statement to create a persistent query
stream from an existing stream.

CREATE STREAM AS SELECT creates a stream that contains the results from
a SELECT query. ksqlDB persists the SELECT query results into a
corresponding new topic. A stream created this way represents a
persistent, continuous, streaming query, which means that it runs until
you stop it explicitly.

!!! note
      A SELECT statement by itself is a *transient* query. The
      result of a SELECT statement isn't persisted in a Kafka topic and is
      only printed in the ksqlDB console or streaming to your client. 
      Don't confuse persistent queries created by CREATE STREAM AS SELECT 
      with the streaming query result from a SELECT statement.

Use the SHOW QUERIES statement to list the persistent queries that are
running currently.

Use the PRINT statement to view the results of a persistent query in the
ksqlDB CLI. Press CTRL+C to stop printing records. When you stop printing,
the query continues to run.

Use the TERMINATE statement to stop a persistent query. Exiting the ksqlDB
CLI *does not stop* persistent queries. Your ksqlDB servers continue to
process the queries, and queries run continuously until you terminate
them explicitly.

To stream the result of a SELECT query into an *existing* stream and its
underlying topic, use the INSERT INTO statement.

The following SQL statement creates a `pageviews_intro` stream that
contains results from a persistent query that matches "introductory"
pages that have a `pageid` value that's less than `Page_20`:

```sql
CREATE STREAM pageviews_intro AS
      SELECT * FROM pageviews
      WHERE pageid < 'Page_20'
      EMIT CHANGES;
```

Your output should resemble:

```
 Message
----------------------------
 Stream created and running
----------------------------
```

To confirm that the `pageviews_intro` query is running continuously as a
stream, run the PRINT statement:

```sql
PRINT pageviews_intro;
```

Your output should resemble:

```
Key format: KAFKA_BIGINT or KAFKA_DOUBLE
Value format: KAFKA_STRING
rowtime: 10/30/18 10:15:51 PM GMT, key: 1540937751186, value: User_8,Page_12
rowtime: 10/30/18 10:15:55 PM GMT, key: 1540937755255, value: User_1,Page_15
rowtime: 10/30/18 10:15:57 PM GMT, key: 1540937757265, value: User_8,Page_10
rowtime: 10/30/18 10:15:59 PM GMT, key: 1540937759330, value: User_4,Page_15
rowtime: 10/30/18 10:15:59 PM GMT, key: 1540937759699, value: User_1,Page_12
rowtime: 10/30/18 10:15:59 PM GMT, key: 1540937759990, value: User_6,Page_15
^CTopic printing ceased
```

Press Ctrl+C to stop printing the stream.

!!! note
		The query continues to run after you stop printing the stream.

Use the SHOW QUERIES statement to view the query that ksqlDB created for
the `pageviews_intro` stream:

```sql
SHOW QUERIES;
```

Your output should resemble:

```
     Query ID               | Kafka Topic     | Query String

     CSAS_PAGEVIEWS_INTRO_0 | PAGEVIEWS_INTRO | CREATE STREAM pageviews_intro AS       SELECT * FROM pageviews       WHERE pageid < 'Page_20' EMIT CHANGES;

    For detailed information on a Query run: EXPLAIN <Query ID>;
```

A persistent query that's created by the CREATE STREAM AS SELECT
statement has the string `CSAS` in its ID, for example,
`CSAS_PAGEVIEWS_INTRO_0`.

Delete a ksqlDB Stream
--------------------

Use the DROP STREAM statement to delete a stream. If you created the
stream by using CREATE STREAM AS SELECT, you must first terminate the
corresponding persistent query.

Use the TERMINATE statement to stop the `CSAS_PAGEVIEWS_INTRO_0` query:

```sql
TERMINATE CSAS_PAGEVIEWS_INTRO_0;
```

Your output should resemble:

```
 Message
-------------------
 Query terminated.
-------------------
```

Use the DROP STREAM statement to delete a persistent query stream. You
must TERMINATE the query before you can drop the corresponding stream.

```sql
DROP STREAM pageviews_intro;
```

Your output should resemble:

```
 Message
-------------------
 Source PAGEVIEWS_INTRO was dropped.
-------------------
```

Next Steps
----------

-   [Join Event Streams with ksqlDB](joins/join-streams-and-tables.md)
