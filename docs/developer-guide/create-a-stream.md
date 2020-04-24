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

Create a Stream from an existing Kafka topic
--------------------------------------------

Use the [CREATE STREAM](./create-stream) statement to create a stream from an existing underlying
Kafka topic.

The following examples show how to create streams from an existing Kafka topic,
named `pageviews`. To see these examples in action, create the
`pageviews` topic by following the procedure in
[Write Streaming Queries Against {{ site.aktm }} Using ksqlDB](../tutorials/basics-docker.md).

### Create a Stream with Selected Columns

The following example creates a stream that has three columns from the
`pageviews` topic: `viewtime`, `userid`, and `pageid`.

ksqlDB can't infer the topic value's data format, so you must provide the
format of the values that are stored in the topic. In this example, the
data format is `DELIMITED`. Other options are `Avro`, `JSON`, `JSON_SR`, `PROTOBUF`, and `KAFKA`.
See [Serialization Formats](serialization.md#serialization-formats) for more
details.

ksqlDB requires keys to have been serialized using {{ site.ak }}'s own serializers or compatible
serializers. ksqlDB supports `INT`, `BIGINT`, `DOUBLE`, and `STRING` key types.

In the ksqlDB CLI, paste the following CREATE STREAM statement:

```sql
CREATE STREAM pageviews
  (viewtime BIGINT,
   userid VARCHAR,
   pageid VARCHAR)
  WITH (KAFKA_TOPIC='pageviews',
        VALUE_FORMAT='DELIMITED')
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
 ROWKEY   | VARCHAR(STRING)  (key)
 VIEWTIME | BIGINT
 USERID   | VARCHAR(STRING)
 PAGEID   | VARCHAR(STRING)
--------------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
```

### Create a Stream with a Specified Key

The previous SQL statement makes no assumptions about the Kafka message
key in the underlying Kafka topic. If the value of the message key in
the topic is the same as one of the columns defined in the stream, you
can specify the key in the WITH clause of the CREATE STREAM statement.
If you use this column name later to perform a join or a repartition, ksqlDB
knows that no repartition is needed. In effect, the named column becomes an
alias for ROWKEY.

For example, if the Kafka message key has the same value as the `pageid`
column, you can write the CREATE STREAM statement like this:

```sql
CREATE STREAM pageviews_withkey
  (viewtime BIGINT,
   userid VARCHAR,
   pageid VARCHAR)
 WITH (KAFKA_TOPIC='pageviews',
       VALUE_FORMAT='DELIMITED',
       KEY='pageid');
```

Confirm that the KEY field in the new stream is `pageid` by using the
DESCRIBE EXTENDED statement:

```sql
DESCRIBE EXTENDED pageviews_withkey;
```

Your output should resemble:

```
Name                 : PAGEVIEWS_WITHKEY
Type                 : STREAM
Key field            : PAGEID
Key format           : STRING
Timestamp field      : Not set - using <ROWTIME>
Value format         : DELIMITED
Kafka topic          : pageviews (partitions: 1, replication: 1)
[...]
```

### Create a Stream with Timestamps

In ksqlDB, message timestamps are used for window-based operations, like
windowed aggregations, and to support event-time processing.

If you want to use the value of one of the topic's columns as the Kafka
message timestamp, set the TIMESTAMP property in the WITH clause.

For example, if you want to use the value of the `viewtime` column as
the message timestamp, you can rewrite the previous CREATE STREAM AS
SELECT statement like this:

```sql
CREATE STREAM pageviews_timestamped
  (viewtime BIGINT,
   userid VARCHAR,
   pageid VARCHAR)
  WITH (KAFKA_TOPIC='pageviews',
        VALUE_FORMAT='DELIMITED',
        KEY='pageid',
        TIMESTAMP='viewtime')
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
Key field            : PAGEID
Key format           : STRING
Timestamp field      : VIEWTIME
Value format         : DELIMITED
Kafka topic          : pageviews (partitions: 1, replication: 1)
[...]
```

Create a Stream backed by a new Kafka Topic
-------------------------------------------

Use the CREATE STREAM statement to create a stream without a preexisting
topic by providing the PARTITIONS count, and optionally the REPLICA count,
in the WITH clause.

Taking the example of the pageviews table from above, but where the underlying
Kafka topic does not already exist, you can create the stream by pasting
the following CREATE STREAM statement into the CLI:

```sql
CREATE STREAM pageviews
  (viewtime BIGINT,
   userid VARCHAR,
   pageid VARCHAR)
  WITH (KAFKA_TOPIC='pageviews',
        PARTITIONS=4,
        REPLICAS=3
        VALUE_FORMAT='DELIMITED')
```

This will create the pageviews topics for you with the supplied partition and replica count.

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
      A SELECT statement by itself is a *non-persistent* continuous query. The
      result of a SELECT statement isn't persisted in a Kafka topic and is
      only printed in the ksqlDB console. Don't confuse persistent queries
      created by CREATE STREAM AS SELECT with the streaming query result from
      a SELECT statement.

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

!!! note
      The CREATE STREAM AS SELECT statement doesn't support the KEY property.
      To specify a KEY field, use the PARTITION BY clause. For more
      information, see
      [Partition Data to Enable Joins](joins/partition-data.md).

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
rowtime: 10/30/18 10:15:51 PM GMT, key: 294851, value: 1540937751186,User_8,Page_12
rowtime: 10/30/18 10:15:55 PM GMT, key: 295051, value: 1540937755255,User_1,Page_15
rowtime: 10/30/18 10:15:57 PM GMT, key: 295111, value: 1540937757265,User_8,Page_10
rowtime: 10/30/18 10:15:59 PM GMT, key: 295221, value: 1540937759330,User_4,Page_15
rowtime: 10/30/18 10:15:59 PM GMT, key: 295231, value: 1540937759699,User_1,Page_12
rowtime: 10/30/18 10:15:59 PM GMT, key: 295241, value: 1540937759990,User_6,Page_15
^CTopic printing ceased
```

Press Ctrl+C to stop printing the stream.

!!! note
		The query continues to run after you stop printing the stream.

!!! note
    KsqlDB has determined that the key format is either `KAFKA_BIGINT` or `KAFKA_DOUBLE`.
    KsqlDB has not narrowed it further because it is not possible to rule out
    either format just by inspecting the key's serialized bytes. In this case we know the key is
    a `BIGINT`. For other cases you may know the key type or you may need to speak to the author
    of the data.

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
-   [Clickstream Data Analysis Pipeline Using ksqlDB (Docker)](../tutorials/clickstream-docker.md)

