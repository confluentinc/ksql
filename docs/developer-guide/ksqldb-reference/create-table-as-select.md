---
layout: page
title: CREATE TABLE AS SELECT
tagline:  ksqlDB CREATE TABLE AS SELECT statement
description: Syntax for the CREATE TABLE AS SELECT statement in ksqlDB
keywords: ksqlDB, create, table, push query
---

CREATE TABLE AS SELECT
======================

Synopsis
--------

```sql
CREATE TABLE table_name
  [WITH ( property_name = expression [, ...] )]
  AS SELECT  select_expr [, ...]
  FROM from_item
  [[ LEFT | FULL | INNER ] JOIN [join_table | join_stream] ON join_criteria]* 
  [ WINDOW window_expression ]
  [ WHERE condition ]
  [ GROUP BY grouping_expression ]
  [ HAVING having_expression ]
  [ EMIT output_refinement ];
```

Description
-----------

Create a new ksqlDB materialized table view, along with the corresponding Kafka topic, and
stream the result of the query as a changelog into the topic.

Note that the WINDOW clause can only be used if the `from_item` is a stream and the query contains
a `GROUP BY` clause.

Note that EMIT `output_refinement` defaults to `CHANGES` unless explicitly set to `FINAL` on a
windowed aggregation.

Joins to streams can use any stream column. If the join criteria is not the key column of the stream
ksqlDB will internally repartition the data. 

!!! important
    {{ site.ak }} guarantees the relative order of any two messages from
    one source partition only if they are also both in the same partition
    *after* the repartition. Otherwise, {{ site.ak }} is likely to interleave
    messages. The use case will determine if these ordering guarantees are
    acceptable.

Joins to tables must use the table's PRIMARY KEY as the join criteria: none primary key joins are 
[not yet supported](https://github.com/confluentinc/ksql/issues/4424).
For more information, see [Join Event Streams with ksqlDB](../joins/join-streams-and-tables.md).

See [Partition Data to Enable Joins](../joins/partition-data.md) for more information about how to
correctly partition your data for joins.

The primary key of the resulting table is determined by the following rules, in order of priority:
 1. if the query has a  `GROUP BY`: 
    1. if the `GROUP BY` is on a single source column reference, the primary key will match the 
       name, type and contents of the source column.
    1. if the `GROUP BY` is any other single expression, the primary key will have a system 
       generated name, unless you provide an alias in the projection, and will match the type and 
       contents of the result of the expression.
    1. otherwise, the primary key will have a system generated name, and will be of type `STRING` 
       and contain the grouping expression concatenated together.
 1. if the query has a join see [Join Synthetic Key Columns](../joins/synthetic-keys) for more info.
 1. otherwise, the primary key will match the name, unless you provide an alias in the projection, 
    and type of the source table's primary key.
 
The projection must include all columns required in the result, including any primary key columns.

For supported [serialization formats](../developer-guide/serialization.md),
ksqlDB can integrate with the [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).
ksqlDB registers the value schema of the new table with {{ site.sr }} automatically. 
The schema is registered under the subject `<topic-name>-value`.

The WITH clause supports the following properties:

|     Property      |                                             Description                                              |
| ----------------- | ---------------------------------------------------------------------------------------------------- |
| KAFKA_TOPIC       | The name of the Kafka topic that backs this table. If this property is not set, then the name of the table will be used as default. |
| VALUE_FORMAT      | Specifies the serialization format of the message value in the topic. For supported formats, see [Serialization Formats](../serialization.md#serialization-formats). If this property is not set, then the format of the input stream/table is used. |
| VALUE_DELIMITER   | Used when VALUE_FORMAT='DELIMITED'. Supports single character to be a delimiter, defaults to ','. For space and tab delimited values you must use the special values 'SPACE' or 'TAB', not an actual space or tab character. |
| PARTITIONS        | The number of partitions in the backing topic. If this property is not set, then the number of partitions of the input stream/table will be used. In join queries, the property values are taken from the left-side stream or table. |
| REPLICAS          | The replication factor for the topic. If this property is not set, then the number of replicas of the input stream or table will be used. In join queries, the property values are taken from the left-side stream or table. |
| TIMESTAMP         | Sets a column within this stream's schema to be used as the default source of `ROWTIME` for any downstream queries. Downstream queries that use time-based operations, such as windowing, will process records in this stream based on the timestamp in this column. The column will be used to set the timestamp on any records emitted to Kafka. Timestamps have a millisecond accuracy. If not supplied, the `ROWTIME` of the source stream is used. <br>**Note**: This doesn't affect the processing of the query that populates this stream. For example, given the following statement:<br><pre>CREATE STREAM foo WITH (TIMESTAMP='t2') AS<br>&#0009;SELECT * FROM bar<br>&#0009;WINDOW TUMBLING (size 10 seconds);<br>&#0009;EMIT CHANGES;</pre>The window into which each row of `bar` is placed is determined by bar's `ROWTIME`, not `t2`. |
| TIMESTAMP_FORMAT  | Used in conjunction with TIMESTAMP. If not set the timestamp column must be of type `bigint`. If it is set, then the TIMESTAMP column must be of type varchar and have a format that can be parsed with the Java `DateTimeFormatter`. If your timestamp format has characters requiring single quotes, you can escape them with two successive single quotes, `''`, for example: `'yyyy-MM-dd''T''HH:mm:ssX'`. For more information on timestamp formats, see [DateTimeFormatter](https://cnfl.io/java-dtf). |
| WRAP_SINGLE_VALUE | Controls how values are serialized where the values schema contains only a single column. The setting controls how the query will serialize values with a single-column schema.<br>If set to `true`, ksqlDB will serialize the column as a named column within a record.<br>If set to `false`, ksqlDB will serialize the column as an anonymous value.<br>If not supplied, the system default, defined by [ksql.persistence.wrap.single.values](../../operate-and-deploy/installation/server-config/config-reference.md#ksqlpersistencewrapsinglevalues), then the format's default is used.<br>**Note:** `null` values have special meaning in ksqlDB. Care should be taken when dealing with single-column schemas where the value can be `null`. For more information, see [Single column (un)wrapping](../serialization.md#single-field-unwrapping).<br>**Note:** Supplying this property for formats that do not support wrapping, for example `DELIMITED`, or when the value schema has multiple columns, will result in an error. |


!!! note
	  - To use Avro or Protobuf, you must have {{ site.sr }} enabled and
    `ksql.schema.registry.url` must be set in the ksqlDB server configuration
    file. See [Configure ksqlDB for Avro, Protobuf, and JSON schemas](../../operate-and-deploy/installation/server-config/avro-schema.md#configure-avro-and-schema-registry-for-ksql).
    - Avro and Protobuf field names are not case sensitive in ksqlDB. This matches the ksqlDB
    column name behavior.

Example
-------

```sql
-- Derive a new view from an existing table:
CREATE TABLE derived AS
  SELECT
    a,
    b,
    d
  FROM source
  WHERE A is not null;

-- Or, join a stream of play events to a songs table, windowing weekly, to create a weekly chart:
CREATE TABLE weeklyMusicCharts AS
   SELECT
      s.songName,
      count(1) AS playCount
   FROM playStream p
      JOIN songs s ON p.song_id = s.id
   WINDOW TUMBLING (7 DAYS)
   GROUP BY s.songName;
```
