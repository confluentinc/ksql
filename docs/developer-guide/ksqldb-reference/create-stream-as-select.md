---
layout: page
title: CREATE STREAM AS SELECT
tagline:  ksqlDB CREATE STREAM AS SELECT statement
description: Syntax for the CREATE STREAM AS SELECT statement in ksqlDB
keywords: ksqlDB, create, stream, push query
---

CREATE STREAM AS SELECT
=======================

Synopsis
--------

```sql
CREATE [OR REPLACE] STREAM stream_name
  [WITH ( property_name = expression [, ...] )]
  AS SELECT  select_expr [, ...]
  FROM from_stream
  [[ LEFT | FULL | INNER ] JOIN [join_table | join_stream] [ WITHIN [(before TIMEUNIT, after TIMEUNIT) | N TIMEUNIT] ] ON join_criteria]* 
  [ WHERE condition ]
  [PARTITION BY column_name]
  EMIT CHANGES;
```

Description
-----------

Create a new materialized stream view, along with the corresponding Kafka topic, and
stream the result of the query into the topic.

The PARTITION BY clause, if supplied, is applied to the source _after_ any JOIN or WHERE clauses, 
and _before_ the SELECT clause, in much the same way as GROUP BY. 

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

For stream-stream joins, you must specify a WITHIN clause for matching
records that both occur within a specified time interval. For valid time
units, see [Time Units](/reference/sql/time/#time-units).

The key of the resulting stream is determined by the following rules, in order of priority:
 1. if the query has a `PARTITION BY`, then the resulting number of key columns will match the
    number of expressions in the `PARTITION BY` clause. For each expression: 
    1. if the `PARTITION BY` expression is a single source column reference, the corresponding key
       column will match the name, type and contents of the source column.
    1. if the `PARTITION BY` expression is a reference to a field within a `STRUCT`-type column, then the
       corresponding key column will match the name, type, and contents of the `STRUCT` field.
    1. if the `PARTITION BY` expression is any other expression, the key column will have a system 
       generated name, unless you provide an alias in the projection, and will match the type and 
       contents of the result of the expression.
 1. if the query has a join see [Join Synthetic Key Columns](/developer-guide/joins/synthetic-keys) for more info.
 1. otherwise, the key will match the name, unless you provide an alias in the projection, 
    and type of the source stream's key.

The projection must include all columns required in the result, including any key columns.

For supported [serialization formats](/reference/serialization),
ksqlDB can integrate with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).
ksqlDB registers the key and/or value schema(s) of the new stream with {{ site.sr }} automatically. 
Key and value schemas are registered under the subjects `<topic-name>-key` and `<topic-name>-value`, respectively.

The WITH clause for the result supports the following properties:

|     Property      |                                             Description                                              |
| ----------------- | ---------------------------------------------------------------------------------------------------- |
| KAFKA_TOPIC       | The name of the Kafka topic that backs this stream. If this property is not set, then the name of the stream in upper case will be used as default. |
| KEY_FORMAT        | Specifies the serialization format of the message key in the topic. For supported formats, see [Serialization Formats](/reference/serialization). If this property is not set, the format from the left-most input stream/table is used. |
| VALUE_FORMAT      | Specifies the serialization format of the message value in the topic. For supported formats, see [Serialization Formats](/reference/serialization). If this property is not set, the format from the left-most input stream/table is used. |
| FORMAT            | Specifies the serialization format of both the message key and value in the topic. It is not valid to supply this property alongside either `KEY_FORMAT` or `VALUE_FORMAT`. For supported formats, see [Serialization Formats](/reference/serialization). |
| VALUE_DELIMITER   | Used when VALUE_FORMAT='DELIMITED'. Supports single character to be a delimiter, defaults to ','. For space and tab delimited values you must use the special values 'SPACE' or 'TAB', not an actual space or tab character. |
| PARTITIONS        | The number of partitions in the backing topic. If this property is not set, then the number of partitions of the input stream/table will be used. In join queries, the property values are taken from the left-most stream or table. |
| REPLICAS          | The replication factor for the topic. If this property is not set, then the number of replicas of the input stream or table will be used. In join queries, the property values are taken from the left-most stream or table. |
| TIMESTAMP         | Sets a column within this stream's schema to be used as the default source of `ROWTIME` for any downstream queries. Downstream queries that use time-based operations, such as windowing, will process records in this stream based on the timestamp in this column. The column will be used to set the timestamp on any records emitted to Kafka. Timestamps have a millisecond accuracy. If not supplied, the `ROWTIME` of the source stream is used. <br>**Note**: This doesn't affect the processing of the query that populates this stream. For example, given the following statement:<br><pre>CREATE STREAM foo WITH (TIMESTAMP='t2') AS<br>&#0009;SELECT * FROM bar<br>&#0009;WINDOW TUMBLING (size 10 seconds);<br>&#0009;EMIT CHANGES;</pre>The window into which each row of `bar` is placed is determined by bar's `ROWTIME`, not `t2`. |
| TIMESTAMP_FORMAT  | Used in conjunction with TIMESTAMP. If not set, ksqlDB timestamp column must be of type `bigint`. When set, the TIMESTAMP column must be of type `varchar` and have a format that can be parsed with the Java `DateTimeFormatter`. If your timestamp format has characters requiring single quotes, you can escape them with two successive single quotes, `''`, for example: `'yyyy-MM-dd''T''HH:mm:ssX'`. For more information on timestamp formats, see [DateTimeFormatter](https://cnfl.io/java-dtf). |
| WRAP_SINGLE_VALUE | Controls how values are serialized where the values schema contains only a single column. This setting controls how the query serializes values with a single-column schema.<br>If set to `true`, ksqlDB serializes the column as a named column within a record.<br>If set to `false`, ksqlDB serializes the column as an anonymous value.<br>If not supplied, the system default, defined by [ksql.persistence.wrap.single.values](/reference/server-configuration#ksqlpersistencewrapsinglevalues), then the format's default is used.<br>**Note:** `null` values have special meaning in ksqlDB. Care should be taken when dealing with single-column schemas where the value can be `null`. For more information, see [Single column (un)wrapping](/reference/serialization#single-field-unwrapping).<br>**Note:** Supplying this property for formats that do not support wrapping, for example `DELIMITED`, or when the value schema has multiple columns, results in an error. |


!!! note
      - To use Avro or Protobuf, you must have {{ site.sr }} enabled and
        `ksql.schema.registry.url` must be set in the ksqlDB server configuration
        file. See [Configure ksqlDB for Avro, Protobuf, and JSON schemas](../../operate-and-deploy/installation/server-config/avro-schema.md). 
      - Avro field names are not case sensitive in ksqlDB. This matches the ksqlDB
        column name behavior.

Example
-------

```sql
-- Create a view that filters an existing stream:
CREATE STREAM filtered AS
   SELECT 
     a, 
     few,
     columns 
   FROM source_stream;

-- Create a view that enriches a stream with a table lookup:
CREATE STREAM enriched AS
   SELECT
      cs.*,
      u.name,
      u.classification,
      u.level
   FROM clickstream cs
      JOIN users u ON u.id = cs.userId;
```

