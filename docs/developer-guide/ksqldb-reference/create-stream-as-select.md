---
layout: page
title: CREATE STREAM AS SELECT
tagline:  ksqlDB CREATE STREAM AS SELECT syntax
description: Register a stream on a SQL query result to enable operations like joins and aggregations
keywords: ksqlDB, create, stream, push query
---

## Synopsis

```sql
CREATE [OR REPLACE] STREAM stream_name
  [WITH ( property_name = expression [, ...] )]
  AS SELECT  select_expr [, ...]
  FROM from_stream
  [[ LEFT | FULL | INNER ]
      JOIN [join_table | join_stream]
          [WITHIN [<size> <timeunit> | (<before_size> <timeunit>, <after_size> <timeunit>)]
          [GRACE PERIOD <grace_size> <timeunit>]]
      ON join_criteria]*
  [ WHERE condition ]
  [PARTITION BY column_name]
  EMIT CHANGES;
```

## Description

Create a new materialized stream view with a corresponding new {{ site.ak }}
sink topic, and stream the result of the query into the sink topic. The new
stream is said to be a _persistent query_ and is _derived from_ the `from_stream`.

In ksqlDB, you issue a persistent query to transform one stream into another
using a SQL programming model. You derive a new stream from an existing one
by selecting and manipulating the columns you're interested in.

Creating a stream from a query creates a new backing topic with the specified
number of partitions, if the topic doesn't exist already.

!!! important
    Registering a stream on a query result with `CREATE STREAM AS SELECT` is
    distinct from using the [CREATE STREAM](/developer-guide/ksqldb-reference/create-stream)
    statement, which registers a stream on a {{ site.ak }} source topic.

The stream metadata, like the column layout, serialization scheme, and other
information, is placed into ksqlDB's
[command topic](/operate-and-deploy/how-it-works/#command-topic),
which is its internal cluster communication channel.

When you create a persistent query, ksqlDB assigns a generated name based
on the name you provide. For example, if you create a stream named
"pageviews_enriched", ksqlDB might assign an ID like "CSAS_PAGEVIEWS_ENRICHED_1".
The prepended string, "CSAS", is an acronym for `CREATE STREAM AS SELECT`. 

ksqlDB reads rows from the stream partitions that the query selects from.
As each row passes through the persistent query, the transformation logic is
applied to create a new row. Reading a record from {{ site.ak }} doesn't delete
it. Instead, you receive a copy of it.

When you write ksqlDB programs, you chain streams and tables together. You
create a path that your data traverses, with each step in the path performing
a step of processing. ksqlDB handles the mechanics of how your data propagates
through the chain.

!!! Tip "See CREATE STREAM AS SELECT in action"
    - [Detect Unusual Credit Card Activity](https://developer.confluent.io/tutorials/credit-card-activity/confluent.html#execute-ksqldb-code)
    - [Notify Passengers of Flight Updates](https://developer.confluent.io/tutorials/aviation/confluent.html#execute-ksqldb-code)
    - [Detect and analyze SSH attacks](https://developer.confluent.io/tutorials/SSH-attack/confluent.html#execute-ksqldb-code)

### Joins

Joins to streams can use any stream column. If the join criteria is not the key
column of the stream, ksqlDB repartitions the data internally. 

!!! important
    {{ site.ak }} guarantees the relative order of any two messages from
    one source partition only if they are also both in the same partition
    *after* the repartition. Otherwise, {{ site.ak }} is likely to interleave
    messages. The use case will determine if these ordering guarantees are
    acceptable.

Joins to tables must use the table's `PRIMARY KEY` as the join criteria.
Non-key joins aren't supported. For more information, see
[Joining Collections](../joins/join-streams-and-tables.md).

For more information on how to partition your data correctly for joins,
see [Partition Data to Enable Joins](../joins/partition-data.md).

!!! note

    - Partitioning streams and tables is especially important for stateful or
      otherwise intensive queries. For more information, see
      [Parallelization](/operate-and-deploy/performance-guidelines/#parallelization).
    - Once a stream is created, you can't change the number of partitions.
      To change the partition count, you must drop the stream and create it again.

For stream-stream joins, you must specify a `WITHIN` clause for matching
records that both occur within a specified time interval. For valid time
units, see [Time Units](/reference/sql/time/#time-units).

The `PARTITION BY` clause, if supplied, is applied to the source _after_ any
`JOIN` or `WHERE` clauses, and _before_ the `SELECT` clause, in much the same
way as `GROUP BY`.

The key of the resulting stream is determined by the following rules, in order
of priority:

1. If the query has a `PARTITION BY` clause, the resulting number of key
   columns matches the number of expressions in the `PARTITION BY` clause.
   For each expression:

    1. If the `PARTITION BY` expression is a single source-column reference,
       the corresponding key column matches the name, type, and contents of
       the source column.

    2. If the `PARTITION BY` expression is a reference to a field within a
       `STRUCT`-type column, the corresponding key column matches the name,
       type, and contents of the `STRUCT` field.

    3. If the `PARTITION BY` expression is any other expression, the key column
       has a system-generated name, unless you provide an alias in the projection,
       and matches the type and contents of the result of the expression.

2. If the query has a join. For more information, see
   [Join Synthetic Key Columns](/developer-guide/joins/synthetic-keys).

3. Otherwise, the key matches the name, unless you provide an alias in the
   projection, and type of the source stream's key.

The projection must include all columns required in the result, including any
key columns.

### Serialization

For supported [serialization formats](/reference/serialization),
ksqlDB can integrate with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).

ksqlDB registers the key and/or value schemas of the new stream with
{{ site.sr }} automatically. Key and value schemas are registered under the
subjects `<topic-name>-key` and `<topic-name>-value`, respectively.

ksqlDB can also use [Schema Inference With ID](/operate-and-deploy/schema-inference-with-id)
to enable using a physical schema for data serialization.

## Stream properties

Use the `WITH` clause to specify details about your stream.

!!! important
    In join queries, property values are taken from the left-most stream or
    table of the join.

The `WITH` clause supports the following properties.

### FORMAT

The serialization format of both the message key and value in the topic.
For supported formats, see [Serialization Formats](/reference/serialization).

!!! note
    - To use the Avro, Protobuf, or JSON_SR formats, you must enable {{ site.sr }}
      and set [ksql.schema.registry.url](/reference/server-configuration/#ksqlschemaregistryurl)
      in the ksqlDB Server configuration file. For more information, see
      [Configure ksqlDB for Avro, Protobuf, and JSON schemas](/operate-and-deploy/installation/server-config/avro-schema).
    - The JSON format doesn't require {{ site.sr }} to be enabled. 
    - Avro and Protobuf field names are not case sensitive in ksqlDB.
      This matches the ksqlDB column name behavior.

You can't use the `FORMAT` property with the `KEY_FORMAT` or
`VALUE_FORMAT` properties in the same `CREATE STREAM AS SELECT` statement.

### KAFKA_TOPIC

The name of the {{ site.ak }} topic that backs the stream.

If `KAFKA_TOPIC` isn't set, the topic name is set to the `ksql.service.id`
server setting concatenated with the stream name, with all characters
capitalized. In {{ site.ccloud }}, the service ID is the ksqlDB cluster ID.

### KEY_FORMAT

The serialization format of the message key in the topic. For supported formats,
see [Serialization Formats](/reference/serialization).

If this property is not set, the format from the left-most input stream or
table is used.

In join queries, the `KEY_FORMAT` value is taken from the left-most stream or
table.

You can't use the `KEY_FORMAT` property with the `FORMAT` property in the
same `CREATE STREAM AS SELECT` statement.

### KEY_PROTOBUF_NULLABLE_REPRESENTATION

In the default configuration, primitive fields in protobuf do not distinguish `null` from the
default values (such as zero, empty string). To enable the use of a protobuf schema that can make
this distinction, set `KEY_PROTOBUF_NULLABLE_REPRESENTATION` to either `OPTIONAL` or `WRAPPER`.
The schema will be used to serialize keys for the stream created by this `CREATE` statement.
For more details, see the corresponding section in the
[Serialization Formats](/reference/serialization#protobuf) documentation.

### KEY_SCHEMA_FULL_NAME

The full name of the key schema in {{ site.sr }}.

The schema is used for schema inference and data serialization.

### KEY_SCHEMA_ID

The schema ID of the key schema in {{ site.sr }}.

The schema is used for schema inference and data serialization.

For more information, see
[Schema Inference With Schema ID](/operate-and-deploy/schema-inference-with-id).

### PARTITIONS

The number of partitions in the backing topic.

If `PARTITIONS` isn't set, the number of partitions of the input stream is
used.

In join queries, the `PARTITIONS` value is taken from the left-most stream or
table.

You can't change the number of partitions on an existing stream. To change the
partition count, you must drop the stream and create it again.

### REPLICAS

The number of replicas in the backing topic.

If `REPLICAS` isn't set, the number of replicas of the input stream or table
is used.

In join queries, the `REPLICAS` value is taken from the left-most stream or
table.

### RETENTION_MS

!!! note
    Available starting in version `0.28.3-RC7`.

The retention specified in milliseconds in the backing topic.

If `RETENTION_MS` isn't set, the retention of the input stream is used.
But in the case of inheritance, the CREATE STREAM declaration is not the source
of the `RETENTION_MS` value.

In join queries, the `RETENTION_MS` value is taken from the left-most stream.

You can't change the retention on an existing stream. To change the
retention, you have these options:

- Drop the stream and the topic it's registered on with the DROP STREAM and
  DELETE TOPIC statements, and create them again.
- Drop the stream with the DROP STREAM statement, update the topic with
  `retention.ms=<new-value>` and register the stream again with
  `CREATE STREAM WITH (RETENTION_MS=<new-value>)`.
- For a stream that was created with `CREATE STREAM WITH (RETENTION_MS=<old-value>)`,
  update the topic with `retention.ms=<new-value>`, and update the stream with the
  `CREATE OR REPLACE STREAM WITH (RETENTION_MS=<new-value>)` statement.

### TIMESTAMP

Sets a column within the stream's schema to be used as the default source of
`ROWTIME` for any downstream queries.

Timestamps have an accuracy of milliseconds.

Downstream queries that use time-based operations, like windowing, process
records in this stream based on the timestamp in this column. The column is
used to set the timestamp on any records emitted to {{ site.ak }}.

If not provided, the `ROWTIME` of the source stream is used.

This doesn't affect the processing of the query that populates
this stream. For example, given the following statement:

```sql
CREATE STREAM foo WITH (TIMESTAMP='t2') AS
  SELECT * FROM bar
  WINDOW TUMBLING (size 10 seconds);
  EMIT CHANGES;
```

The window into which each row of `bar` is placed is determined by `bar`'s
`ROWTIME`, not `t2`.

### TIMESTAMP_FORMAT

Use with the `TIMESTAMP` property to specify the type and format of the
timestamp column.

- If set, the `TIMESTAMP` column must be of type `varchar` and have a format that
  can be parsed with the Java [DateTimeFormatter](https://cnfl.io/java-dtf).
- If not set, the ksqlDB timestamp column must be of type `bigint` or `timestamp`.

If your timestamp format has characters that require single quotes, escape them
with successive single quotes, `''`, for example: `'yyyy-MM-dd''T''HH:mm:ssX'`.

For more information, see [Timestamp formats](/reference/sql/time/#timestamp-formats).

### VALUE_AVRO_SCHEMA_FULL_NAME

The full name of the value AVRO schema in {{ site.sr }}.

The schema is used for schema inference and data serialization. 

### VALUE_DELIMITER

Set the delimiter string to use when `VALUE_FORMAT` is set to `DELIMITED`.

You can use a single character as a delimiter. The default is `','`.

For space-delimited and tab-delimited values, use the special values `SPACE`
or `TAB` instead of the actual space or tab characters.

### VALUE_FORMAT

The serialization format of the message value in the topic. For supported formats,
see [Serialization Formats](/reference/serialization).

If this property is not set, the format from the left-most input stream or
table is used.

In join queries, the `VALUE_FORMAT` value is taken from the left-most stream or
table.

You can't use the `VALUE_FORMAT` property with the `FORMAT` property in the
same `CREATE STREAM AS SELECT` statement.

### VALUE_PROTOBUF_NULLABLE_REPRESENTATION

In the default configuration, primitive fields in protobuf do not distinguish `null` from the
default values (such as zero, empty string). To enable the use of a protobuf schema that can make
this distinction, set `VALUE_PROTOBUF_NULLABLE_REPRESENTATION` to either `OPTIONAL` or `WRAPPER`.
The schema will be used to serialize values for the stream created by this `CREATE` statement.
For more details, see the corresponding section in the
[Serialization Formats](/reference/serialization#protobuf) documentation.

### VALUE_SCHEMA_FULL_NAME

The full name of the value schema in {{ site.sr }}.

The schema is used for schema inference and data serialization. 

### VALUE_SCHEMA_ID

The schema ID of the value schema in {{ site.sr }}.

The schema is used for schema inference and data serialization.

For more information, see
[Schema Inference With Schema ID](/operate-and-deploy/schema-inference-with-id).

### WRAP_SINGLE_VALUE

Specifies how ksqlDB deserializes the value of messages in the backing
topic that contain only a single column.

- If set to `true`, ksqlDB expects the column to have been serialized as a
  named column within a record.
- If set to `false`, ksqlDB expects the column to have been serialized as an
  anonymous value.
- If not supplied, the system default is used, defined by the
  [ksql.persistence.wrap.single.values](/reference/server-configuration#ksqlpersistencewrapsinglevalues)
  configuration property and defaulting to `true`.

!!! note

    - Be careful when you have a single-column schema where the value can be `NULL`,
      because `NULL` values have a special meaning in ksqlDB.
    - Supplying this property for formats that don't support wrapping, for example
      `DELIMITED`, or when the value schema has multiple columns, causes an error.

For more information, see [Single field unwrapping](/reference/serialization/#single-field-unwrapping).

Examples
--------

```sql
-- Create a view that filters an existing stream:
CREATE STREAM filtered AS
   SELECT 
     a, 
     few,
     columns 
   FROM source_stream
   EMIT CHANGES;

-- Create a view that enriches a stream with a table lookup:
CREATE STREAM enriched AS
   SELECT
      cs.*,
      u.name,
      u.classification,
      u.level
   FROM clickstream cs
      JOIN users u ON u.id = cs.userId
   EMIT CHANGES;
   
-- Create a view that enriches a stream with a table lookup with value serialization schema 
-- defined by VALUE_SCHEMA_ID:
CREATE STREAM enriched WITH (
    VALUE_SCHEMA_ID = 1
  ) AS
  SELECT
     cs.*,
     u.name,
     u.classification,
     u.level
  FROM clickstream cs
    JOIN users u ON u.id = cs.userId
  EMIT CHANGES;
```

