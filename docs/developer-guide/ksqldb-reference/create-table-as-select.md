---
layout: page
title: CREATE TABLE AS SELECT
tagline:  ksqlDB CREATE TABLE AS SELECT syntax
description: Register a table on a SQL query result to enable operations like joins and aggregations
keywords: ksqlDB, create, table, push query
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/create-table-as-select.html';
</script>

## Synopsis

```sql
CREATE [OR REPLACE] TABLE table_name
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

## Description

Create a new materialized table view with a corresponding new {{ site.ak }}
sink topic, and stream the result of the query as a changelog into the topic.

ksqlDB enables a _materialized view_, which is a table that maintains running,
aggregate calculations that are updated incrementally as new data rows arrive.
Queries against materialized views are fast, and ksqlDB ensures that a key's
rows appear in a single partition. For more information, see
[Materialized Views](/concepts/materialized-views/#materialized-views).

Materialized views keep only the aggregation, so the full history of view
changes is stored in a _changelog_ topic, which you can replay later to restore
state, if a materialized view is lost.

Both of ksqlDB's two kinds of queries, [pull](select-pull-query.md) and
[push](select-push-query.md), can fetch materialized view data from a table.
Pull queries terminate in a traditional relational manner. Push queries stay
alive to capture streaming changes. For more information, see
[Queries](/concepts/queries/).

!!! Tip "See CREATE TABLE AS SELECT in action"
    - [Detect Unusual Credit Card Activity](https://developer.confluent.io/tutorials/credit-card-activity/confluent.html#execute-ksqldb-code)
    - [Notify Passengers of Flight Updates](https://developer.confluent.io/tutorials/aviation/confluent.html#execute-ksqldb-code)
    - [Understand user behavior with clickstream data](https://developer.confluent.io/tutorials/clickstream/confluent.html#execute-ksqldb-code)
### Joins

In ksqlDB, you can join streams to streams, streams to tables, and tables to
tables, which means that you can join _data at rest_ with _data in motion_.

Joins to streams can use any stream column. If the join criteria is not the key
column of the stream, ksqlDB internally repartitions the data. 

Joins to tables must use the table's PRIMARY KEY as the join criteria. Non-key
joins are not supported. For more information, see
[Joining Collections](../joins/join-streams-and-tables.md).

{{ site.ak }} guarantees the relative order of any two messages from one source
partition only if they are also both in the same partition *after* the
repartition. Otherwise, {{ site.ak }} is likely to interleave messages. The use
case will determine if these ordering guarantees are acceptable.

For more information on how to partition your data correctly for joins,
see [Partition Data to Enable Joins](../joins/partition-data.md).

!!! note

    - Partitioning streams and tables is especially important for stateful or otherwise
      intensive queries. For more information, see
      [Parallelization](/operate-and-deploy/performance-guidelines/#parallelization).
    - Once a table is created, you can't change the number of partitions.
      To change the partition count, you must drop the table and create it again.

The primary key of the resulting table is determined by the following rules,
in order of priority:

1. If the query has a `GROUP BY` clause, the resulting number of primary key
   columns matches the number of grouping expressions. For each grouping
   expression:

    1. If the grouping expression is a single source-column reference, the
       corresponding primary key column matches the name, type, and contents
       of the source column.

    2. If the grouping expression is a reference to a field within a
       `STRUCT`-type column, the corresponding primary key column matches the
       name, type, and contents of the `STRUCT` field.

    3. If the `GROUP BY` is any other expression, the primary key has a
       system-generated name, unless you provide an alias in the projection,
       and the key matches the type and contents of the result of the expression.

1. If the query has a join. For more information, see
   [Join Synthetic Key Columns](/developer-guide/joins/synthetic-keys).
1. Otherwise, the primary key matches the name, unless you provide an alias
   in the projection, and type of the source table's primary key.
 
The projection must include all columns required in the result, including any
primary key columns.

### Serialization

For supported [serialization formats](/reference/serialization),
ksqlDB can integrate with the [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).

ksqlDB registers the value schema of the new table with {{ site.sr }} automatically. 
The schema is registered under the subject `<topic-name>-value`.

ksqlDB can also use [Schema Inference With ID](/operate-and-deploy/schema-inference-with-id)
to enable using a physical schema for data serialization.

### Windowed aggregation

Specify the `WINDOW` clause to create a windowed aggregation. For more information,
see [Time and Windows in ksqlDB](../../concepts/time-and-windows-in-ksqldb-queries.md).

The `WINDOW` clause can only be used if the `from_item` is a stream and the query
contains a `GROUP BY` clause.

## Table properties

Use the `WITH` clause to specify details about your table.

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
`VALUE_FORMAT` properties in the same `CREATE TABLE AS SELECT` statement.

### KAFKA_TOPIC

The name of the {{ site.ak }} topic that backs the table.

If `KAFKA_TOPIC` isn't set, the name of the table in upper case is used
as the topic name. 

### KEY_FORMAT

The serialization format of the message key in the topic. For supported formats,
see [Serialization Formats](/reference/serialization).

In join queries, the `KEY_FORMAT` value is taken from the left-most stream or
table.

You can't use the `KEY_FORMAT` property with the `FORMAT` property in the
same `CREATE TABLE AS SELECT` statement.

### KEY_PROTOBUF_NULLABLE_REPRESENTATION

In the default configuration, primitive fields in protobuf do not distinguish `null` from the
default values (such as zero, empty string). To enable the use of a protobuf schema that can make
this distinction, set `KEY_PROTOBUF_NULLABLE_REPRESENTATION` to either `OPTIONAL` or `WRAPPER`.
The schema will be used to serialize keys for the table created by this `CREATE` statement.
For more details, see the corresponding section in the
[Serialization Formats](/reference/serialization#protobuf) documentation.

### KEY_SCHEMA_ID

The schema ID of the key schema in {{ site.sr }}.

The schema is used for schema inference and data serialization.

For more information, see
[Schema Inference With Schema ID](/operate-and-deploy/schema-inference-with-id).

### PARTITIONS

The number of partitions in the backing topic.

If `PARTITIONS` isn't set, the number of partitions of the input stream or
table is used.

In join queries, the `PARTITIONS` value is taken from the left-most stream or
table.

You can't change the number of partitions on an existing table. To change the
partition count, you must drop the table and create it again.

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

If `RETENTION_MS` isn't set, the retention of the input stream is
used. But in the case of inheritance, the CREATE TABLE declaration is not
the source of the `RETENTION_MS` value.

This setting is only accepted while creating windowed tables.
Additionally, the larger of `RETENTION_MS` and `RETENTION` is used while
creating the backing topic if it doesn't exist.

In join queries, the `RETENTION_MS` value is taken from the left-most stream.

For example, to retain the computed windowed aggregation results for a week,
you might run the following query with `retention_ms` = 604800000 and `retention` = 2 days:
```sql
CREATE TABLE pageviews_per_region 
WITH (kafka_topic='pageviews-per-region', format='avro', partitions=3, retention_ms=604800000)
AS SELECT regionid, count(*) FROM s1 
WINDOW TUMBLING (SIZE 10 SECONDS, RETENTION 2 DAYS)
GROUP BY regionid;
```

You can't change the retention on an existing table. To change the
retention, you have these options:

- Drop the table and the topic it's registered on with the DROP TABLE and
  DELETE TOPIC statements, and create them again.
- Drop the table with the DROP TABLE statement, update the topic with
  `retention.ms=<new-value>` and register the table again with
  `CREATE TABLE WITH (RETENTION_MS=<new-value>)`.
- For a table that was created with `CREATE TABLE WITH (RETENTION_MS=<old-value>)`,
  update the topic with `retention.ms=<new-value>`, and update the table with the
  `CREATE OR REPLACE TABLE WITH (RETENTION_MS=<new-value>)` statement.

### TIMESTAMP

Sets a column within the tables's schema to be used as the default source of
`ROWTIME` for any downstream queries.

Timestamps have an accuracy of milliseconds.

Downstream queries that use time-based operations, like windowing, process
records in this table based on the timestamp in this column. The column is
used to set the timestamp on any records emitted to {{ site.ak }}.

If not provided, the `ROWTIME` of the source table is used.

This doesn't affect the processing of the query that populates
this table. For example, given the following statement:

```sql
CREATE TABLE foo WITH (TIMESTAMP='t2') AS
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

### VALUE_DELIMITER

Set the delimiter string to use when `VALUE_FORMAT` is set to `DELIMITED`.

You can use a single character as a delimiter. The default is `','`.

For space-delimited and tab-delimited values, use the special values `SPACE`
or `TAB` instead of the actual space or tab characters.

### VALUE_FORMAT

The serialization format of the message value in the topic. For supported formats,
see [Serialization Formats](/reference/serialization).

If `VALUE_FORMAT` isn't provided, the system default is used, defined by
[ksql.persistence.default.format.value](/reference/server-configuration#ksqlpersistencedefaultformatvalue).
If the default is also not set, the statement is rejected as invalid.

You can't use the `VALUE_FORMAT` property with the `FORMAT` property in the
same `CREATE TABLE AS SELECT` statement.

### VALUE_PROTOBUF_NULLABLE_REPRESENTATION

In the default configuration, primitive fields in protobuf do not distinguish `null` from the
default values (such as zero, empty string). To enable the use of a protobuf schema that can make
this distinction, set `VALUE_PROTOBUF_NULLABLE_REPRESENTATION` to either `OPTIONAL` or `WRAPPER`.
The schema will be used to serialize values for the table created by this `CREATE` statement.
For more details, see the corresponding section in the
[Serialization Formats](/reference/serialization#protobuf) documentation.

### VALUE_SCHEMA_ID

The schema ID of the value schema in {{ site.sr }}. The schema is used for
schema inference and data serialization. For more information, see
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
-- Derive a new view from an existing table:
CREATE TABLE derived AS
  SELECT
    a,
    b,
    d
  FROM source
  WHERE A is not null
  EMIT CHANGES;
```

```sql
-- Derive a new view from an existing table with value serialization 
-- schema defined by VALUE_SCHEMA_ID:
CREATE TABLE derived WITH (
    VALUE_SCHEMA_ID=1
  ) AS
  SELECT
    a,
    b,
    d
  FROM source
  WHERE A is not null
  EMIT CHANGES;
```

```sql
-- Join a stream of play events to a songs table, windowing weekly, to create a weekly chart:
CREATE TABLE weeklyMusicCharts AS
   SELECT
      s.songName,
      count(1) AS playCount
   FROM playStream p
      JOIN songs s ON p.song_id = s.id
   WINDOW TUMBLING (7 DAYS)
   GROUP BY s.songName
   EMIT CHANGES;
```

```sql
-- Window retention: configure the number of windows in the past that ksqlDB retains.
CREATE TABLE pageviews_per_region AS
  SELECT regionid, COUNT(*) FROM pageviews
  WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS, RETENTION 7 DAYS, GRACE PERIOD 30 MINUTES)
  WHERE UCASE(gender)='FEMALE' AND LCASE (regionid) LIKE '%_6'
  GROUP BY regionid
  EMIT CHANGES;
```

```sql
-- out-of-order events: accept events for up to two hours after the window ends.
CREATE TABLE top_orders AS
  SELECT orderzip_code, TOPK(order_total, 5) FROM orders
  WINDOW TUMBLING (SIZE 1 HOUR, GRACE PERIOD 2 HOURS) 
  GROUP BY order_zipcode
  EMIT CHANGES;
```
