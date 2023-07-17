---
layout: page
title: CREATE TABLE
tagline:  ksqlDB CREATE TABLE statement syntax
description: Register a table on a Kafka topic to enable SQL joins and aggregations.
keywords: ksqlDB, create, table
---

## Synopsis

```sql
CREATE [OR REPLACE] [SOURCE] TABLE [IF NOT EXISTS] table_name
  ( { column_name data_type [PRIMARY KEY] } [, ...] )
  WITH ( property_name = expression [, ...] );
```

## Description

Create a new table with the specified columns and properties.

Creating a table registers it on an underlying {{ site.aktm }} topic, so you
can use SQL statements to perform operations like joins and aggregations on the
topic's data. The table is said to be _backed by the topic_.

!!! important
    Registering a table on a topic by using the `CREATE TABLE` statement is
    distinct from using the [CREATE TABLE AS SELECT](../create-table-as-select)
    statement, which creates a table from the result of a SELECT query.

Specify `CREATE OR REPLACE` to replace an existing table with a new query that
resumes from the same processing point as the previously existing query.

If you provide the `IF NOT EXISTS` clause, the statement won't fail if a table
with the same name already exists. Instead, ksqlDB returns a warning,
`A table with the same name already exists.`

For more information, see [Stream Processing](/concepts/stream-processing).

!!! Tip "See CREATE TABLE in action"
    - [Analyze datacenter power usage](https://developer.confluent.io/tutorials/datacenter/confluent.html#execute-ksqldb-code)
    - [Flag Unhealthy IoT Devices](https://developer.confluent.io/tutorials/internet-of-things/confluent.html#execute-ksqldb-code)
    - [Notify Passengers of Flight Updates](https://developer.confluent.io/tutorials/aviation/confluent.html#execute-ksqldb-code)

### PRIMARY KEY

A ksqlDB table works much like tables in other SQL systems. A table has zero or
more rows. Each row is identified by its `PRIMARY KEY`. A row's `PRIMARY KEY`
can't be `NULL`.

Tables with multiple primary keys can have nulls written into the primary key
fields, because the key becomes a struct concatenation of the individual
fields, resulting in the actual key never being null.

!!! important
    You must declare a PRIMARY KEY when you create a table on a {{ site.ak }}
    topic.

If an incoming message in the underlying {{ site.ak }} topic has the same key
as an existing row, it _replaces_ the existing row in the table. But if the
message's value is `NULL`, it _deletes_ the row.

!!! tip
    A message that has a `NULL` value is known as a _tombstone_, because it
    causes the existing row to be deleted.

This situation is handled differently by a [ksqlDB STREAM](../create-stream), as
shown in the following summary.

|                         |  STREAM   | TABLE          |
| ----------------------- | --------- | -------------- |
| Key column type         | `KEY`     | `PRIMARY KEY`  |
| NON NULL key constraint | **No**    | **Yes:** A message in the {{ site.ak }} topic with a NULL `PRIMARY KEY` is ignored.  |
| Unique key constraint   | **No:** A message with the same key as another has no special meaning. | **Yes:** A later message with the same key _replaces_ earlier messages in the table. |
| Tombstones              | **No:** A message with a NULL value is ignored. | **Yes:** A NULL message value is treated as a _tombstone_. Any existing row with a matching key is deleted from the table. |

### Columns

Each column in a table is defined by the following syntax:

* `column_name`: the name of the column. If unquoted, the name must be a valid
   [SQL identifier](/reference/sql/syntax/lexical-structure#identifiers) and
   ksqlDB converts it to uppercase. The name can be quoted if case needs to be
   preserved or if the name is not a valid SQL identifier, for example
   ``` `mixedCaseId` ``` or ``` `$with@invalid!chars` ```.

* `data_type`: the SQL type of the column. Columns can be any of the
   [data types](/reference/sql/data-types) supported by ksqlDB.

* `HEADERS` or `HEADER('<key>')`: columns that are populated by the
   {{ site.ak }} message's header should be marked as `HEADERS` or
   `HEADER('<key>')` columns. If a column is marked by `HEADERS`, it contains
   the full list of header keys and values. If a column is marked by
   `HEADER('<key>')`, it contains the last header that matches the key, or
   `NULL` if that key is not in the list of headers.

* `PRIMARY KEY`: columns that are stored in the {{ site.ak }} message's key
   should be marked as `PRIMARY KEY` columns. If a column is unmarked, ksqlDB
   loads it from the {{ site.ak }} message's value. Unlike a stream's `KEY`
   column, a table's `PRIMARY KEY` column(s) are `NOT NULL`. Any records in the
   {{ site.ak }} topic with `NULL` key columns are dropped.

### Serialization

For supported [serialization formats](/reference/serialization),
ksqlDB can integrate with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html)
to help ensure the correct message format for a table. 

ksqlDB can use [Schema Inference](/operate-and-deploy/schema-registry-integration#schema-inference)
to define columns automatically in your `CREATE TABLE` statements, so you
don't need to declare them manually. Also, ksqlDB can use
[Schema Inference With ID](/operate-and-deploy/schema-inference-with-id)
to define columns automatically and enable using a physical schema for data
serialization.

!!! note
    - To use the Avro, Protobuf, or JSON_SR formats, you must enable {{ site.sr }}
      and set [ksql.schema.registry.url](/reference/server-configuration/#ksqlschemaregistryurl)
      in the ksqlDB Server configuration file. For more information, see
      [Configure ksqlDB for Avro, Protobuf, and JSON schemas](/operate-and-deploy/installation/server-config/avro-schema).
    - The JSON format doesn't require {{ site.sr }} to be enabled. 
    - Avro and Protobuf field names are not case sensitive in ksqlDB.
      This matches the ksqlDB column name behavior.

### ROWTIME

Each row within the table has a `ROWTIME` pseudo column, which represents the
_last modified time_ of the row. The timestamp is used by ksqlDB during windowing
operations and during joins, where data from each side of a join is processed in
time order.

The `ROWTIME` timestamp has an accuracy of milliseconds.

By default, `ROWTIME` is populated from the corresponding {{ site.ak }} message
timestamp. Set `TIMESTAMP` in the `WITH` clause to populate `ROWTIME` from a
column in the {{ site.ak }} message key or value.

For more information, see
[Time and Windows in ksqlDB Queries](/concepts/time-and-windows-in-ksqldb-queries/).

### Partitioning

Assign the `PARTITIONS` property in the `WITH` clause to specify the number of
[partitions](/developer-guide/joins/partition-data) in the table's backing
topic.

Partitioning tables is especially important for stateful or otherwise
intensive queries. For more information, see
[Parallelization](/operate-and-deploy/performance-guidelines/#parallelization).

### ROWPARTITION and ROWOFFSET

Like `ROWTIME`, `ROWPARTITION` and `ROWOFFSET` are pseudo columns. They
represent the partition and offset of the source topic.

For example, if you issue a [push query](/developer-guide/ksqldb-reference/select-push-query)
on a table backed by topic _x_ that specifies `ROWPARTITION` or `ROWOFFSET` in
the `SELECT` clause, the push query's projection contains the partition and offset
information of the underlying messages in topic _x_.

### Source tables

Provide the `SOURCE` clause to enable running
[pull queries](/developer-guide/ksqldb-reference/select-pull-query) on the table.

The `SOURCE` clause runs an internal query for the table to create a materialized
state that's used by pull queries. You can't terminate this query manually.
Terminate it by dropping the table with the
[DROP TABLE](/developer-guide/ksqldb-reference/drop-table) statement.

When you create a `SOURCE` table, the table is created as read-only. For a
read-only table, `INSERT`, `DELETE TOPIC`, and `DROP TABLE` statements aren't
permitted.

To disable the `SOURCE` table feature, set
[ksql.source.table.materialization.enabled](/reference/server-configuration/#ksqlsourcetablematerializationenabled)
to `false` in the ksqlDB Server properties file.

## Table properties

Use the `WITH` clause to specify details about your table. The `WITH` clause
supports the following properties.

### FORMAT

The serialization format of both the message key and value in the topic.
For supported formats, see [Serialization Formats](/reference/serialization).

You can't use the `FORMAT` property with the `KEY_FORMAT` or 
`VALUE_FORMAT` properties in the same `CREATE TABLE` statement.

### KAFKA_TOPIC (required)

The name of the {{ site.ak }} topic that backs the table.

The topic must already exist in {{ site.ak }}, or you must specify `PARTITIONS`
when you create the topic. The statement fails if the topic exists already with
different partition or replica counts.

### KEY_FORMAT

The serialization format of the message key in the topic. For supported formats,
see [Serialization Formats](/reference/serialization).

If not supplied, the system default is used, defined by
[ksql.persistence.default.format.key](/reference/server-configuration#ksqlpersistencedefaultformatkey).
If the default is also not set, the statement is rejected as invalid.

You can't use the `KEY_FORMAT` property with the `FORMAT` property in the
same `CREATE TABLE` statement.

### KEY_PROTOBUF_NULLABLE_REPRESENTATION

In the default configuration, primitive fields in protobuf do not distinguish `null` from the
default values (such as zero, empty string). To enable the use of a protobuf schema that can make
this distinction, set `KEY_PROTOBUF_NULLABLE_REPRESENTATION` to either `OPTIONAL` or `WRAPPER`.
The schema will be used to serialize keys for the table created by this `CREATE` statement.
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

The number of partitions in the backing topic. You must set this property if
you create a table without an existing topic, and the statement fails if the
topic doesn't exist.

You can't change the number of partitions on an existing table. To change the
partition count, you must drop the table and create it again.

### REPLICAS

The number of replicas in the backing topic. If this property isn't set, but
`PARTITIONS` is set, the default {{ site.ak }} cluster configuration for replicas
is used for creating a new topic.

### TIMESTAMP

By default, the `ROWTIME` pseudo column is the timestamp of the message in the
{{ site.ak }} topic.

You can use the `TIMESTAMP` property to override `ROWTIME` with the contents
of the specified column within the {{ site.ak }} message, similar to timestamp
extractors in the {{ site.kstreams }} API.

Time-based operations, like windowing, process a record according to the
timestamp in `ROWTIME`.

Timestamps have an accuracy of milliseconds.

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

The serialization format of the message value in the topic. For supported
formats, see [Serialization Formats](/reference/serialization).

If `VALUE_FORMAT` isn't provided, the system default is used, defined by
[ksql.persistence.default.format.value](/reference/server-configuration#ksqlpersistencedefaultformatvalue).
If the default is also not set, the statement is rejected as invalid.

You can't use the `VALUE_FORMAT` property with the `FORMAT` property in the
same `CREATE TABLE` statement.

### VALUE_PROTOBUF_NULLABLE_REPRESENTATION

In the default configuration, primitive fields in protobuf do not distinguish `null` from the
default values (such as zero, empty string). To enable the use of a protobuf schema that can make
this distinction, set `VALUE_PROTOBUF_NULLABLE_REPRESENTATION` to either `OPTIONAL` or `WRAPPER`.
The schema will be used to serialize values for the table created by this `CREATE` statement.
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
-- table with declared columns: 
CREATE TABLE users (
     id BIGINT PRIMARY KEY,
     usertimestamp BIGINT,
     gender VARCHAR,
     region_id VARCHAR
   ) WITH (
     KAFKA_TOPIC = 'my-users-topic', 
     VALUE_FORMAT = 'JSON'
   );
```

```sql
-- table with value columns loaded from Schema Registry: 
CREATE TABLE users (
     id BIGINT PRIMARY KEY
   ) WITH (
     KAFKA_TOPIC = 'my-users-topic', 
     VALUE_FORMAT = 'JSON_SR'
   );
```

```sql
-- table with value columns loaded from Schema Registry with VALUE_SCHEMA_ID: 
CREATE TABLE users (
     id BIGINT PRIMARY KEY
   ) WITH (
     KAFKA_TOPIC = 'my-users-topic', 
     VALUE_FORMAT = 'JSON_SR',
     VALUE_SCHEMA_ID =2
   );
```

```sql
-- table with key columns loaded from Schema Registry with KEY_SCHEMA_ID: 
CREATE TABLE users (
     usertimestamp BIGINT,
     gender VARCHAR,
     region_id VARCHAR
   ) WITH (
     KAFKA_TOPIC = 'my-users-topic', 
     KEY_FORMAT = 'AVRO',
     KEY_SCHEMA_ID = 1,
     VALUE_FORMAT = 'JSON_SR'
   );
```
