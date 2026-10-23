---
layout: page
title: CREATE STREAM
tagline:  ksqlDB CREATE STREAM statement syntax
description: Register a stream on a Kafka topic to enable SQL joins and aggregations.
keywords: ksqlDB, create, stream
---

## Synopsis

```sql
CREATE [OR REPLACE] [SOURCE] STREAM [IF NOT EXISTS] stream_name 
  ( { column_name data_type [KEY | HEADERS | HEADER(key)] } [, ...] )
  WITH ( property_name = expression [, ...] );
```

## Description

Create a new stream with the specified columns and properties.

Creating a stream registers it on an underlying {{ site.aktm }} topic, so you
can use SQL statements to perform operations like joins and aggregations on the
topic's data. The stream is said to be _backed by the topic_.

!!! important
    Registering a stream on a topic by using the `CREATE STREAM` statement is
    distinct from using the [CREATE STREAM AS SELECT](../create-stream-as-select)
    statement, which creates a stream from the result of a SELECT query.

Specify `CREATE OR REPLACE` to replace an existing stream with a new query that
resumes from the same processing point as the previously existing query.

If you provide the `IF NOT EXISTS` clause, the statement won't fail if a stream
with the same name already exists. Instead, ksqlDB returns a warning,
`A stream with the same name already exists.`

For more information, see [Stream Processing](/concepts/stream-processing).

!!! Tip "See CREATE STREAM in action"
    - [Detect Unusual Credit Card Activity](https://developer.confluent.io/tutorials/credit-card-activity/confluent.html#execute-ksqldb-code)
    - [Handle corrupted data from Salesforce](https://developer.confluent.io/tutorials/salesforce/confluent.html#execute-ksqldb-code)
    - [Match Users for Online Dating](https://developer.confluent.io/tutorials/online-dating/confluent.html#execute-ksqldb-code)

### Columns

A stream can store its data in `KEY`, `VALUE`, or `HEADERS`/`HEADER('<key>')`
columns.

`KEY`, `VALUE`, and `HEADER('<key>')` columns can be NULL.

`HEADERS` columns can't be NULL. If the {{ site.ak }} message doesn't have
headers, the `HEADERS` columns are populated by an empty array.

If two rows have the same `KEY`, no special processing is done. This
situation is handled differently by a [ksqlDB TABLE](../create-table),
as shown in the following summary.

|                         |  STREAM   | TABLE          |
| ----------------------- | --------- | -------------- |
| Key column type         | `KEY`     | `PRIMARY KEY`  |
| NON NULL key constraint | **No**    | **Yes:** A message in the {{ site.ak }} topic with a NULL `PRIMARY KEY` is ignored.  |
| Unique key constraint   | **No:** A message with the same key as another has no special meaning. | **Yes:** A later message with the same key _replaces_ earlier messages in the table. |
| Tombstones              | **No:** A message with a NULL value is ignored. | **Yes:** A NULL message value is treated as a _tombstone_. Any existing row with a matching key is deleted from the table. |

Each column in a stream is defined by the following syntax:

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

* `KEY`: columns that are stored in the {{ site.ak }} message's key should be
   marked as `KEY` columns. If a column is unmarked, ksqlDB loads it from the
   {{ site.ak }} message's value. Unlike a table's `PRIMARY KEY`, a stream's
   keys can be `NULL`.

### Serialization

For supported [serialization formats](/reference/serialization),
ksqlDB can integrate with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html)
to help ensure the correct message format for a stream. 

ksqlDB can use [Schema Inference](/operate-and-deploy/schema-registry-integration#schema-inference)
to define columns automatically in your `CREATE STREAM` statements, so you
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

Each row within the stream has a `ROWTIME` pseudo column, which represents the
[event time](/concepts/time-and-windows-in-ksqldb-queries/#event-time) of the
row. The timestamp is used by ksqlDB during windowing operations and during
joins, where data from each side of a join is processed in time order.

The `ROWTIME` timestamp has an accuracy of milliseconds.

By default, `ROWTIME` is populated from the corresponding {{ site.ak }} message
timestamp. Set `TIMESTAMP` in the `WITH` clause to populate `ROWTIME` from a
column in the {{ site.ak }} message key or value.

For more information, see
[Time and Windows in ksqlDB Queries](/concepts/time-and-windows-in-ksqldb-queries/).

### Partitioning

Assign the `PARTITIONS` property in the `WITH` clause to specify the number of
[partitions](/developer-guide/joins/partition-data) in the stream's backing
topic.

Partitioning streams is especially important for stateful or otherwise
intensive queries. For more information, see
[Parallelization](/operate-and-deploy/performance-guidelines/#parallelization).

### ROWPARTITION and ROWOFFSET

Like `ROWTIME`, `ROWPARTITION` and `ROWOFFSET` are pseudo columns. They
represent the partition and offset of the source topic.

For example, if you issue a [push query](/developer-guide/ksqldb-reference/select-push-query)
on a stream backed by topic _x_ that specifies `ROWPARTITION` or `ROWOFFSET` in
the `SELECT` clause, the push query's projection contains the partition and offset
information of the underlying messages in topic _x_.

### Source streams

Provide the `SOURCE` clause to create a read-only stream.

When you create a `SOURCE` stream, the `INSERT`, `DELETE TOPIC`, and
`DROP STREAM` statements aren't permitted. Also, source streams don't support
[pull queries](/developer-guide/ksqldb-reference/select-pull-query). 

!!! note
    Source tables support pull queries. For more information,
    see [CREATE TABLE](/developer-guide/ksqldb-reference/create-table/#source-tables).

To disable the `SOURCE` stream feature, set
[ksql.source.table.materialization.enabled](/reference/server-configuration/#ksqlsourcetablematerializationenabled)
to `false` in the ksqlDB Server properties file.

## Stream properties

Use the `WITH` clause to specify details about your stream. The `WITH` clause
supports the following properties.

### FORMAT

The serialization format of both the message key and value in the topic.
For supported formats, see [Serialization Formats](/reference/serialization).

You can't use the `FORMAT` property with the `KEY_FORMAT` or 
`VALUE_FORMAT` properties in the same `CREATE STREAM` statement.

### KAFKA_TOPIC (required)

The name of the {{ site.ak }} topic that backs the stream.

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
same `CREATE STREAM` statement.

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

The number of partitions in the backing topic. You must set this property if
you create a stream without an existing topic, and the statement fails if the
topic doesn't exist.

You can't change the number of partitions on an existing stream. To change the
partition count, you must drop the stream and create it again.

### REPLICAS

The number of replicas in the backing topic. If this property isn't set, but
`PARTITIONS` is set, the default {{ site.ak }} cluster configuration for replicas
is used for creating a new topic.

### RETENTION_MS

!!! note
    Available starting in version `0.28.3-RC7`.

The retention specified in milliseconds in the backing topic.

If you create a stream without an existing topic, `RETENTION_MS` is set to the
broker default log retention.

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

The serialization format of the message value in the topic. For supported formats,
see [Serialization Formats](/reference/serialization).

If `VALUE_FORMAT` isn't provided, the system default is used, defined by
[ksql.persistence.default.format.value](/reference/server-configuration#ksqlpersistencedefaultformatvalue).
If the default is also not set, the statement is rejected as invalid.

You can't use the `VALUE_FORMAT` property with the `FORMAT` property in the
same CREATE STREAM statement.

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

## Examples

```sql
-- stream with a page_id column loaded from the kafka message value:
CREATE STREAM pageviews (
    page_id BIGINT,
    viewtime BIGINT,
    user_id VARCHAR
  ) WITH (
    KAFKA_TOPIC = 'keyless-pageviews-topic',
    VALUE_FORMAT = 'JSON'
  );

-- stream with a page_id column loaded from the kafka message key:
CREATE STREAM pageviews (
    page_id BIGINT KEY,
    viewtime BIGINT,
    user_id VARCHAR
  ) WITH (
    KAFKA_TOPIC = 'keyed-pageviews-topic',
    VALUE_FORMAT = 'JSON'
  );

-- keyless stream, with value columns loaded from Schema Registry:
CREATE STREAM pageviews WITH (
    KAFKA_TOPIC = 'keyless-pageviews-topic',
    VALUE_FORMAT = 'JSON_SR'
  );

-- keyed stream, with value columns loaded from Schema Registry:
CREATE STREAM pageviews (
    page_id BIGINT KEY
  ) WITH (
    KAFKA_TOPIC = 'keyed-pageviews-topic',
    VALUE_FORMAT = 'JSON_SR'
  );
  
-- Stream with key column loaded from Schema Registry by specifying KEY_SCHEMA_ID:
CREATE STREAM pageviews (
    viewtime BIGINT,
    user_id VARCHAR
  ) WITH (
    KAFKA_TOPIC = 'keyless-pageviews-topic',
    KEY_FORMAT = 'AVRO',
    KEY_SCHEMA_ID = 1,
    VALUE_FORMAT = 'JSON_SR'
  );

-- Stream with value column loaded from Schema Registry by specifying VALUE_SCHEMA_ID:
CREATE STREAM pageviews (
    page_id BIGINT KEY
  ) WITH (
    KAFKA_TOPIC = 'keyed-pageviews-topic',
    VALUE_FORMAT = 'JSON_SR',
    VALUE_SCHEMA_ID = 2
  );
```
