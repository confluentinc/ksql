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

Creating a stream means registering it on an underlying {{ site.aktm }}
topic, so you can use SQL statements to perform operations like joins and
aggregations on the topic's data. The stream is _backed by the topic_.

!!! important
    Registering a stream on a topic by using the CREATE STREAM statement is
    distinct from using the [CREATE STREAM AS SELECT](../create-stream-as-select)
    statement, which creates a stream from the result of a SELECT query.

Specify CREATE OR REPLACE to replace an existing stream with a new query that
resumes from the same processing point as the previously existing query.

If you provide the IF NOT EXISTS clause, the statement won't fail if a stream
with the same name already exists. Instead, ksqlDB returns a warning,
`A stream with the same name already exists.`

For more information, see [Stream Processing](/concepts/stream-processing).

!!! Tip "See CREATE STREAM in action"
    - [Detect Unusual Credit Card Activity](https://confluentinc.github.io/ksqldb-recipes/anomaly-detection/credit-card-activity/#ksqldb-code)
    - [Handle corrupted data from Salesforce](https://confluentinc.github.io/ksqldb-recipes/anomaly-detection/salesforce/#ksqldb-code)
    - [Match Users for Online Dating](https://confluentinc.github.io/ksqldb-recipes/customer-360/online-dating/#ksqldb-code)

### Columns

A stream can store its data in `KEY`, `VALUE` or `HEADERS`/`HEADER('<key>')`
columns.

`KEY`, `VALUE` and `HEADER('<key>')` columns can be NULL.

`HEADERS` columns can't be NULL. If the {{ site.ak }} message doesn't have
headers, the `HEADERS` columns are populated by an empty array.

If two rows have the same `KEY`, no special processing is done. This
situation is handled differently by [ksqlDB TABLEs](../create-table),
as shown in the following table.

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
   keys can be NULL.

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

Assign the PARTITIONS property in the WITH clause to specify the number of
[partitions](/developer-guide/joins/partition-data) in the stream's backing
topic.

Partitioning streams is especially important for stateful or otherwise
intensive queries. For more information, see
[Parallelization](/operate-and-deploy/performance-guidelines/#parallelization).

### ROWPARTITION and ROWOFFSET

Like `ROWTIME`, `ROWPARTITION` and `ROWOFFSET` are pseudo columns. They
represent the partition and offset of the *source* topic.

For example, if you issue a [push query](/developer-guide/ksqldb-reference/select-push-query)
on a stream backed by topic _x_ that specifies `ROWPARTITION` or `ROWOFFSET` in
the SELECT clause, the push query's projection contains the partition and offset
information of the underlying messages in topic _x_.

### Source streams

Provide the SOURCE clause to create a read-only stream.

When you create a SOURCE stream, the INSERT, DELETE TOPIC, and DROP STREAM
statements aren't permitted. Also, source streams don't support
[pull queries](/developer-guide/ksqldb-reference/select-pull-query). 

To disable the SOURCE stream feature, set
[ksql.source.table.materialization.enabled](/reference/server-configuration/#ksqlsourcetablematerializationenabled)
to `false` in the ksqlDB Server properties file.

!!! note
    Source tables support running pull queries on them. For more information,
    see [CREATE TABLE](/developer-guide/ksqldb-reference/create-table/#source-tables).

## Stream properties

Use the WITH clause to specify details about your stream. The WITH clause
supports the following properties:

|        Property         |                                            Description                                            |
| ----------------------- | ------------------------------------------------------------------------------------------------- |
| KAFKA_TOPIC (required)  | The name of the Kafka topic that backs this source. The topic must either already exist in Kafka, or PARTITIONS must be specified to create the topic. Command will fail if the topic exists with different partition/replica counts. |
| KEY_FORMAT              | Specifies the serialization format of the message key in the topic. For supported formats, see [Serialization Formats](/reference/serialization). If not supplied, the system default is used, defined by [ksql.persistence.default.format.key](/reference/server-configuration#ksqlpersistencedefaulformatkey). If the default is also not set the statement will be rejected as invalid. |
| KEY_SCHEMA_ID           | Specifies the schema ID of key schema in {{ site.sr }}. The schema will be used for schema inference and data serialization. See [Schema Inference With Schema ID](/operate-and-deploy/schema-inference-with-id). |
| VALUE_FORMAT            | Specifies the serialization format of the message value in the topic. For supported formats, see [Serialization Formats](/reference/serialization). If not supplied, the system default is used, defined by [ksql.persistence.default.format.value](/reference/server-configuration#ksqlpersistencedefaultformatvalue). If the default is also not set the statement will be rejected as invalid. |
| VALUE_SCHEMA_ID         | Specifies the schema ID of value schema in {{ site.sr }}. The schema will be used for schema inference and data serialization. See [Schema Inference With Schema ID](/operate-and-deploy/schema-inference-with-id). |
| FORMAT                  | Specifies the serialization format of both the message key and value in the topic. It is not valid to supply this property alongside either `KEY_FORMAT` or `VALUE_FORMAT`. For supported formats, see [Serialization Formats](/reference/serialization). |
| PARTITIONS              | The number of partitions in the backing topic. This property must be set if creating a STREAM without an existing topic (the command will fail if the topic does not exist). You can't change the number of partitions on a stream. To change the partition count, you must drop the stream and create it again. |
| REPLICAS                | The number of replicas in the backing topic. If this property is not set but PARTITIONS is set, then the default Kafka cluster configuration for replicas will be used for creating a new topic. |
| VALUE_DELIMITER         | Used when VALUE_FORMAT='DELIMITED'. Supports single character to be a delimiter, defaults to ','. For space and tab delimited values you must use the special values 'SPACE' or 'TAB', not an actual space or tab character. |
| TIMESTAMP               | By default, the pseudo `ROWTIME` column is the timestamp of the record in the Kafka topic. The TIMESTAMP property can be used to override `ROWTIME` with the contents of the specified column within the Kafka message (similar to timestamp extractors in Kafka's Streams API). Timestamps have a millisecond accuracy. Time-based operations, such as windowing, will process a record according to the timestamp in `ROWTIME`.  |
| TIMESTAMP_FORMAT        | Used in conjunction with TIMESTAMP. If not set, ksqlDB timestamp column must be of type `bigint` or `timestamp`. If it is set, the TIMESTAMP column must be of type `varchar` and have a format that can be parsed with the java `DateTimeFormatter`. If your timestamp format has characters requiring single quotes, you can escape them with successive single quotes, `''`, for example: `'yyyy-MM-dd''T''HH:mm:ssX'`. For more information on timestamp formats, see [DateTimeFormatter](https://cnfl.io/java-dtf).                                       |
| WRAP_SINGLE_VALUE       | Controls how values are deserialized where the value schema contains only a single column. The setting controls how ksqlDB will deserialize the value of the records in the supplied `KAFKA_TOPIC` that contain only a single column. If set to `true`, ksqlDB expects the column to have been serialized as a named column within a record. If set to `false`, ksqlDB expects the column to have been serialized as an anonymous value. If not supplied, the system default is used, defined by [ksql.persistence.wrap.single.values](/reference/server-configuration#ksqlpersistencewrapsinglevalues) and defaulting to `true`.<br>**Note:** `null` values have special meaning in ksqlDB. Care should be taken when dealing with single-column schemas where the value can be `null`. For more information, see [Single column (un)wrapping](/reference/serialization#single-field-unwrapping).<br>**Note:** Supplying this property for formats that do not support wrapping, for example `DELIMITED`, or when the value schema has multiple columns, will result in an error. |
| WINDOW_TYPE             | By default, the topic is assumed to contain non-windowed data. If the data is windowed, i.e., was created using ksqlDB using a query that contains a `WINDOW` clause, the `WINDOW_TYPE` property can be used to provide the window type. Valid values are `SESSION`, `HOPPING`, and `TUMBLING`. |
| WINDOW_SIZE             | By default, the topic is assumed to contain non-windowed data. If the data is windowed, i.e., was created using ksqlDB using a query that contains a `WINDOW` clause, and the `WINDOW_TYPE` property is TUMBLING or HOPPING, then the WINDOW_SIZE property should be set. The property is a string with two literals, window size (a number) and window size unit (a time unit). For example: `10 SECONDS`. |


For more information on timestamp formats, see
[DateTimeFormatter](https://cnfl.io/java-dtf).

!!! note
    - To use the Avro, Protobuf, or JSON_SR formats, you must enable {{ site.sr }}
      and set `ksql.schema.registry.url` in the ksqlDB Server configuration file.
      The JSON format doesn't require {{ site.sr }} to be enabled. For more
      information, see
      [Configure ksqlDB for Avro, Protobuf, and JSON schemas](/operate-and-deploy/installation/server-config/avro-schema). 
    - Avro and Protobuf field names are not case sensitive in ksqlDB.
      This matches the ksqlDB column name behavior.

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
