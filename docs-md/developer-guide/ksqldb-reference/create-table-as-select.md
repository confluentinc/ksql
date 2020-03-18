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
  [ LEFT | FULL | INNER ] JOIN join_table ON join_criteria 
  [ WINDOW window_expression ]
  [ WHERE condition ]
  [ GROUP BY grouping_expression ]
  [ HAVING having_expression ]
  EMIT CHANGES;
```

Description
-----------

Create a new ksqlDB table along with the corresponding Kafka topic and
stream the result of the SELECT query as a changelog into the topic.
Note that the WINDOW clause can only be used if the `from_item` is a
stream.

For joins, the key of the resulting table will be the value from the
column from the left table that was used in the join criteria. This
column will be registered as the key of the resulting table if included
in the selected columns.

For joins, the columns used in the join criteria must be the keys of the
tables being joined.

For more information, see [Join Event Streams with ksqlDB](../joins/join-streams-and-tables.md).

The WITH clause supports the following properties:

|     Property      |                                             Description                                              |
| ----------------- | ---------------------------------------------------------------------------------------------------- |
| KAFKA_TOPIC       | The name of the Kafka topic that backs this table. If this property is not set, then the name of the table will be used as default. |
| VALUE_FORMAT      | Specifies the serialization format of the message value in the topic. Supported formats: `JSON`, `JSON_SR`, `DELIMITED` (comma-separated value), `AVRO`, `KAFKA`, and `PROTOBUF`. If this property is not set, then the format of the input stream/table is used. For more information, see [Serialization Formats](../serialization.md#serialization-formats). |
| VALUE_DELIMITER   | Used when VALUE_FORMAT='DELIMITED'. Supports single character to be a delimiter, defaults to ','. For space and tab delimited values you must use the special values 'SPACE' or 'TAB', not an actual space or tab character. |
| PARTITIONS        | The number of partitions in the backing topic. If this property is not set, then the number of partitions of the input stream/table will be used. In join queries, the property values are taken from the left-side stream or table. The `ksql.sink.partitions` property can be set in the properties file the ksqlDB Server is started with, or by using the `SET` statement. |
| REPLICAS          | The replication factor for the topic. If this property is not set, then the number of replicas of the input stream or table will be used. In join queries, the property values are taken from the left-side stream or table. The `ksql.sink.replicas` property can be set in the properties file the ksqlDB Server is started with, or by using the `SET` statement. |
| TIMESTAMP         | Sets a field within this tables's schema to be used as the default source of `ROWTIME` for any downstream queries. Downstream queries that use time-based operations, such as windowing, will process records in this stream based on the timestamp in this field. Timestamps have a millisecond accuracy. If not supplied, the `ROWTIME` of the source stream is used. <br>**Note**: This doesn't affect the processing of the query that populates this table. For example, given the following statement:<br><pre>CREATE TABLE foo WITH (TIMESTAMP='t2') AS<br>&#0009;SELECT host, COUNT(*) FROM bar<br>&#0009;WINDOW TUMBLING (size 10 seconds)<br>&#0009;GROUP BY host<br>&#0009;EMIT CHANGES;</pre>The window into which each row of `bar` is placed is determined by bar's `ROWTIME`, not `t2`. |
| TIMESTAMP_FORMAT  | Used in conjunction with TIMESTAMP. If not set will assume that the timestamp field is a `bigint`. If it is set, then the TIMESTAMP field must be of type varchar and have a format that can be parsed with the Java `DateTimeFormatter`. If your timestamp format has characters requiring single quotes, you can escape them with two successive single quotes, `''`, for example: `'yyyy-MM-dd''T''HH:mm:ssX'`. For more information on timestamp formats, see [DateTimeFormatter](https://cnfl.io/java-dtf). |
| WRAP_SINGLE_VALUE | Controls how values are serialized where the values schema contains only a single field. The setting controls how the query will serialize values with a single-field schema.<br>If set to `true`, ksqlDB will serialize the field as a named field within a record.<br>If set to `false`, ksqlDB will serialize the field as an anonymous value.<br>If not supplied, the system default, defined by [ksql.persistence.wrap.single.values](../../operate-and-deploy/installation/server-config/config-reference.md#ksqlpersistencewrapsinglevalues) and defaulting to `true`, is used.<br>**Note:** `null` values have special meaning in ksqlDB. Care should be taken when dealing with single-field schemas where the value can be `null`. For more information, see [Single field (un)wrapping](../serialization.md#single-field-unwrapping).<br>**Note:** Supplying this property for formats that do not support wrapping, for example `DELIMITED`, or when the value schema has multiple fields, will result in an error. |


!!! note
	  - To use Avro or Protobuf, you must have {{ site.sr }} enabled and
    `ksql.schema.registry.url` must be set in the ksqlDB server configuration
    file. See [Configure ksqlDB for Avro or Protobuf](../../operate-and-deploy/installation/server-config/avro-schema.md#configure-avro-and-schema-registry-for-ksql).
    - Avro and Protobuf field names are not case sensitive in ksqlDB. This matches the ksqlDB
    column name behavior.

Example
-------

TODO: example


Page last revised on: {{ git_revision_date }}
