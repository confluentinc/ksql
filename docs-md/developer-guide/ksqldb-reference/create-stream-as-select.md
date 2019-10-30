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
CREATE STREAM stream_name
  [WITH ( property_name = expression [, ...] )]
  AS SELECT  select_expr [, ...]
  FROM from_stream
  [ LEFT | FULL | INNER ] JOIN [join_table | join_stream] [ WITHIN [(before TIMEUNIT, after TIMEUNIT) | N TIMEUNIT] ] ON join_criteria 
  [ WHERE condition ]
  [PARTITION BY column_name]
  EMIT CHANGES;
```

Description
-----------

Create a new stream along with the corresponding Kafka topic, and
continuously write the result of the SELECT query into the stream and
its corresponding topic.

If the PARTITION BY clause is present, then the resulting stream will
have the specified column as its key. The `column_name` must be present
in the `select_expr`. For more information, see
[Partition Data to Enable Joins](../partition-data.md).

For joins, the key of the resulting stream will be the value from the
column from the left stream that was used in the join criteria. This
column will be registered as the key of the resulting stream if included
in the selected columns.

For stream-table joins, the column used in the join criteria for the
table must be the table key.

For stream-stream joins, you must specify a WITHIN clause for matching
records that both occur within a specified time interval. For valid time
units, see [KSQL Time Units](../syntax-reference.md#ksql-time-units).

For more information, see [Join Event Streams with KSQL](../join-streams-and-tables.md).

The WITH clause for the result supports the following properties:

TODO: Fix table cells

|     Property      |                                             Description                                              |
| ----------------- | ---------------------------------------------------------------------------------------------------- |
| KAFKA_TOPIC       | The name of the Kafka topic that backs this stream. If this property is not set, then the            |
|                   | name of the stream in upper case will be used as default.                                            |
| VALUE_FORMAT      | Specifies the serialization format of the message value in the topic. Supported formats:             |
|                   | `JSON`, `DELIMITED` (comma-separated value), `AVRO` and `KAFKA`.                                     |
|                   | If this property is not set, then the format of the input stream/table is used.                      |
|                   | For more information, see [Serialization Formats](../serialization.md#serialization-formats).           |
| VALUE_DELIMITER   | Used when VALUE_FORMAT='DELIMITED'. Supports single character to be a delimiter,                     |
|                   | defaults to ','.                                                                                     |
|                   | For space and tab delimited values you must use the special values 'SPACE' or 'TAB', not             |
|                   | an actual space or tab character.                                                                    |
| PARTITIONS        | The number of partitions in the backing topic. If this property is not set, then the number          |
|                   | of partitions of the input stream/table will be used. In join queries, the property values are taken |
|                   | from the left-side stream or table.                                                                  |
|                   | For KSQL 5.2 and earlier, if the property is not set, the value of the `ksql.sink.partitions`        |
|                   | property, which defaults to four partitions, will be used. The `ksql.sink.partitions` property can   |
|                   | be set in the properties file the KSQL server is started with, or by using the `SET` statement.      |
| REPLICAS          | The replication factor for the topic. If this property is not set, then the number of                |
|                   | replicas of the input stream or table will be used. In join queries, the property values are taken   |
|                   | from the left-side stream or table.                                                                  |
|                   | For KSQL 5.2 and earlier, if the REPLICAS is not set, the value of the `ksql.sink.replicas`          |
|                   | property, which defaults to one replica, will be used. The `ksql.sink.replicas` property can         |
|                   | be set in the properties file the KSQL server is started with, or by using the `SET` statement.      |
| TIMESTAMP         | Sets a field within this stream's schema to be used as the default source of `ROWTIME` for           |
|                   | any downstream queries. Downstream queries that use time-based operations, such as windowing,        |
|                   | will process records in this stream based on the timestamp in this field. By default,                |
|                   | such queries will also use this field to set the timestamp on any records emitted to Kafka.          |
|                   | Timestamps have a millisecond accuracy.                                                              |
|                   |                                                                                                      |
|                   | If not supplied, the `ROWTIME` of the source stream will be used.                                    |
|                   |                                                                                                      |
|                   | **Note**: This doesn't affect the processing of the query that populates this stream.                |
|                   | For example, given the following statement:                                                          |
|                   | TODO: Solve this code block issue                                                                    |
|                   | CREATE STREAM foo WITH (TIMESTAMP='t2') AS                                                           |
|                   |   SELECT * FROM bar	  SELECT * FROM bar                                                              |
|                   |   WINDOW TUMBLING (size 10 seconds); 	  WINDOW TUMBLING (size 10 seconds)                            |
|                   |   EMIT CHANGES;                                                                                      |
|                   |                                                                                                      |
|                   | The window into which each row of `bar` is placed is determined by bar's `ROWTIME`, not `t2`.        |
| TIMESTAMP_FORMAT  | Used in conjunction with TIMESTAMP. If not set will assume that the timestamp field is a             |
|                   | bigint. If it is set, then the TIMESTAMP field must be of type varchar and have a format             |
|                   | that can be parsed with the Java `DateTimeFormatter`. If your timestamp format has                   |
|                   | characters requiring single quotes, you can escape them with two successive single quotes,           |
|                   | `''`, for example: `'yyyy-MM-dd''T''HH:mm:ssX'`. For more information on timestamp                   |
|                   | formats, see [DateTimeFormatter](https://cnfl.io/java-dtf).                                          |
| WRAP_SINGLE_VALUE | Controls how values are serialized where the values schema contains only a single field.             |
|                   |                                                                                                      |
|                   | The setting controls how the query will serialize values with a single-field schema.                 |
|                   | If set to `true`, KSQL will serialize the field as a named field within a record.                    |
|                   | If set to `false` KSQL, KSQL will serialize the field as an anonymous value.                         |
|                   | If not supplied, the system default, defined by [ksql.persistence.wrap.single.values](../../installation/server-config/config-reference.md#ksqlpersistencewrapsinglevalues) and       |
|                   | defaulting to `true``, is used.                                                                      |
|                   |                                                                                                      |
|                   | Note: `null` values have special meaning in KSQL. Care should be taken when dealing with             |
|                   | single-field schemas where the value can be `null`. For more information, see                        |
|                   | [Single field (un)wrapping](../serialization.md#single-field-unwrapping).                               |
|                   |                                                                                                      |
|                   | Note: Supplying this property for formats that do not support wrapping, for example                  |
|                   | `DELIMITED`, or when the value schema has multiple fields, will result in an error.                  |


!!! note
		- To use Avro, you must have {{ site.sr }} enabled and
    `ksql.schema.registry.url` must be set in the KSQL server configuration
    file. See [Configure Avro and {{ site.sr }} for KSQL](../../installation/server-config/avro-schema.md). 
    - Avro field names >are not case sensitive in KSQL. This matches the KSQL
    column name behavior.

!!! note
		The `KEY` property is not supported -- use PARTITION BY instead.

Example
-------

TODO: example