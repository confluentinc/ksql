---
layout: page
title: CREATE STREAM
tagline:  ksqlDB CREATE STREAM statement
description: Syntax for the CREATE STREAM statement in ksqlDB
keywords: ksqlDB, create, stream
---

## Synopsis

```sql
CREATE [OR REPLACE] STREAM [IF NOT EXISTS] stream_name ( { column_name data_type [KEY] } [, ...] )
  WITH ( property_name = expression [, ...] );
```

## Description

Create a new stream with the specified columns and properties.

If the IF NOT EXISTS clause is present, the statement won't fail if a 
stream with the same name already exists.

### Partitioning

Assign the PARTITIONS property in the WITH clause to specify the number of
partitions in the stream's backing topic. Partitioning streams and tables is
especially important for stateful or otherwise intensive queries. For more
information, see [Parallelization](/operate-and-deploy/performance-guidelines/#parallelization).

### KEY and VALUE columns

A ksqlDB STREAM is a stream of _facts_. Each fact is immutable and unique.
A stream can store its data in either `KEY` or `VALUE` columns.
Both `KEY` and `VALUE` columns can be NULL. No special processing is done if two rows have the same
key. This situation is handled differently by [ksqlDB TABLEs](../create-table), as shown in the following table.

|                          |  STREAM                                                       | TABLE                                                             |
| ------------------------ | --------------------------------------------------------------| ----------------------------------------------------------------- |
| Key column type          | `KEY`                                                         | `PRIMARY KEY`                                                     |
| NON NULL key constraint  | No                                                            | Yes <br> Records in the {{ site.ak }} topic with a NULL `PRIMARY KEY` are ignored.  |
| Unique key constraint    | No <br> Messages with the same key as another have no special meaning. | Yes <br> Later messages with the same key _replace_ earlier. |
| Tombstones               | No <br> Messages with NULL values are ignored.                | Yes <br> NULL message values are treated as a _tombstone_ <br> Any existing row with a matching key is deleted. |

Each column is defined by:

* `column_name`: the name of the column. If unquoted, the name must be a valid
   [SQL identifier](/reference/sql/syntax/lexical-structure#identifiers) and ksqlDB converts it to uppercase.
   The name can be quoted if case needs to be preserved or if the name is not a valid SQL
   identifier, for example ``` `mixedCaseId` ``` or ``` `$with@invalid!chars` ```.

* `data_type`: the SQL type of the column. Columns can be any of the
   [data types](/reference/sql/data-types) supported by ksqlDB.

* `KEY`: columns that are stored in the Kafka message's key should be marked as `KEY` columns.
   If a column is not marked as a `KEY` column, ksqlDB loads it from the Kafka message's value.
   Unlike a table's `PRIMARY KEY`, a stream's keys can be NULL.
   
For supported [serialization formats](/reference/serialization),
ksqlDB can integrate with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).
ksqlDB can use [Schema Inference](/operate-and-deploy/schema-registry-integration#schema-inference) to
spare you from defining columns manually in your `CREATE STREAM` statements.
### ROWTIME

Each row within the stream has a `ROWTIME` pseudo column, which represents the _event time_ 
of the row. The timestamp has milliseconds accuracy. The timestamp is used by ksqlDB during any
windowing operations and during joins, where data from each side of a join is generally processed
in time order.  

By default, `ROWTIME` is populated from the corresponding {{ site.ak }} message timestamp. Set `TIMESTAMP` in
the `WITH` clause to populate `ROWTIME` from a column in the {{ site.ak }} message key or value.
## Stream properties

Specify details about your stream by using the WITH clause, which supports the
following properties:

|        Property         |                                            Description                                            |
| ----------------------- | ------------------------------------------------------------------------------------------------- |
| KAFKA_TOPIC (required)  | The name of the Kafka topic that backs this source. The topic must either already exist in Kafka, or PARTITIONS must be specified to create the topic. Command will fail if the topic exists with different partition/replica counts. |
| KEY_FORMAT              | Specifies the serialization format of the message key in the topic. For supported formats, see [Serialization Formats](/reference/serialization).<br>If not supplied, the system default, defined by [ksql.persistence.default.format.key](/reference/server-configuration#ksqlpersistencedefaulformatkey), is used. If the default is also not set the statement will be rejected as invalid. |
| VALUE_FORMAT            | Specifies the serialization format of the message value in the topic. For supported formats, see [Serialization Formats](/reference/serialization).<br>If not supplied, the system default, defined by [ksql.persistence.default.format.value](/reference/server-configuration#ksqlpersistencedefaultformatvalue), is used. If the default is also not set the statement will be rejected as invalid. |
| FORMAT                  | Specifies the serialization format of both the message key and value in the topic. It is not valid to supply this property alongside either `KEY_FORMAT` or `VALUE_FORMAT`. For supported formats, see [Serialization Formats](/reference/serialization). |
| PARTITIONS              | The number of partitions in the backing topic. This property must be set if creating a STREAM without an existing topic (the command will fail if the topic does not exist). You can't change the number of partitions on a stream. To change the partition count, you must drop the stream and create it again. |
| REPLICAS                | The number of replicas in the backing topic. If this property is not set but PARTITIONS is set, then the default Kafka cluster configuration for replicas will be used for creating a new topic. |
| VALUE_DELIMITER         | Used when VALUE_FORMAT='DELIMITED'. Supports single character to be a delimiter, defaults to ','. For space and tab delimited values you must use the special values 'SPACE' or 'TAB', not an actual space or tab character. |
| TIMESTAMP               | By default, the pseudo `ROWTIME` column is the timestamp of the record in the Kafka topic. The TIMESTAMP property can be used to override `ROWTIME` with the contents of the specified column within the Kafka message (similar to timestamp extractors in Kafka's Streams API). Timestamps have a millisecond accuracy. Time-based operations, such as windowing, will process a record according to the timestamp in `ROWTIME`.  |
| TIMESTAMP_FORMAT        | Used in conjunction with TIMESTAMP. If not set the timestamp column must be of type `bigint`. If it is set, then the TIMESTAMP column must be of type `varchar` and have a format that can be parsed with the java `DateTimeFormatter`. If your timestamp format has characters requiring single quotes, you can escape them with successive single quotes, `''`, for example: `'yyyy-MM-dd''T''HH:mm:ssX'`. For more information on timestamp formats, see [DateTimeFormatter](https://cnfl.io/java-dtf).                                       |
| WRAP_SINGLE_VALUE       | Controls how values are deserialized where the value schema contains only a single column. The setting controls how ksqlDB will deserialize the value of the records in the supplied `KAFKA_TOPIC` that contain only a single column.<br>If set to `true`, ksqlDB expects the column to have been serialized as a named column within a record.<br>If set to `false`, ksqlDB expects the column to have been serialized as an anonymous value.<br>If not supplied, the system default, defined by [ksql.persistence.wrap.single.values](/reference/server-configuration#ksqlpersistencewrapsinglevalues) and defaulting to `true`, is used.<br>**Note:** `null` values have special meaning in ksqlDB. Care should be taken when dealing with single-column schemas where the value can be `null`. For more information, see [Single column (un)wrapping](/reference/serialization#single-field-unwrapping).<br>**Note:** Supplying this property for formats that do not support wrapping, for example `DELIMITED`, or when the value schema has multiple columns, will result in an error. |
| WINDOW_TYPE             | By default, the topic is assumed to contain non-windowed data. If the data is windowed, i.e., was created using ksqlDB using a query that contains a `WINDOW` clause, then the `WINDOW_TYPE` property can be used to provide the window type. Valid values are `SESSION`, `HOPPING`, and `TUMBLING`. |
| WINDOW_SIZE             | By default, the topic is assumed to contain non-windowed data. If the data is windowed, i.e., was created using ksqlDB using a query that contains a `WINDOW` clause, and the `WINDOW_TYPE` property is TUMBLING or HOPPING, then the WINDOW_SIZE property should be set. The property is a string with two literals, window size (a number) and window size unit (a time unit). For example: `10 SECONDS`. |


For more information on timestamp formats, see
[DateTimeFormatter](https://cnfl.io/java-dtf).

!!! note
	  - To use Avro or Protobuf, you must have {{ site.sr }} enabled and
    `ksql.schema.registry.url` must be set in the ksqlDB Server configuration
    file. See [Configure ksqlDB for Avro, Protobuf, and JSON schemas](../../operate-and-deploy/installation/server-config/avro-schema.md). 
    - Avro and Protobuf field names are not case sensitive in ksqlDB. This matches the ksqlDB column name behavior.

## Example

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
    VALUE_FORMAT = 'JSON'
  );

-- keyed stream, with value columns loaded from Schema Registry:
CREATE STREAM pageviews (
    page_id BIGINT KEY
  ) WITH (
    KAFKA_TOPIC = 'keyed-pageviews-topic',
    VALUE_FORMAT = 'JSON'
  );
```
