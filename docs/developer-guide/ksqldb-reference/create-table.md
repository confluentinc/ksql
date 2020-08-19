---
layout: page
title: CREATE TABLE
tagline:  ksqlDB CREATE TABLE statement
description: Syntax for the CREATE TABLE statement in ksqlDB
keywords: ksqlDB, create, table
---

Synopsis
--------

```sql
CREATE TABLE table_name ( { column_name data_type [PRIMARY KEY] } [, ...] )
  WITH ( property_name = expression [, ...] );
```

Description
-----------

Create a new table with the specified columns and properties.

A ksqlDB TABLE works much like tables in other SQL systems. A table has zero or more rows. Each
row is identified by its `PRIMARY KEY`. A row's `PRIMARY KEY` can not be `NULL`. A message in the
underlying Kafka topic with the same key as an existing row will _replace_ the existing row in the
table, or _delete_ the row if the message's value is `NULL`, i.e. a _tombstone_, as long as it has
a later timestamp / `ROWTIME`. 
This situation is handled differently by [ksqlDB STREAM](./create-stream), as shown in the following table.

|                          |  STREAM                                                       | TABLE                                                             |
| ------------------------ | --------------------------------------------------------------| ----------------------------------------------------------------- |
| Key column type          | `KEY`                                                         | `PRIMARY KEY`                                                     |
| NON NULL key constraint  | No                                                            | Yes <br> Messages in the Kafka topic with a NULL `PRIMARY KEY` are ignored. |
| Unique key constraint    | No <br> Messages with the same key as another have no special meaning. | Yes <br> Later messages with the same key _replace_ earlier. |
| Tombstones               | No <br> Messages with NULL values are ignored.                | Yes <br> NULL message values are treated as a _tombstone_ <br> Any existing row with a matching key is deleted. |

Each column is defined by:
 * `column_name`: the name of the column. If unquoted, the name must be a valid
   [SQL identifier](../../concepts/schemas#valid-identifiers) and ksqlDB converts it to uppercase.
   The name can be quoted if case needs to be preserved or if the name is not a valid SQL
   identifier, for example ``` `mixedCaseId` ``` or ``` `$with@invalid!chars` ```.
 * `data_type`: the SQL type of the column. Columns can be any of the
   [data types](../syntax-reference.md#data-types) supported by ksqlDB.
 * `PRIMARY KEY`: columns that are stored in the Kafka message's key should be marked as
   `PRIMARY KEY` columns. If a column is not marked as a `PRIMARY KEY` column ksqlDB loads it
   from the {{ site.ak }} message's value. Unlike a stream's `KEY` column, a table's `PRIMARY KEY` column(s)
   are NON NULL. Any records in the Kafka topic with NULL key columns are dropped.
   
For supported [serialization formats](../developer-guide/serialization.md),
ksqlDB can integrate with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).
ksqlDB can use [Schema Inference](../concepts/schemas.md#schema-inference) to
spare you from defining columns manually in your `CREATE TABLE` statements.
   
Each row within the table has a `ROWTIME` pseudo column, which represents the _last modified time_ 
of the row. The timestamp has milliseconds accuracy. The timestamp is used by ksqlDB when updating
a row, during any windowing operations and during joins, where data from each side of a join is 
generally processed in time order.  

By default `ROWTIME` is populated from the corresponding Kafka message timestamp. Set `TIMESTAMP` in
the `WITH` clause to populate `ROWTIME` from a column in the Kafka message key or value.

The WITH clause supports the following properties:

|        Property         |                                            Description                                            |
| ----------------------- | ------------------------------------------------------------------------------------------------- |
| KAFKA_TOPIC (required)  | The name of the Kafka topic that backs this source. The topic must either already exist in Kafka, or PARTITIONS must be specified to create the topic. Command will fail if the topic exists with different partition/replica counts. |
| VALUE_FORMAT (required) | Specifies the serialization format of message values in the topic. For supported formats, see [Serialization Formats](../serialization.md#serialization-formats). |
| PARTITIONS              | The number of partitions in the backing topic. This property must be set if creating a TABLE without an existing topic (the command will fail if the topic does not exist). |
| REPLICAS                | The number of replicas in the backing topic. If this property is not set but PARTITIONS is set, then the default Kafka cluster configuration for replicas will be used for creating a new topic. |
| VALUE_DELIMITER         | Used when VALUE_FORMAT='DELIMITED'. Supports single character to be a delimiter, defaults to ','. For space and tab delimited values you must use the special values 'SPACE' or 'TAB', not an actual space or tab character. |
| TIMESTAMP               | By default, the pseudo `ROWTIME` column is the timestamp of the message in the Kafka topic. The TIMESTAMP property can be used to override `ROWTIME` with the contents of the specified column within the Kafka message (similar to timestamp extractors in Kafka's Streams API). Timestamps have a millisecond accuracy. Time-based operations, such as windowing, will process a record according to the timestamp in `ROWTIME`. |
| TIMESTAMP_FORMAT        | Used in conjunction with TIMESTAMP. If not set the timestamp column must be of type `bigint`. If it is set, then the TIMESTAMP column must be of type `varchar` and have a format that can be parsed with the java `DateTimeFormatter`. If your timestamp format has characters requiring single quotes, you can escape them with successive single quotes, `''`, for example: `'yyyy-MM-dd''T''HH:mm:ssX'`. For more information on timestamp formats, see [DateTimeFormatter](https://cnfl.io/java-dtf).                                       |
| WRAP_SINGLE_VALUE       | Controls how values are deserialized where the values schema contains only a single column. The setting controls how ksqlDB will deserialize the value of the records in the supplied `KAFKA_TOPIC` that contain only a single column.<br>If set to `true`, ksqlDB expects the column to have been serialized as named column within a record.<br>If set to `false`, ksqlDB expects the column to have been serialized as an anonymous value.<br>If not supplied, the system default, defined by [ksql.persistence.wrap.single.values](../../operate-and-deploy/installation/server-config/config-reference.md#ksqlpersistencewrapsinglevalues), then the format's default is used.<br>**Note:** `null` values have special meaning in ksqlDB. Care should be taken when dealing with single-column schemas where the value can be `null`. For more information, see [Single column (un)wrapping](../serialization.md#single-field-unwrapping).<br>**Note:** Supplying this property for formats that do not support wrapping, for example `DELIMITED`, or when the value schema has multiple columns, will result in an error. |
| WINDOW_TYPE             | By default, the topic is assumed to contain non-windowed data. If the data is windowed, i.e. was created using ksqlDB using a query that contains a `WINDOW` clause, then the `WINDOW_TYPE` property can be used to provide the window type. Valid values are `SESSION`, `HOPPING`, and `TUMBLING`. |
| WINDOW_SIZE             | By default, the topic is assumed to contain non-windowed data. If the data is windowed, i.e., was created using ksqlDB using a query that contains a `WINDOW` clause, and the `WINDOW_TYPE` property is TUMBLING or HOPPING, then the WINDOW_SIZE property should be set. The property is a string with two literals, window size (a number) and window size unit (a time unit). For example: `10 SECONDS`. |

!!! note
	  - To use Avro or Protobuf, you must have {{ site.sr }} enabled and
    `ksql.schema.registry.url` must be set in the ksqlDB server configuration
    file. See [Configure ksqlDB for Avro, Protobuf, and JSON schemas](../../operate-and-deploy/installation/server-config/avro-schema.md).
    - Avro and Protobuf field names are not case sensitive in ksqlDB. This matches the ksqlDB column name behavior.

Example
-------

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

-- table with value columns loaded from Schema Registry: 
CREATE TABLE users (
     id BIGINT PRIMARY KEY
   ) WITH (
     KAFKA_TOPIC = 'my-users-topic', 
     VALUE_FORMAT = 'JSON'
   );
```
