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
CREATE TABLE table_name ( { column_name data_type } [, ...] )
  WITH ( property_name = expression [, ...] );
```

Description
-----------

Create a new table with the specified columns and properties. Columns
can be any of the [data types](../syntax-reference.md#ksql-data-types) supported by
KSQL.

KSQL adds the implicit columns `ROWTIME` and `ROWKEY` to every stream
and table, which represent the corresponding Kafka message timestamp and
message key, respectively. The timestamp has milliseconds accuracy.

When creating a table from a Kafka topic, KSQL requries the message key
to be a `VARCHAR` aka `STRING`. If the message key is not of this type
follow the instructions in [Key Requirements](key-requirements.md).

The WITH clause supports the following properties:

|        Property         |                                            Description                                            |
| ----------------------- | ------------------------------------------------------------------------------------------------- |
| KAFKA_TOPIC (required)  | The name of the Kafka topic that backs this source. The topic must either already exist in Kafka, or PARTITIONS must be specified to create the topic. Command will fail if the topic exists with different partition/replica counts. |
| VALUE_FORMAT (required) | Specifies the serialization format of message values in the topic. Supported formats: `JSON`, `DELIMITED` (comma-separated value), `AVRO` and `KAFKA`. For more information, see [Serialization Formats](../serialization.md#serialization-formats). |
| PARTITIONS              | The number of partitions in the backing topic. This property must be set if creating a TABLE without an existing topic (the command will fail if the topic does not exist). |
| REPLICAS                | The number of replicas in the backing topic. If this property is not set but PARTITIONS is set, then the default Kafka cluster configuration for replicas will be used for creating a new topic. |
| VALUE_DELIMITER         | Used when VALUE_FORMAT='DELIMITED'. Supports single character to be a delimiter, defaults to ','. For space and tab delimited values you must use the special values 'SPACE' or 'TAB', not an actual space or tab character. |
| KEY                     | **Optimization hint:** If the Kafka message key is also present as a field/column in the Kafka message value, you may set this property to associate the corresponding field/column with the implicit `ROWKEY` column (message key). If set, KSQL uses it as an optimization hint to determine if repartitioning can be avoided when performing aggregations and joins. You can only use this if the key format in kafka is `VARCHAR` or `STRING`. Do not use this hint if the message key format in kafka is `AVRO` or `JSON`. For more information, see [Key Requirements](key-requirements.md).  |
| TIMESTAMP               | By default, the implicit `ROWTIME` column is the timestamp of the message in the Kafka topic. The TIMESTAMP property can be used to override `ROWTIME` with the contents of the specified field/column within the Kafka message value (similar to timestamp extractors in the Kafka Streams API). Timestamps have a millisecond accuracy. Time-based operations, such as windowing, will process a record according to the timestamp in `ROWTIME`.  |
| TIMESTAMP_FORMAT        | Used in conjunction with TIMESTAMP. If not set will assume that the timestamp field is a `bigint`. If it is set, then the TIMESTAMP field must be of type varchar and have a format that can be parsed with the Java `DateTimeFormatter`. If your timestamp format has characters requiring single quotes, you can escape them with two successive single quotes, `''`, for example: `'yyyy-MM-dd''T''HH:mm:ssX'`. For more information on timestamp formats, see [DateTimeFormatter](https://cnfl.io/java-dtf). |
| WRAP_SINGLE_VALUE       | Controls how values are deserialized where the values schema contains only a single field. The setting controls how KSQL will deserialize the value of the records in the supplied `KAFKA_TOPIC` that contain only a single field.<br>If set to `true`, KSQL expects the field to have been serialized as named field within a record.<br>If set to `false`, KSQL expects the field to have been serialized as an anonymous value.<br>If not supplied, the system default, defined by [ksql.persistence.wrap.single.values](../../installation/server-config/config-reference.md#ksqlpersistencewrapsinglevalues) and defaulting to `true`, is used.<br>**Note:** `null` values have special meaning in KSQL. Care should be taken when dealing with single-field schemas where the value can be `null`. For more information, see [Single field (un)wrapping](../serialization.md#single-field-unwrapping).<br>**Note:** Supplying this property for formats that do not support wrapping, for example `DELIMITED`, or when the value schema has multiple fields, will result in an error. |
| WINDOW_TYPE             | By default, the topic is assumed to contain non-windowed data. If the data is windowed, i.e. was created using KSQL using a query that contains a `WINDOW` clause, then the `WINDOW_TYPE` property can be used to provide the window type. Valid values are `SESSION`, `HOPPING`, and `TUMBLING`. |
| WINDOW_SIZE             | By default, the topic is assumed to contain non-windowed data. If the data is windowed, i.e., was created using KSQL using a query that contains a `WINDOW` clause, and the `WINDOW_TYPE` property is TUMBLING or HOPPING, then the WINDOW_SIZE property should be set. The property is a string with two literals, window size (a number) and window size unit (a time unit). For example: `10 SECONDS`. |

!!! note
	  - To use Avro, you must have {{ site.sr }} enabled and
    `ksql.schema.registry.url` must be set in the KSQL server configuration
    file. See [Configure Avro and {{ site.sr }} for KSQL](../../installation/server-config/avro-schema.md).
    - Avro field names are not case sensitive in KSQL. This matches the KSQL column name behavior.

Example
-------

```sql
CREATE TABLE users (usertimestamp BIGINT, user_id VARCHAR, gender VARCHAR, region_id VARCHAR) WITH (
    KAFKA_TOPIC = 'my-users-topic',
    KEY = 'user_id');
```

Page last revised on: {{ git_revision_date }}
