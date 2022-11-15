---
layout: page
title: ksqlDB Serialization Formats
tagline:  Serialize and deserialize data with ksqlDB
description: Learn how to control serialization and deserialization in ksqlDB queries
---

The term _serialization format_ refers to the manner in which an event's raw bytes
are translated to and from information structures that ksqlDB can understand
at runtime. ksqlDB offers several mechanisms for controlling serialization
and deserialization.

The primary mechanism is by choosing the serialization format when you
create a stream or table and specify `FORMAT`, `KEY_FORMAT` or `VALUE_FORMAT` in the `WITH`
clause.

```sql
-- create table with JSON value format:
CREATE TABLE ORDERS (
    F0 INT PRIMARY KEY, 
    F1 STRING
  ) WITH (
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='JSON', 
    ...
  );
```

Serialization Formats
---------------------

ksqlDB supports these serialization formats:

-   [`NONE`](#none) used to indicate the data should not be deserialized.
-   [`DELIMITED`](#delimited) supports comma separated values.
-   [`JSON`](#json) and [`JSON_SR`](#json) support JSON values, with and within schema registry integration 
-   [`AVRO`](#avro) supports AVRO serialized values. 
-   [`KAFKA`](#kafka) supports primitives serialized using the standard Kafka serializers. 
-   [`PROTOBUF`](#protobuf) supports Protocol Buffers.


Not all formats can be used as both key and value formats. See individual formats for details.

### NONE

| Feature                      | Supported |
|------------------------------|-----------|
| As value format              | No        |
| As key format                | Yes       |
| [Schema Registry required][0]| No        |
| [Schema inference][1]        | No        |
| [Single field wrapping][2]   | No        |
| [Single field unwrapping][2] | No        | 

The `NONE` format is a special marker format that is used to indicate ksqlDB should not attempt to 
deserialize that part of the  {{ site.ak }} record.

It's main use is as the `KEY_FORMAT` of key-less streams, especially where a default key format 
has been set, via [`ksql.persistence.default.format.key`][1] that supports Schema inference. If the
key format was not overridden, the server would attempt to load the key schema from the {{ site.sr }}.
If the schema existed, the key columns would be inferred from the schema, which may not be the intent.
If the schema did not exist, the statement would be rejected.  In such situations, the key format can
be set to `NONE`: 

```sql
CREATE STREAM KEYLESS_STREAM (
    VAL STRING
  ) WITH (
    KEY_FORMAT='NONE',
    VALUE_FORMAT='JSON',
    KAFKA_TOPIC='foo'
  );
```

Any statement that sets the key format to `NONE` and has key columns defined, will result in an error.

If a `CREATE TABLE AS` or `CREATE STREAM AS` statement has a source with a key format of `NONE`, but
the newly created table or stream has key columns, then you may either explicitly define the key 
format to use in the `WITH` clause, or the default key format, as set in [`ksql.persistence.default.format.key`][1]
will be used.

Conversely, a `CREATE STREAM AS` statement that removes the key columns, i.e. via `PARTITION BY null`
will automatically set the key format to `NONE`.

```sql
-- keyless stream with NONE key format:
CREATE STREAM KEYLESS_STREAM (
    VAL STRING
  ) WITH (
    KEY_FORMAT='NONE',
    VALUE_FORMAT='JSON',
    KAFKA_TOPIC='foo'
  );

-- Table created from stream with explicit key format declared in WITH clause:
CREATE TABLE T WITH (KEY_FORMAT='KAFKA') AS 
  SELECT VAL, COUNT() FROM KEYLESS_STREAM
  GROUP BY VAL;

-- or, using the default key format set in the ksql.persistence.default.format.key config:
CREATE TABLE T AS 
  SELECT VAL, COUNT() FROM KEYLESS_STREAM
  GROUP BY VAL;
```

### DELIMITED

| Feature                      | Supported |
|------------------------------|-----------|
| As value format              | Yes       |
| As key format                | Yes       |
| [Schema Registry required][0]| No        |
| [Schema inference][1]        | No        |
| [Single field wrapping][2]   | No        |
| [Single field unwrapping][2] | Yes       | 

The `DELIMITED` format supports comma-separated values. You can use other
delimiter characters by specifying the VALUE_DELIMITER when you use
VALUE_FORMAT='DELIMITED' in a WITH clause. Only a single character is valid
as a delimiter. The default is the comma character. For space- and
tab-delimited values, use the special values `SPACE` or `TAB`, not an actual
space or tab character.

The serialized object should be a Kafka-serialized string, which will be
split into columns.

For example, given a SQL statement such as:

```sql
CREATE STREAM x (ID BIGINT, NAME STRING, AGE INT) WITH (VALUE_FORMAT='DELIMITED', ...);
```

ksqlDB splits a value of `120, bob, 49` into the three fields with `ID` of
`120`, `NAME` of `bob` and `AGE` of `49`.

This data format supports all SQL
[data types](syntax-reference.md#data-types) except `ARRAY`, `MAP` and
`STRUCT`.

### JSON

| Feature                      | Supported |
|------------------------------|-----------|
| As value format              | Yes       |
| As key format                | `JSON`: Yes, `JSON_SR`: No |
| [Schema Registry required][0]| `JSON`: No, `JSON_SR`: Yes |
| [Schema inference][1]        | `JSON`: No, `JSON_SR`: Yes|
| [Single field unwrapping][2] | Yes       |

There are two JSON formats, `JSON` and `JSON_SR`. Both support serializing and
deserializing JSON data. The latter offers integration with the {{ site.sr }},
registering and retrieving JSON schemas while the former does not. These two
formats are _not_ byte compatible (you cannot read data produced by one by the
other).

The JSON formats supports all SQL [data types](syntax-reference.md#data-types).
By itself, JSON doesn't support a map type, so ksqlDB serializes `MAP` types as
JSON objects. For this reason, the JSON format  supports only `MAP` objects
that have `STRING` keys.

The serialized object should be a {{ site.ak }}-serialized string that contains
a valid JSON value. The format supports JSON objects and top-level primitives,
arrays, and maps.

!!! important
    If you want the sources that you create to store their schemas in
    {{ site.sr }}, specify the `JSON_SR` format.

#### JSON Objects

Values that are JSON objects are probably the most common.

For example, given a SQL statement such as:

```sql
CREATE STREAM x (ID BIGINT, NAME STRING, AGE INT) WITH (VALUE_FORMAT='JSON', ...);
```

And a JSON value of:

```json
{
  "id": 120,
  "name": "bob",
  "age": "49"
}
```

ksqlDB deserializes the JSON object's fields into the corresponding
fields of the stream.

#### Top-level primitives, arrays and maps

The JSON format supports reading and writing top-level primitives,
arrays and maps.

For example, given a SQL statement with only a single field in the
value schema and the `WRAP_SINGLE_VALUE` property set to `false`:

```sql
CREATE STREAM x (ID BIGINT) WITH (VALUE_FORMAT='JSON', WRAP_SINGLE_VALUE=false, ...);
```

And a JSON value of:

```json
10
```

ksqlDB can deserialize the values into the `ID` field of the stream.

When serializing data with a single field, ksqlDB can serialize the field
as an anonymous value if the `WRAP_SINGLE_VALUE` is set to `false`, for
example:

```sql
CREATE STREAM y WITH (WRAP_SINGLE_VALUE=false) AS SELECT id FROM x EMIT CHANGES;
```

For more information, see [Single field (un)wrapping](#single-field-unwrapping).

#### Decimal Serialization

ksqlDB accepts decimals that are serialized either as numbers or the text
representation of the base 10 equivalent. For example, ksqlDB can read JSON data
from both formats below:

```json
{
  "numericDecimal": 1.12345678912345,
  "stringDecimal": "1.12345678912345"
}
```

Decimals with specified precision and scale are serialized as JSON numbers. For example:

```json
{
  "value": 1.12345678912345
}
```

#### Field Name Case Sensitivity

The format is case-insensitive when matching a SQL field name with a
JSON document's property name. The first case-insensitive match is
used.

### Avro

| Feature                      | Supported |
|------------------------------|-----------|
| As value format              | Yes       |
| As key format                | No        |
| [Schema Registry required][0]| Yes       |
| [Schema inference][1]        | Yes       |
| [Single field wrapping][2]   | Yes       |
| [Single field unwrapping][2] | Yes       |

The `AVRO` format supports Avro binary serialization of all SQL
[data types](syntax-reference.md#data-types), including records and
top-level primitives, arrays, and maps.

The format requires ksqlDB to be configured to store and retrieve the Avro
schemas from the {{ site.srlong }}. For more information, see
[Configure ksqlDB for Avro, Protobuf, and JSON schemas](../operate-and-deploy/installation/server-config/avro-schema.md).

#### Avro Records

Avro records can be deserialized into matching ksqlDB schemas.

For example, given a SQL statement such as:

```sql
CREATE STREAM x (ID BIGINT, NAME STRING, AGE INT) WITH (VALUE_FORMAT='AVRO', ...);
```

And an Avro record serialized with the schema:

```json
{
  "type": "record",
  "namespace": "com.acme",
  "name": "UserDetails",
  "fields": [
    { "name": "id", "type": "long" },
    { "name": "name", "type": "string" }
    { "name": "age", "type": "int" }
  ]
}
```

ksqlDB deserializes the Avro record's fields into the corresponding
fields of the stream.

!!! important
      By default, ksqlDB-registered schemas have the same name
      (`KsqlDataSourceSchema`) and the same namespace
      (`io.confluent.ksql.avro_schemas`). You can override this behavior by
      providing a `VALUE_AVRO_SCHEMA_FULL_NAME` property in the `WITH` clause,
      where you set the `VALUE_FORMAT` to `'AVRO'`. As the name suggests, this
      property overrides the default name/namespace with the provided one.
      For example, `com.mycompany.MySchema` registers a schema with the
      `MySchema` name and the `com.mycompany` namespace.

#### Top-level primitives, arrays and maps

The Avro format supports reading and writing top-level primitives,
arrays and maps.

For example, given a SQL statement with only a single field in the
value schema and the `WRAP_SINGLE_VALUE` property set to `false`:

```sql
CREATE STREAM x (ID BIGINT) WITH (VALUE_FORMAT='AVRO', WRAP_SINGLE_VALUE=false, ...);
```

And an Avro value serialized with the schema:

```json
{
  "type": "long"
}
```

ksqlDB can deserialize the values into the `ID` field of the stream.

When serializing data with a single field, ksqlDB can serialize the field
as an anonymous value if the `WRAP_SINGLE_VALUE` is set to `false`, for
example:

```sql
CREATE STREAM y WITH (WRAP_SINGLE_VALUE=false) AS SELECT id FROM x EMIT CHANGES;
```

For more information, see [Single field (un)wrapping](#single-field-unwrapping).

#### Field Name Case Sensitivity

The format is case-insensitive when matching a SQL field name with an
Avro record's field name. The first case-insensitive match is used.

### KAFKA

| Feature                      | Supported |
|------------------------------|-----------|
| As value format              | Yes       |
| As key format                | Yes       |
| [Schema Registry required][0]| No        |
| [Schema inference][1]        | No        |
| [Single field wrapping][2]   | No        |
| [Single field unwrapping][2] | Yes       |

The `KAFKA` format supports `INT`, `BIGINT`, `DOUBLE` and `STRING`
primitives that have been serialized using Kafka's standard set of
serializers.

The format is designed primarily to support primitive message keys. It
can be used as a value format, though certain operations aren't
supported when this is the case.

Unlike some other formats, the `KAFKA` format does not perform any type
coercion, so it's important to correctly match the field type to the
underlying serialized form to avoid deserialization errors.

The table below details the SQL types the format supports, including
details of the associated Kafka Java Serializer, Deserializer and
Connect Converter classes you would need to use to write the key to
Kafka, read the key from Kafka, or use to configure Apache Connect to
work with the `KAFKA` format, respectively.

| SQL Field Type  | Kafka Type                     | Kafka Serializer                                          | Kafka Deserializer                                          | Connect Converter                                   |
|------------------|--------------------------------|-----------------------------------------------------------|-------------------------------------------------------------|-----------------------------------------------------|
| INT / INTEGER    | A 32-bit signed integer        | `org.apache.kafka.common.serialization.IntegerSerializer` | `org.apache.kafka.common.serialization.IntegerDeserializer` | `org.apache.kafka.connect.storage.IntegerConverter` |
| BIGINT           | A 64-bit signed integer        | `org.apache.kafka.common.serialization.LongSerializer`    | `org.apache.kafka.common.serialization.LongDeserializer`    | `org.apache.kafka.connect.storage.LongConverter`    |
| DOUBLE           | A 64-bit floating point number | `org.apache.kafka.common.serialization.DoubleSerializer`  | `org.apache.kafka.common.serialization.DoubleDeserializer`  | `org.apache.kafka.connect.storage.DoubleConverter`  |
| STRING / VARCHAR | A UTF-8 encoded text string    | `org.apache.kafka.common.serialization.StringSerializer`  | `org.apache.kafka.common.serialization.StringDeserializer`  | `org.apache.kafka.connect.storage.StringConverter`  |


Because the format supports only primitive types, you can only use it
when the schema contains a single field.

For example, if your {{ site.ak }} messages have a `long` key, you can make
them available to ksqlDB by using a statement like:

```sql
CREATE STREAM USERS (ID BIGINT KEY, NAME STRING) WITH (VALUE_FORMAT='JSON', ...);
```

If you integrate ksqlDB with
[Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html),
and your ksqlDB application uses a compatible value format (Avro, JSON_SR, or
Protobuf), you can just supply the key column, and ksqlDB loads the value
columns from {{ site.sr }}:

```sql
CREATE STREAM USERS (ID BIGINT KEY) WITH (VALUE_FORMAT='JSON_SR', ...);
```

The key column must be supplied, because ksqlDB supports only keys in `KAFKA`
format.

### Protobuf

| Feature                      | Supported |
|------------------------------|-----------|
| As value format              | Yes       |
| As key format                | No        |
| [Schema Registry required][0]| Yes       |
| [Schema inference][1]        | Yes       |
| [Single field wrapping][2]   | Yes       |
| [Single field unwrapping][2] | No        |

Protobuf handles `null` values differently than AVRO and JSON. Protobuf doesn't
have the concept of a `null` value, so the conversion between PROTOBUF and Java
({{ site.kconnectlong }}) objects is undefined. Usually, Protobuf resolves a
"missing field" to the default value of its type.

- **String:** the default value is the empty string.
- **Byte:** the default value is empty bytes.
- **Bool:** the default value is `false`.
- **Numeric type:** the default value is zero.
- **Enum:** the default value is the first defined enum value, which must be zero.
- **Message field:** the field is not set. Its exact value is language-dependent.
  See the generated code guide for details.

Single field (un)wrapping
-------------------------

### (de)serialization of single keys

At this time, ksqlDB supports only a single key column and that key columns
must be unwrapped, i.e. not contained within a outer record or object. See
the next two sections on single values for more information about wrapped
and unwrapped data.

### Controlling deserializing of single values

When ksqlDB deserializes a Kafka message into a row, the key is
deserialized into the key field, and the message's value is
deserialized into the value fields.

By default, ksqlDB expects any value with a single-field schema to have
been serialized as a named field within a record. However, this is not
always the case. ksqlDB also supports reading data that has been
serialized as an anonymous value.

For example, a value with multiple fields might look like the following
in JSON:

```json
{
   "id": 134,
   "name": "John"
}
```

If the value only had the `id` field, ksqlDB would still expect the value
to be serialized as a named field, for example:

```json
{
   "id": 134
}
```

If your data contains only a single field, and that field is not wrapped
within a JSON object, or an Avro record is using the `AVRO` format, then
you can use the `WRAP_SINGLE_VALUE` property in the `WITH` clause of
your [CREATE TABLE](ksqldb-reference/create-table.md) or
[CREATE STREAM](ksqldb-reference/create-stream.md) statements. Setting the
property to `false` tells ksqlDB that the value isn't wrapped, so the
example above would be a JSON number:

```json
134
```

For example, the following creates a table where the values in the
underlying topic have been serialized as an anonymous JSON number:

```sql
CREATE TABLE TRADES (
    ID INT PRIMARY KEY, 
    VALUE INT
  ) WITH (
    WRAP_SINGLE_VALUE=false, 
    ...
  );
```

If a statement doesn't set the value wrapping explicitly, ksqlDB uses the
system default, which is defined by `ksql.persistence.wrap.single.values`.
You can change the system default, if the format supports it. For more information, see
[ksql.persistence.wrap.single.values](../operate-and-deploy/installation/server-config/config-reference.md#ksqlpersistencewrapsinglevalues).

!!! important
      ksqlDB treats `null` keys and values as a special case. We recommend
      avoiding unwrapped single-field schemas if the field can have a `null`
      value.

A `null` value in a table's topic is treated as a tombstone, which
indicates that a row has been removed. If a table's source topic has an
unwrapped single-field key schema and the value is `null`, it's treated
as a tombstone, resulting in any previous value for the key being
removed from the table.

A `null` key or value in a stream's topic is ignored when the stream is
part of a join. A `null` value in a table's topic is treated as a
tombstone, and a `null` key is ignored when the table is part of a join.

When you have an unwrapped single-field schema, ensure that any `null`
key or value has the desired result.

### Controlling serialization of single values

When ksqlDB serializes a row into a Kafka message, the key field is
serialized into the message's key, and any value fields are serialized
into the message's value.

By default, if the value has only a single field, ksqlDB serializes the
single field as a named field within a record. However, this doesn't
always match the requirements of downstream consumers, so ksqlDB allows
the value to be serialized as an anonymous value.

For example, consider the statements:

```sql
CREATE STREAM x (f0 INT, f1 STRING) WITH (VALUE_FORMAT='JSON', ...);
CREATE STREAM y AS SELECT f0 FROM x EMIT CHANGES;
```

The second statement defines a stream with only a single field in the
value, named `f0`.

By default, when ksqlDB writes out the result to Kafka, it persists the
single field as a named field within a JSON object, or an Avro record if
using the `AVRO` format:

```json
{
   "F0": 10
}
```

If you require the value to be serialized as an anonymous value, for
example:

```json
10
```

Then you can use the `WRAP_SINGLE_VALUE` property in your statement.
For example,

```sql
CREATE STREAM y WITH(WRAP_SINGLE_VALUE=false) AS SELECT f0 FROM x EMIT CHANGES;
```

If a statement doesn't set the value wrapping explicitly, ksqlDB uses the
system default, defined by `ksql.persistence.wrap.single.values`, if the format supports it. 
You can change the system default. For more information, see
[ksql.persistence.wrap.single.values](../operate-and-deploy/installation/server-config/config-reference.md#ksqlpersistencewrapsinglevalues).

!!! important
      ksqlDB treats `null` keys and values as a special case. We recommended
      avoiding unwrapped single-field schemas if the field can have a `null`
      value.

A `null` value in a table's topic is treated as a tombstone, which
indicates that a row has been removed. If a table's source topic has an
unwrapped single-field key schema and the value is `null`, it's treated
as a tombstone, resulting in any previous value for the key being
removed from the table.

A `null` key or value in a stream's topic is ignored when the stream is
part of a join. A `null` value in a table's topic is treated as a
tombstone, and a `null` key is ignored when the table is part of a join.

When you have an unwrapped single-field schema, ensure that any `null`
key or value has the desired result.

### Single-field serialization examples

```sql
-- Assuming system configuration is at the default:
--  ksql.persistence.wrap.single.values=true

-- creates a stream, picking up the system default of wrapping values.
-- the serialized value is expected to be wrapped.
-- if the serialized forms do not match the expected wrapping it will result in a deserialization error.
CREATE STREAM IMPLICIT_SOURCE (NAME STRING) WITH (...);

-- override 'ksql.persistence.wrap.single.values' to false
-- the serialized value is expected to not be unwrapped.
CREATE STREAM EXPLICIT_SOURCE (ID INT) WITH (WRAP_SINGLE_VALUE=false, ...);

-- results in an error as the value schema is multi-field
CREATE STREAM BAD_SOURCE (ID INT, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, ...);

-- creates a stream, picking up the system default of wrapping values.
-- the serialized values in the sink topic will be wrapped.
CREATE STREAM IMPLICIT_SINK AS SELECT ID FROM S EMIT CHANGES;

-- override 'ksql.persistence.wrap.single.values' to false
-- the serialized values will not be wrapped.
CREATE STREAM EXPLICIT_SINK WITH(WRAP_SINGLE_VALUE=false) AS SELECT ID FROM S EMIT CHANGES;

-- results in an error as the value schema is multi-field
CREATE STREAM BAD_SINK WITH(WRAP_SINGLE_VALUE=true) AS SELECT ID, COST FROM S EMIT CHANGES;
```

[0]: ../operate-and-deploy/installation/server-config/avro-schema.md
[1]: ../concepts/schemas.md#schema-inference 
[2]: #single-field-unwrapping

## Suggested Reading

- Blog post: [I’ve Got the Key, I’ve Got the Secret. Here’s How Keys Work in ksqlDB 0.10](https://www.confluent.io/blog/ksqldb-0-10-updates-key-columns/)

[1]: ../operate-and-deploy/installation/server-config/config-reference.md#ksqlpersistencedefaultformatkey