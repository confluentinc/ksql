---
layout: page
title: Schemas in ksqlDB
tagline: Defining the structure of your data
description: Learn how schemas work with ksqlDB
keywords: ksqldb, schema, evolution, avro, protobuf, json, csv
---

Data sources like streams and tables have an associated schema. This schema defines the columns
available in the data, just like a the columns in a traditional SQL database table.

## Key vs Value columns

ksqlDB supports both key and value columns. These map to the data held in the
key and value of the underlying {{ site.ak }} topic message.

A column is defined by a combination of its [name](#valid-identifiers), its [SQL data type](#sql-data-types),
and possibly a namespace.

Key columns have a `KEY` or `PRIMARY KEY` suffix for streams and tables, respectively.
ksqlDB supports a single key column only.

Value columns have no namespace suffix. There can be one or more value columns.

For example, the following example statement declares a stream with a single
key column and several value columns:

```sql
CREATE STREAM USER_UPDATES (
   ID BIGINT KEY, 
   STRING NAME, 
   ADDRESS ADDRESS_TYPE
 ) WITH (
   ...
 );
```

This statement declares a table with a primary key and value columns:

```sql
CREATE TABLE USERS (
   ID BIGINT PRIMARY KEY, 
   STRING NAME, 
   ADDRESS ADDRESS_TYPE
 ) WITH (
   ...
 );
```

Tables _require_ a primary key, but the key column of a stream is optional.
For example, the following statement defines a stream with no key column:

```sql
CREATE STREAM APP_LOG (
   LOG_LEVEL STRING,
   APP_NAME STRING,
   MESSAGE STRING
 ) WITH (
   ...
 );
```

!!! tip
    [How Keys Work in ksqlDB](https://www.confluent.io/blog/ksqldb-0-10-updates-key-columns) 
    has more details on key columns and provides guidance for when you may
    want to use streams with and without key columns.

## Schema Inference

For supported [serialization formats](../developer-guide/serialization.md),
ksqlDB can integrate with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).
ksqlDB automatically retrieves (reads) and registers (writes) schemas as needed,
which spares you from defining columns and data types manually in `CREATE`
statements and from manual interaction with {{ site.sr }}. Before using schema
inference in ksqlDB, make sure that the {{ site.sr }} is up and running and
ksqlDB is [configured to use it](../operate-and-deploy/installation/server-config/avro-schema.md).

Here's what you can do with schema inference in ksqlDB:

-   Declare streams and tables on {{ site.ak }} topics with supported value formats by using 
    `CREATE STREAM` and `CREATE TABLE` statements, without needing to declare the value columns.
-   Declare derived views with `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT` statements.
    The schema of the view is registered in {{ site.sr }} automatically.
-   Convert data to different formats with `CREATE STREAM AS SELECT` and
    `CREATE TABLE AS SELECT` statements, by declaring the required output
    format in the `WITH` clause. For example, you can convert a stream from
    Avro to JSON.

Only the schema of the message *value* can be retrieved from {{ site.sr }}. Message
*keys* must be compatible with the [`KAFKA` format](../developer-guide/serialization.md#kafka)
to be accessible within ksqlDB. ksqlDB ignores schemas that have been registered
for message keys. 

!!! note
    Message *keys* in Avro and Protobuf are not supported. If your message keys
    are in an unsupported format, see [What to do if your key is not set or is in a different format](../developer-guide/syntax-reference.md#what-to-do-if-your-key-is-not-set-or-is-in-a-different-format). 
    JSON message keys can be accessed by defining the key as a single `STRING` value, which will 
    contain the JSON document.
    
Although ksqlDB doesn't support loading the message key's schema from {{ site.sr }},
you can provide the key column definition within the `CREATE TABLE` or `CREATE STREAM`
statement, if the data records are compatible with ksqlDB. This is known as
_partial schema inference_, because the key schema is provided explicitly.

Tables require a `PRIMARY KEY`, so you must supply one explicitly in your
`CREATE TABLE` statement. `KEY` columns are optional for streams, so if you
don't supply one the stream is created without a key column.

The following example statements show how to create streams and tables that have 
Avro-formatted data. If you want to use Protobuf- or JSON-formatted data,
substitute `PROTOBUF`, `JSON` or `JSON_SR` for `AVRO` in each statement.

!!! note
    ksqlDB handles the `JSON` and `JSON_SR` formats differently. While the
    `JSON` format is capable of _reading_ the schema from {{ site.sr }},
    `JSON_SR` both reads and registers new schemas, as necessary.

### Create a new stream

#### Without a key column

The following statement shows how to create a new `pageviews` stream by
reading from a {{ site.ak }} topic that has Avro-formatted message values.

```sql
CREATE STREAM pageviews
  WITH (
    KAFKA_TOPIC='pageviews-avro-topic',
    VALUE_FORMAT='AVRO'
  );
```

In this example, you don't need to define any columns in the CREATE statement. 
ksqlDB infers this information automatically from the latest registered schema
for the `pageviews-avro-topic` topic. ksqlDB uses the most recent schema at the
time the statement is first executed.

!!! important
    The schema must be registered in {{ site.sr }} under the subject
    `pageviews-avro-topic-value`.

#### With a key column

The following statement shows how to create a new `pageviews` stream by reading
from a {{ site.ak }} topic that has Avro-formatted message values and a
`KAFKA`-formatted `INT` message key.

```sql
CREATE STREAM pageviews (
    pageId INT KEY
  ) WITH (
    KAFKA_TOPIC='pageviews-avro-topic',
    VALUE_FORMAT='AVRO'
  );
```

In the previous example, you need only supply the key column in the CREATE
statement.  ksqlDB infers the value columns automatically from the latest
registered schema for the `pageviews-avro-topic` topic. ksqlDB uses the most
recent schema at the time the statement is first executed.

!!! note
    The schema must be registered in {{ site.sr }} under the subject
    `pageviews-avro-topic-value`.

### Create a new table

The following statement shows how to create a new `users` table by reading
from a {{ site.ak }} topic that has Avro-formatted message values and a
`KAFKA`-formatted `BIGINT` message key.

```sql
CREATE TABLE users (
    userId BIGINT PRIMARY KEY
  ) WITH (
    KAFKA_TOPIC='users-avro-topic',
    VALUE_FORMAT='AVRO'
  );
```

In the previous example, you need only supply the key column in the CREATE
statement. ksqlDB infers the value columns automatically from the latest
registered schema for the `users-avro-topic` topic. ksqlDB uses the most
recent schema at the time the statement is first executed.

!!! note
    The schema must be registered in {{ site.sr }} under the subject
    `users-avro-topic-value`.

### Create a new source with selected columns

If you want to create a STREAM or TABLE that has only a subset of the available
fields in the Avro schema, you must explicitly define the columns.

The following statement shows how to create a new `pageviews_reduced`
stream, which is similar to the previous example, but with only a few of
the available fields in the Avro data. In this example, only the
`viewtime` and `url` value columns are picked.

```sql
CREATE STREAM pageviews_reduced (
    viewtime BIGINT,
    url VARCHAR
  ) WITH (
    KAFKA_TOPIC='pageviews-avro-topic',
    VALUE_FORMAT='AVRO'
  );
```

### Declaring a derived view

The following statement shows how to create a materialized view derived from an
existing source. The {{ site.ak }} topic that the view is materialized to
inherits the value format of the source, unless it's overridden explicitly in
the `WITH` clause, as shown. The value schema is registered with {{ site.sr }}
if the value format supports the integration, with the exception of the `JSON`
format, which only _reads_ from {{ site.sr }}.

```sql
CREATE TABLE pageviews_by_url 
  WITH (
    VALUE_FORMAT='AVRO'
  ) AS 
  SELECT
    url,
    COUNT(*) AS VIEW_COUNT
  FROM pageviews
  GROUP BY url;
```

!!! note
    The schema will be registered in {{ site.sr }} under the subject
    `PAGEVIEWS_BY_URL-value`.

### Converting formats

ksqlDB enables you to change the underlying value format of streams and tables. 
This means that you can easily mix and match streams and tables with different
data formats and also convert between value formats. For example, you can join
a stream backed by Avro data with a table backed by JSON data.

The example below converts a JSON-formatted topic into Avro. Only the
`VALUE_FORMAT` is required to achieve the data conversion. ksqlDB generates an
appropriate Avro schema for the new `PAGEVIEWS_AVRO` stream automatically and
registers the schema with {{ site.sr }}.

```sql
CREATE STREAM pageviews_json (
    pageid VARCHAR KEY, 
    viewtime BIGINT, 
    userid VARCHAR
  ) WITH (
    KAFKA_TOPIC='pageviews_kafka_topic_json', 
    VALUE_FORMAT='JSON'
  );

CREATE STREAM pageviews_avro
  WITH (VALUE_FORMAT = 'AVRO') AS
  SELECT * FROM pageviews_json;
```

!!! note
    The schema will be registered in {{ site.sr }} under the subject
    `PAGEVIEWS_AVRO-value`.

For more information, see
[Changing Data Serialization Format from JSON to Avro](https://www.confluent.io/stream-processing-cookbook/ksql-recipes/changing-data-serialization-format-json-avro)
in the [Stream Processing Cookbook](https://www.confluent.io/product/ksql/stream-processing-cookbook).

## Valid Identifiers

Column and field names must be valid identifiers.

Unquoted identifiers will be treated as upper-case, for example `col0` is equivalent to `COL0`, and
must contain only alpha-numeric and underscore characters.

Identifiers containing invalid character, or where case needs to be preserved, can be quoted using
back-tick quotes, for example `` `col0` ``.

## SQL data types

The following SQL types are supported by ksqlDB:

 * [Primitive types](#primitive-types)
 * [Decimal type](#decimal-type)
 * [Array type](#array-type)
 * [Map type](#map-type)
 * [Struct type](#struct-type)
 * [Custom types](#custom-types)

### Primitive types

Supported primitive types are:

  * `BOOLEAN`: a binary value
  * `INT`: 32-bit signed integer
  * `BIGINT`: 64-bit signed integer
  * `DOUBLE`: double precision (64-bit) IEEE 754 floating-point number
  * `STRING`: a unicode character sequence (UTF8)

### Decimal type

The `DECIMAL` type can store numbers with a very large number of digits and perform calculations exactly.
It is recommended for storing monetary amounts and other quantities where exactness is required.
However, arithmetic on decimals is slow compared to integer and floating point types.

`DECIMAL` types have a _precision_ and _scale_.
The scale is the number of digits in the fractional part, to the right of the decimal point.
The precision is the total number of significant digits in the whole number, that is,
the number of digits on both sides of the decimal point.
For example, the number `765.937500` has a precision of 9 and a scale of 6.

To declare a column of type `DECIMAL` use the syntax:

```sql
DECIMAL(precision, scale)
```

The precision must be positive, the scale zero or positive.

### Array type

The `ARRAY` type defines a variable-length array of elements. All elements in the array must be of
the same type.

To declare an `ARRAY` use the syntax:

```
ARRAY<element-type>
```

The _element-type_ of an another [SQL data type](#sql-data-types).

For example, the following creates an array of `STRING`s:

```sql
ARRAY<STRING>
```

Instances of an array can be created using the syntax:

```
ARRAY[value [, value]*]
```

For example, the following creates an array with three `INT` elements:

```sql
ARRAY[2, 4, 6]
```

### Map type

The `MAP` type defines a variable-length collection of key-value pairs. All keys in the map must be
of the same type. All values in the map must be of the same type.

To declare a `MAP` use the syntax:

```
MAP<key-type, element-type>
```

The _key-type_ must currently be `STRING` while the _value-type_ can an any other [SQL data type](#sql-data-types).

For example, the following creates a map with `STRING` keys and values:

```sql
MAP<STRING, STRING>
```

Instances of a map can be created using the syntax:

```
MAP(key := value [, key := value]*)
```

For example, the following creates a map with three key-value pairs:

```sql
MAP('a' := 1, 'b' := 2, 'c' := 3)
```

### Struct type

The `STRUCT` type defines a list of named fields, where each field can have any [SQL data type](#sql-data-types).

To declare a `STRUCT` use the syntax:

```
STRUCT<field-name field-type [, field-name field-type]*>
```

The _field-name_ can be any [valid identifier](#valid-identifiers). The _field-type_ can be any
valid [SQL data type](#sql-data-types).

For example, the following creates a struct with an `INT` field called `FOO` and a `BOOLEAN` field
call `BAR`:

```sql
STRUCT<FOO INT, BAR BOOLEAN>
```

Instances of a struct can be created using the syntax:

```
STRUCT(field-name := field-value [, field-name := field-value]*)
```

For example, the following creates a struct with fields called `FOO` and `BAR` and sets their values
to `10` and `true`, respectively:

```sql
STRUCT('FOO' := 10, 'BAR' := true)
```

### Custom types

KsqlDB supports custom types using the `CREATE TYPE` statements.
See the [`CREATE TYPE` docs](../developer-guide/ksqldb-reference/create-type) for more information.