---
layout: page
title: Schema Inference With ID
tagline: ksqlDB Schema Inference With ID
description: ksqlDB integrates with Confluent Schema Registry to read and write schemas as needed 
keywords: serialization, schema, schema registry, json, avro, delimited, KEY_SCHEMA_ID, VALUE_SCHEMA_ID
---
## Schema Inference With ID

For supported [serialization formats](/reference/serialization), ksqlDB can use 
[Schema Inference](/operate-and-deploy/schema-registry-integration#schema-inference) to retrieve 
(read) and register (write) schemas as needed. If you specify a `KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID`
explicitly in the `CREATE` statements, ksqlDB retrieves and registers the schema specified by the 
ID from {{ site.sr }}, and it also serializes data using exactly the same schema referred to
by the ID. This can spare you from defining columns and data types manually and also make sure the data 
are serialized by the specified physical schema, which can be consumed in downstream systems. Before using 
schema inference with explicit IDs in ksqlDB, make sure that the {{ site.sr }} is up and running and 
ksqlDB is [configured to use it](../operate-and-deploy/installation/server-config/avro-schema.md).

Here's what you can do with schema inference with IDs in ksqlDB:

- Declare streams and tables on {{ site.ak }} topics with supported key and value formats by using 
  `CREATE STREAM` and `CREATE TABLE` statements with `KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID` properties, 
  without the need to declare the key and/or value columns.
- Declare derived views with `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT` statements with
  `KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID` properties. The schema of the view is registered in {{ site.sr }} 
  automatically.
- Serialize output data using the schema referred to by `KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID`, instead
  of the logical data source schema stored in ksqlDB.
    
If you're declaring a stream or table with a key format that's different from its
value format, and only one of the two formats supports schema inference,
you can explicitly provide the columns for the format that does not support schema inference
while still having ksqlDB load columns for the format that does support schema inference
from {{ site.sr }}. This is known as _partial schema inference_. To infer value columns
for a keyless stream, set the key format to the [`NONE` format](/reference/serialization#none).

Tables require a `PRIMARY KEY`, so you must supply one explicitly in your
`CREATE TABLE` statement. `KEY` columns are optional for streams, so if you
don't supply one, the stream is created without a key column.

The following example statements show how to create streams and tables that have 
Avro-formatted data. If you want to use Protobuf or JSON-formatted data,
substitute `PROTOBUF` or `JSON_SR` for `AVRO` in each statement.

### Create a new stream or table

When `KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID` is used in statements to create stream or table, the schema
fetched from {{ site.sr }} will be used to infer data source's columns and serialize output data. See 
[Schema Inference and Data Serialization](#Schema-Inference-and-Data-Serialization) for details about 
how columns are inferred and data are serialized.

#### Without a key column

The following statement shows how to create a new `pageviews` stream by reading from a {{ site.ak }} 
topic that has Avro-formatted message values.

```sql
CREATE STREAM pageviews
  WITH (
    KAFKA_TOPIC='pageviews-avro-topic',
    VALUE_FORMAT='AVRO',
    VALUE_SCHEMA_ID=1
  );
```

In this example, you don't need to define any columns in the CREATE statement. ksqlDB infers this 
information automatically from {{ site.sr }} using the provided `VALUE_SCHEMA_ID`.

!!! important
    The schema referred to by `VALUE_SCHEMA_ID` must be registered in
    {{ site.sr }}. It can be under any subject but must match the format
    defined by `VALUE_FORMAT`.

#### With a key column

The following statement shows how to create a new `pageviews` stream by reading
from a {{ site.ak }} topic that has Avro-formatted key and message values.

```sql
CREATE STREAM pageviews WITH (
    KAFKA_TOPIC='pageviews-avro-topic',
    KEY_FORMAT='AVRO',
    VALUE_FORMAT='AVRO',
    KEY_SCHEMA_ID=1,
    VALUE_SCHEMA_ID=2
  );
```

In the previous example, ksqlDB infers the key and value columns automatically from {{ site.sr }} using
the provided `KEY_SCHEMA_ID` and `VALUE_SCHEMA_ID`.

!!! important
    The schema referred to by `KEY_SCHEMA_ID` and `VALUE_SCHEMA_ID` must be registered in {{ site.sr }}. 
    It can be under any subject but must match the format defined by `KEY_FORMAT` and `VALUE_FORMAT` respectively.

!!! important
    You can't define key or value columns in a statement if a corresponding
    `KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID` is supplied.

#### With partial schema inference

The following statement shows how to create a new `pageviews` table by reading
from a {{ site.ak }} topic that has Avro-formatted message values and a
`KAFKA`-formatted `INT` primary key.

```sql
CREATE TABLE pageviews (
    pageId INT PRIMARY KEY
  ) WITH (
    KAFKA_TOPIC='pageviews-avro-topic',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='AVRO',
    VALUE_SCHEMA_ID=1
  );
```

In the previous example, only the key column is supplied in the CREATE
statement. ksqlDB infers the value columns automatically from {{ site.sr }} using the provided 
`VALUE_SCHEMA_ID`.

!!! important
    The schema referred to by `VALUE_SCHEMA_ID` must be registered in
    { site.sr }}. It can be under any subject but must match the format
    defined by `VALUE_FORMAT`.

### Declaring a derived view with schema ID.

The following statement shows how to create a materialized view derived from an existing source with the 
`VALUE_SCHEMA_ID` property. The schema referred to by `VALUE_SCHEMA_ID` is used to check column compatibility with
output columns and serialize output data. For more information, see [Schema Inference and Data Serialization](#Schema-Inference-and-Data-Serialization).

```sql
CREATE STREAM pageviews_new
  WITH (
    VALUE_FORMAT='AVRO',
    VALUE_SCHEMA_ID=1
  ) AS 
  SELECT
    pageId,
    ts
  FROM pageviews
```

!!! important
    The schema referred to by `VALUE_SCHEMA_ID` must be registered in {{ site.sr }}. It can be under 
    any subject but must match the format defined by `VALUE_FORMAT`. The schema referred to by `VALUE_SCHEMA_ID`
    must be compatible with the logical schema defined by the `SELECT` clause. For more information, see [Schema Inference and Data Serialization](#Schema-Inference-and-Data-Serialization).

### Schema Inference and Data Serialization

This sections talks about the details in schema inference from schema in {{ site.sr }} to ksqlDB schema
and how output data are serialized when schema ID is used. We refer the schema in {{ site.sr }} as 
physical schema and the schema in ksqlDB as logical schema. Same physical schema instead of logical schema
will be registered under the subject `<topic-name>-key` or `<topic-name>-value` if corresponding
`KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID` is provided.

#### Schema Inference Schema Requirements

If `WRAP_SINGLE_VALUE` is set to `true` in the statement, the physical schema is expected to be a `struct`
type, and the field names are used as data source column names. Field types are inferred from
corresponding column data types.



!!! note
    - In `AVRO`, the `struct` type corresponds with the `record` type.
    - In `PROTOBUF` the `struct` type corresponds with the `message` type.
    - In `JSON_SR`, the `struct` type corresponds with the `object` type.

For example, a physical schema is an `AVRO` schema and defined as following in {{ site.sr }} with ID 1:

```json
{
"schema":"{
    \"type\":\"record\",
    \"name\":\"PageViewValueSchema\",
    \"namespace\":\"io.confluent.ksql.avro_schemas\",
    \"fields\":[
        {\"name\":\"page_name\",\"type\":\"string\",\"default\":\"abc\"},
        {\"name\":\"ts\",\"type\":\"int\",\"default\":123}
    ]
}"
}
```

!!! note
    `AVRO` schema raw string in {{ site.sr }} should be single line raw string without `\n`. Above format
    is for ease of read.

`CREATE` statement is defined as following:

```sql
CREATE STREAM pageviews (
    pageId INT KEY
  ) WITH (
    KAFKA_TOPIC='pageviews-avro-topic',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='AVRO',
    VALUE_SCHEMA_ID=1,
    PARTITIONS=1
  );
```

The schema for `pageviews` will be inferred as:

```
ksql> describe pageviews;

Name                 : PAGEVIEWS
 Field     | Type
------------------------------------
 PAGEID    | INTEGER          (key)
 page_name | VARCHAR(STRING)
 ts        | INTEGER
------------------------------------
```

If `WRAP_SINGLE_VALUE` is `false` in the statement, and if `KEY_SCHEMA_ID` is set, `ROWKEY` is used
as key column name.

If `VALUE_SCHEMA_ID` is set, `ROWVAL` is used as value column name. The physical
schema is used as the column data type.

For example, if the physical schema is `AVRO` and defined as following in {{ site.sr }} with ID 2:

```
{"schema": "\"int\""}
```

and a `CREATE` statement is defined as following:

```sql
CREATE TABLE pageview_count (
    pageId INT PRIMARY KEY
  ) WITH (
    KAFKA_TOPIC='pageview-count',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='AVRO',
    VALUE_SCHEMA_ID=2,
    WRAP_SINGLE_VALUE=false,
    PARTITIONS=1
  );
```

The inferred schema for `pageview_count` is:

```
ksql> describe pageview_count;

Name                 : PAGEVIEW_COUNT
 Field  | Type
-----------------------------------------
 PAGEID | INTEGER          (primary key)
 ROWVAL | INTEGER
-----------------------------------------
```

For more information about `WRAP_SINGLE_VALUE`, see 
[Single Field (un)wrapping](/reference/serialization#single-field-unwrapping).

#### Schema Inference Type Handling

ksqlDB supports the `null` value for `KEY` and `VALUE` columns. This means that
if a field in the physical schema is a required type, it's translated to an
optional type in the logical schema. This has subtle implications for data
serialization, which are explained in the following section.

!!! important
    For unsupported types in physical schema, ksqlDB ignores them and continues
    translating supported types to the logical schema. You should verify that
    the logical schema is translated as expected.

!!! important
    During schema translation from a physical schema to a logical schema, `struct` type field names are
    used as column names in the logical schema. Field names aren't uppercased, in contrast with schema inference
    _without_ a schema id, which does translate field names to upper case.

#### Schema Compatibility Check for Derived View

You can use schema IDs when creating a materialized views, but instead of inferring the logical 
schema for the view, the schema is used to check compatibility against the query's projection and serialized output
data. For compatibility checks, the inferred logical schema must be a superset of the query's projection
schema, which means corresponding column names, types, and order must match. The inferred logical schema 
may have extra columns.

Consider the following statement:

```sql
CREATE STREAM pageviews_new
  WITH (
    VALUE_FORMAT='AVRO',
    VALUE_SCHEMA_ID=1
  ) AS 
  SELECT
    pageId,
    ts
  FROM pageviews
```

If the `pageviews` value column has the type `ts INT`, the logical schema of `pageviews_new` is decided by the 
projection in the query `SELECT pageId, ts FROM pageviews`. When `VALUE_SCHEMA_ID` is used, 
the inferred logical schema is checked against `ts INT` for compatibility. 

A compatible example of physical schema would be as follows:

```json
{
"schema":"{
    \"type\":\"record\",
    \"name\":\"PageViewNewSchema\",
    \"namespace\":\"io.confluent.ksql.avro_schemas\",
    \"fields\":[
        {\"name\":\"ts\",\"type\":\"int\",\"default\":123},
        {\"name\":\"title\",\"type\":\"string\",\"default\":\"title\"}
    ]
}"
}
```

!!! note
    `AVRO` schema raw string in {{ site.sr }} should be single line raw string without `\n`. Above format
    is for ease of read.

!!! note
    In the above `AVRO` schema, `title` is an extra field. Because the physical schema is used for data serialization,
    the `title` field with a default value will appear in serialized data, even though the inserted value can never 
    set the `title` field, because it's not in logical schema (the `SELECT` clause of the query).

An incompatible physical schema is show below. It's incompatible because of the type mismatch for `pageId`:

```json
{
"schema":"{
    \"type\":\"record\",
    \"name\":\"PageViewNewSchema\",
    \"namespace\":\"io.confluent.ksql.avro_schemas\",
    \"fields\":[
        {\"name\":\"pageId\",\"type\":\"string\",\"default\":\"id\"},
        {\"name\":\"ts\",\"type\":\"int\",\"default\":123}
    ]
}"
}
```

!!! note
    `AVRO` schema raw string in {{ site.sr }} should be single line raw string without `\n`. Above format
    is for ease of read.

#### Data Serialization

When a schema ID is provided, and schema inference is successful, the data source can be created.
When writing to the data source, the physical schema inferred by the schema ID is used to serialize
data, instead of the logical schema being used in other cases. Because ksqlDB's logical schema accepts
`null` values but the physical schema may not, serialization can fail even if the inserted value is valid 
for the logical schema.

For example, a physical schema is `AVRO` schema and defined as following in {{ site.sr }} with ID 1:

```json
{
"schema":"{
    \"type\":\"record\",
    \"name\":\"PageViewValueSchema\",
    \"namespace\":\"io.confluent.ksql.avro_schemas\",
    \"fields\":[
        {\"name\":\"page_name\",\"type\":\"string\"},
        {\"name\":\"ts\",\"type\":\"int\"}
    ]
}"
}
```

and `CREATE` statement is defined as following:

```sql
CREATE STREAM pageviews (
    pageId INT KEY
  ) WITH (
    KAFKA_TOPIC='pageviews-avro-topic',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='AVRO',
    VALUE_SCHEMA_ID=1,
    PARTITIONS=1
  );
```

then the schema for `pageviews` is inferred as:

```
ksql> describe pageviews;

Name                 : PAGEVIEWS
 Field     | Type
------------------------------------
 PAGEID    | INTEGER          (key)
 page_name | VARCHAR(STRING)
 ts        | INTEGER
------------------------------------
```

When you insert values to `pageviews` with a `null` value, ksqlDB returns an error:

```
ksql> insert into pageviews values (1, null, null);
Failed to insert values into 'PAGEVIEWS'. Could not serialize value: [ null | null ]. Error serializing message to topic: pageviews-avro-topic1. Invalid value: null used for required field: "page_name", schema type: STRING
```

This is because `page_name` and `ts` are required fields without default values in the specified physical schema.

!!! important
    ksqlDB doesn't check a physical schema referred to by a schema ID contains fields are required without 
    making sure `null` can be serialized. Users are supposed to make sure `null` can be handled properly 
    either by making physical schema fields optional or by make sure `null` is never inserted by applying 
    the `IFNULL` function (see [IFNULL](/developer-guide/ksqldb-reference/scalar-functions/#ifnull)).