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

Here's what you can do with schema inference with ID in ksqlDB:

- Declare streams and tables on {{ site.ak }} topics with supported key and value formats by using 
  `CREATE STREAM` and `CREATE TABLE` statements with `KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID` properties, 
  without needing to declare the key and/or value columns.
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
The schema referred to by `VALUE_SCHEMA_ID` must be registered in {{ site.sr }}. It can be under
any subject but must match the format defined by `VALUE_FORMAT`.

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
Key or value columns must NOT be defined in statement if corresponding `KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID`
is supplied.

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
The schema referred to by `VALUE_SCHEMA_ID` must be registered in {{ site.sr }}. It can be under 
any subject but must match the format defined by `VALUE_FORMAT`.

### Declaring a derived view with schema ID.

The following statement shows how to create a materialized view derived from an existing source with the 
`VALUE_SCHEMA_ID` property. The schema referred to by `VALUE_SCHEMA_ID` is used to check column compatibility with
output columns and serialize output data. See [Schema Inference and Data Serialization](#Schema-Inference-and-Data-Serialization) 
for more information.

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
any subject but must match the format defined by `VALUE_FORMAT`. 

### Schema Inference and Data Serialization

This sections talks about the details in schema inference from schema in {{ site.sr }} to ksqlDB schema
and how output data are serialized when schema ID is used. We refer the schema in {{ site.sr }} as 
physical schema and the schema in ksqlDB as logical schema. Same physical schema instead of logical schema
will be registered under the subject `<topic-name>-key` or `<topic-name>-value` if corresponding
`KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID` is provided.

#### Schema Inference Schema Requirements

If `WRAP_SINGLE_VALUE` is set to `true` in the statement, the physical schema is expected to be in `struct`
type and the field names are used as data source column names. Field schemas are inference as
corresponding column data types.

!!! note
`struct` type in `AVRO` should be `record` type; in `PROTOBUF` it should be `message` type and in `JSON_SR`
it should be `object` type.

For example, if physical schema is `AVRO` schema and defined as following in {{ site.sr }} with ID 1:

```
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

then the schema for `pageviews` will be inferred as:

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

If `WRAP_SINGLE_VALUE` is `false` in the statement, if `KEY_SCHEMA_ID` is set, `ROWKEY` will be used
as key column name; if `VALUE_SCHEMA_ID` is set, `ROWVAL` will be used as value column name.
Physical schema will be used as the column data type.

For example, if physical schema is `AVRO` schema and defined as following in {{ site.sr }} with ID 2:

```
{"schema": "\"int\""}
```

and `CREATE` statement is defined as following:

```sql
CREATE TABLE pageview_count (
    pageId INT KEY
  ) WITH (
    KAFKA_TOPIC='pageview-count',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='AVRO',
    VALUE_SCHEMA_ID=2,
    WRAP_SINGLE_VALUE=false,
    PARTITIONS=1
  );
```

then the schema for `pageview_count` will be inferred as:

```
ksql> describe pageview_count;

Name                 : PAGEVIEW_COUNT
 Field  | Type
-----------------------------------------
 PAGEID | INTEGER          (primary key)
 ROWVAL | INTEGER
-----------------------------------------
```

For more information about `WRAP_SINGLE_VALUE`, see [Single Field (un)wrapping](/reference/serialization#single-field-unwrapping)

#### Schema Inference Type Handling

ksqlDB supports `null` value to be used in `KEY` and `VALUE` columns. This means if a field in the physical
schema is a required type, it will be translated to an optional type in logical schema. This has subtle 
implication for data serialization which is explained in below section.

!!! important
For unsupported types in physical schema, ksqlDB will ignore them and continue translating supported 
types to logical schema. For now, users are expected to verify if the logical schema is expected.

!!! important
During schema translation from physical schema to logical schema, for `struct` type, field names will be
used as column names in logical schema. Note that field names WON'T be uppercased whereas in schema inference
without schema id, the names will be uppercased.

#### Schema Compatibility Check for Derived View

When creating a materialized view, schema IDs can also be used. However, instead of inferring the logical 
schema for the view, it will be used to check compatibility agains current subquery's projection and serialize output
data. For compatibility checks, inferred logical schema must be a superset of subquery projection's
schema which means corresponding column name, type and order should match but inferred logical schema 
can have extra columns.

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

`pageviews_new`'s logical schema is decided by the projection in the subquery `SELECT pageId, ts FROM pageviews`.
When `VALUE_SCHEMA_ID` is used, inferred logical schema will be checked against `pageId` and `ts` for
compatibility. 

Compatible example of physical schema:

```sql
{
"schema":"{
    \"type\":\"record\",
    \"name\":\"PageViewNewSchema\",
    \"namespace\":\"io.confluent.ksql.avro_schemas\",
    \"fields\":[
        {\"name\":\"pageId\",\"type\":\"int\",\"default\":123},
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
In previous `AVRO` schema, `title` is an extra field, since physical schema will be used for data serialization,
`title` field with default value will appear in serialized data even though inserted value can never 
have `title` field because it's not in logical schema.

Incompatible example of physical schema because of mismatched type for `pageId`:
```sql
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

When schema ID is provided and schema inference is successful, data source can be successfully created.
When writing to the data source, the physical schema inferred by the schema ID will be used to serialize
data instead of logical schema being used in other cases. Because ksqlDB translated logical schema accepts
`null` value but physical schema may not, serialization can fail even the inserted value is valid value
for logical schema.

For example, if physical schema is `AVRO` schema and defined as following in {{ site.sr }} with ID 1:

```
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

!!! note
`AVRO` schema raw string in {{ site.sr }} should be single line raw string without `\n`. Above format
is for ease of read.

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

then the schema for `pageviews` will be inferred as:

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

When you insert values to `pageviews` with `null` value, it will return error:

```
ksql> insert into pageviews values (1, null, null);
Failed to insert values into 'PAGEVIEWS'. Could not serialize value: [ null | null ]. Error serializing message to topic: pageviews-avro-topic1. Invalid value: null used for required field: "page_name", schema type: STRING
```

This is because `page_name` and `ts` are required fields without default values in original physical schema.

!!! important
ksqlDB doesn't check physical schema referred to by schema ID that its fields are all optional or with 
a default value to make sure `null` can be serialized for flexibility of physical schema. Users are supposed
to make sure `null` can be handled properly either by make physical schema fields optional or with a default
value or by make sure `null` is never inserted.