---
layout: page
title: Schema Inference With ID
tagline: ksqlDB Schema Inference With ID
description: ksqlDB integrates with Confluent Schema Registry to read and write schemas as needed 
keywords: ksqlDB, serialization, schema, schema registry, json, avro, delimited, KEY_SCHEMA_ID, VALUE_SCHEMA_ID
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/schema-inference-with-id.html';
</script>

For supported [serialization formats](/reference/serialization), ksqlDB can use 
[schema inference](/operate-and-deploy/schema-registry-integration)
to retrieve (read) and register (write) schemas as needed. If you specify a
`KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID` explicitly in the `CREATE` statements,
ksqlDB retrieves and registers the schema specified by the ID from {{ site.sr }},
and it also serializes data using exactly the same schema referred to by the ID.
This can spare you from defining columns and data types manually and also make
sure the data are serialized by the specified physical schema, which can be
consumed in downstream systems. Before using schema inference with explicit IDs
in ksqlDB, make sure that the {{ site.sr }} is up and running and ksqlDB is
[configured to use it](../operate-and-deploy/installation/server-config/avro-schema.md).

Here's what you can do with schema inference with IDs in ksqlDB:

- Declare streams and tables on {{ site.ak }} topics with supported key and
  value formats by using `CREATE STREAM` and `CREATE TABLE` statements with
  `KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID` properties, without the need to declare
  the key and value columns.
- Declare derived views with `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT`
  statements with `KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID` properties. The schema
  of the view is registered in {{ site.sr }} automatically.
- Serialize output data using the schema referred to by `KEY_SCHEMA_ID` or
`VALUE_SCHEMA_ID`, instead of the logical data source schema stored in ksqlDB.
    
If you're declaring a stream or table with a key format that's different from
its value format, and only one of the two formats supports schema inference,
you can explicitly provide the columns for the format that doesn't support
schema inference while still having ksqlDB load columns for the format that
does support schema inference from {{ site.sr }}. This is known as
_partial schema inference_. To infer value columns for a keyless stream, set
the key format to [`NONE`](/reference/serialization#none).

Tables require a `PRIMARY KEY`, so you must supply one explicitly in your
`CREATE TABLE` statement. `KEY` columns are optional for streams, so if you
don't supply one, the stream is created without a key column.

!!! note
    The following example statements show how to create streams and tables that
    have Avro-formatted data. If you want to use Protobuf or JSON-formatted data,
    substitute `PROTOBUF` or `JSON_SR` for `AVRO` in each statement.

### Create a new stream or table

When `KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID` is used in statements to create
a stream or table, the schema fetched from {{ site.sr }} is used to infer data
source's columns and serialize output data. See
[Schema inference and data serialization](#schema-inference-and-data-serialization)
for details about how columns are inferred and data are serialized.

!!! important
    - The schemas referred to by `KEY_SCHEMA_ID` and `VALUE_SCHEMA_ID` must be
      registered in {{ site.sr }}. They can be under any subject but must match
      the formats defined by `KEY_FORMAT` and `VALUE_FORMAT`, respectively.
    - You can't define key or value columns in a statement if a corresponding
      `KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID` is supplied.

#### Without a key column

The following statement shows how to create a new `pageviews` stream by
reading from a {{ site.ak }} topic that has Avro-formatted message values.

```sql
CREATE STREAM pageviews
  WITH (
    KAFKA_TOPIC='pageviews-avro-topic',
    VALUE_FORMAT='AVRO',
    VALUE_SCHEMA_ID=1
  );
```

In this example, you don't need to define any columns in the CREATE statement.
ksqlDB infers this information automatically from {{ site.sr }} using the
provided `VALUE_SCHEMA_ID`.

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

In this example, ksqlDB infers the key and value columns automatically from
{{ site.sr }} using the provided `KEY_SCHEMA_ID` and `VALUE_SCHEMA_ID`.

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

In this example, only the key column is supplied in the CREATE statement.
ksqlDB infers the value columns automatically from {{ site.sr }} using the
provided `VALUE_SCHEMA_ID`.

### Declaring a derived view with schema ID.

The following statement shows how to create a materialized view derived from
an existing source with the `VALUE_SCHEMA_ID` property. The schema referred to
by `VALUE_SCHEMA_ID` is used to check column compatibility with output columns
and serialize output data. For more information, see
[Schema inference and data serialization](#schema-inference-and-data-serialization).

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
    The schema referred to by `VALUE_SCHEMA_ID` must be compatible with the
    logical schema defined by the `SELECT` clause. For more information, see
    [Schema inference and data serialization](#schema-inference-and-data-serialization).

### Schema inference and data serialization

The schema in {{ site.sr }} is a "physical schema", and the schema in ksqlDB is
a "logical schema". The physical schema, not the logical schema, is registered
under the subject `<topic-name>-key` or `<topic-name>-value` if corresponding
`KEY_SCHEMA_ID` or `VALUE_SCHEMA_ID` values are provided.

#### Schema inference schema requirements

If `WRAP_SINGLE_VALUE` is set to `true` in the SQL statement, the physical
schema is expected to be a `struct` type, and the field names are used as data
source column names. Field types are inferred from corresponding column data
types.

- In `AVRO`, the `struct` type corresponds with the `record` type.
- In `PROTOBUF` the `struct` type corresponds with the `message` type.
- In `JSON_SR`, the `struct` type corresponds with the `object` type.

!!! note
    In the following examples, the `AVRO` schema string in {{ site.sr }}
    is a single-line raw string without newline characters (`\n`). The 
    strings are shown as human-readable text for convenience.

For example, the following a physical schema is in `AVRO` format and is
registered with {{ site.sr }} with ID 1:

```json
{
  "schema": {
    "type": "record",
    "name": "PageViewValueSchema",
    "namespace": "io.confluent.ksql.avro_schemas",
    "fields": [
      {
        "name": "page_name",
        "type": "string",
        "default": "abc"
      },
      {
        "name": "ts",
        "type": "int",
        "default": 123
      }
    ]
  }
}
```

The following `CREATE` statement defines a stream on the `pageviews` topic
and specifies the physical schema that has an ID of `1`.

```sql hl_lines="7"
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

The following output from the `describe pageviews` command shows the inferred
logical schema for the `pageviews` stream:

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

If `WRAP_SINGLE_VALUE` is `false` in the statement, and if `KEY_SCHEMA_ID` is
set, `ROWKEY` is used as the key's column name.

If `VALUE_SCHEMA_ID` is set, `ROWVAL` is used as the value's column name. The physical
schema is used as the column data type.

For example, the following physical schema is `AVRO` and is defined in {{ site.sr }}
with ID `2`:

```json
{"schema": "int"}
```

The following `CREATE` statement defines a table on the `pageview-count` topic
and specifies the physical schema that has ID `2`.

```sql hl_lines="7"
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

The inferred logical schema for the `pageview_count` table is:

```
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

ksqlDB supports the `null` value for `KEY` and `VALUE` columns. If a field
in the physical schema is a required type, it's translated to an optional type
in the logical schema. This has subtle implications for data serialization
which are explained in the following section.

!!! important
    - ksqlDB ignores unsupported types in the physical schema and continues
      translating supported types to the logical schema. You should verify that
      the logical schema is translated as expected.
    - During schema translation from a physical schema to a logical schema,
      `struct` type field names are used as column names in the logical schema.
      Field names _are not_ translated to uppercase, in contrast with schema
      inference without a schema id, which _does_ translate field names to
      uppercase.

#### Schema Compatibility Check for Derived View

You can use schema IDs when creating a materialized view, but instead of
inferring the logical schema for the view, the schema is used to check
compatibility against the query's projection and serialized output data.
For compatibility checks, the inferred logical schema must be a superset of
the query's projection schema, which means corresponding column names, types,
and order must match. The inferred logical schema may have extra columns.

The following example creates the `pageviews_new` topic as the result of a
SELECT query:

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

If the `pageviews` value column has the type `ts INT`, the logical schema of
`pageviews_new` is decided by the projection in the query
`SELECT pageId, ts FROM pageviews`. When `VALUE_SCHEMA_ID` is used, 
the inferred logical schema is checked against `ts INT` for compatibility. 

The following example shows a compatible physical schema:

```json hl_lines="8-9"
{
  "schema": {
    "type": "record",
    "name": "PageViewNewSchema",
    "namespace": "io.confluent.ksql.avro_schemas",
    "fields": [
      {
        "name": "ts",
        "type": "int",
        "default": 123
      },
      {
        "name": "title",
        "type": "string",
        "default": "title"
      }
    ]
  }
}
```

In this `AVRO` schema, `title` is an extra field. Because the physical schema
is used for data serialization, the `title` field with a default value appears
in serialized data, even though the inserted value can never set the `title`
field, because it's not in the logical schema defined by the `SELECT` clause of
the query.

The following example shows an incompatible physical schema, which is
incompatible because of the type mismatch for `pageId`.

```json hl_lines="8-9"
{
  "schema": {
    "type": "record",
    "name": "PageViewNewSchema",
    "namespace": "io.confluent.ksql.avro_schemas",
    "fields": [
      {
        "name": "pageId",
        "type": "string",
        "default": "id"
      },
      {
        "name": "ts",
        "type": "int",
        "default": 123
      }
    ]
  }
}
```

#### Data Serialization

When a schema ID is provided, and schema inference is successful, ksqlDB can
create the data source. When writing to the data source, the physical schema
inferred by the schema ID is used to serialize data, instead of the logical
schema that's used in other cases. Because ksqlDB's logical schema accepts
`null` values but the physical schema may not, serialization can fail even if
the inserted value is valid for the logical schema.

The following example shows a physical schema that's defined in {{ site.sr }}
with ID `1`. No default values are specified for the `page_name` and `ts`
fields. 

```json hl_lines="8-9 12-13"
{
  "schema": {
    "type": "record",
    "name": "PageViewValueSchema",
    "namespace": "io.confluent.ksql.avro_schemas",
    "fields": [
      {
        "name": "page_name",
        "type": "string"
      },
      {
        "name": "ts",
        "type": "int"
      }
    ]
  }
}
```

The following example creates a stream with schema ID `1`:

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

ksqlDB infers the following schema for `pageviews`:

```
Name                 : PAGEVIEWS
 Field     | Type
------------------------------------
 PAGEID    | INTEGER          (key)
 page_name | VARCHAR(STRING)
 ts        | INTEGER
------------------------------------
```

If you insert values to `pageviews` with `null` values, ksqlDB returns an
error:

```sql
insert into pageviews values (1, null, null);
```
```
Failed to insert values into 'PAGEVIEWS'. Could not serialize value: [ null | null ]. Error serializing message to topic: pageviews-avro-topic1. Invalid value: null used for required field: "page_name", schema type: STRING
```

This error occurs because `page_name` and `ts` are required fields without
default values in the specified physical schema.

!!! important
    ksqlDB doesn't check that `null` can be serialized in a physical schema
    that contains required fields. You must ensure that `null` can be handled
    properly, either by making physical schema fields optional or by using the
    [IFNULL](/developer-guide/ksqldb-reference/scalar-functions/#ifnull)
    function to ensure that `null` is never inserted.

!!! important
    If `key_schema_id` is used in table creation with windowed aggregation, the serialized key value
    also contain window information in addition to original key value.
