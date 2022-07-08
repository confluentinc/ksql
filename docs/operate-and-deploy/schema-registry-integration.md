---
layout: page
title: Schema Inference
tagline: ksqlDB and Confluent Schema Registry integration
description: ksqlDB integrates with Confluent Schema Registry to read and write schemas as needed 
keywords: serialization, schema, schema registry, json, avro, delimited
---
## Schema Inference

For supported [serialization formats](/reference/serialization),
ksqlDB can integrate with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).
ksqlDB automatically retrieves (reads) and registers (writes) schemas as needed,
which spares you from defining columns and data types manually in `CREATE`
statements and from manual interaction with {{ site.sr }}. Before using schema
inference in ksqlDB, make sure that the {{ site.sr }} is up and running and
ksqlDB is [configured to use it](../operate-and-deploy/installation/server-config/avro-schema.md).

Here's what you can do with schema inference in ksqlDB:

-   Declare streams and tables on {{ site.ak }} topics with supported key and value formats by using 
    `CREATE STREAM` and `CREATE TABLE` statements, without needing to declare the key and/or value columns.
-   Declare derived views with `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT` statements.
    The schema of the view is registered in {{ site.sr }} automatically.
-   Convert data to different formats with `CREATE STREAM AS SELECT` and
    `CREATE TABLE AS SELECT` statements, by declaring the required output
    format in the `WITH` clause. For example, you can convert a stream from
    Avro to JSON.
    
If you're declaring a stream or table with a key format that's different from its
value format, and only one of the two formats supports schema inference,
you can explicitly provide the columns for the format that does not support schema inference
while still having ksqlDB load columns for the format that does support schema inference
from {{ site.sr }}. This is known as _partial schema inference_. To infer value columns
for a keyless stream, set the key format to the [`NONE` format](/reference/serialization#none).

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
from a {{ site.ak }} topic that has Avro-formatted key and message values.

```sql
CREATE STREAM pageviews WITH (
    KAFKA_TOPIC='pageviews-avro-topic',
    KEY_FORMAT='AVRO',
    VALUE_FORMAT='AVRO'
  );
```

In the previous example, ksqlDB infers the key and value columns automatically from the latest
registered schemas for the `pageviews-avro-topic` topic. ksqlDB uses the most
recent schemas at the time the statement is first executed.

!!! note
    The key and value schemas must be registered in {{ site.sr }} under the subjects
    `pageviews-avro-topic-key` and `pageviews-avro-topic-value`, respectively.
    
#### With partial schema inference

The following statement shows how to create a new `pageviews` stream by reading
from a {{ site.ak }} topic that has Avro-formatted message values and a
`KAFKA`-formatted `INT` message key.

```sql
CREATE STREAM pageviews (
    pageId INT KEY
  ) WITH (
    KAFKA_TOPIC='pageviews-avro-topic',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='AVRO'
  );
```

In the previous example, only the key column is supplied in the CREATE
statement.  ksqlDB infers the value columns automatically from the latest
registered schema for the `pageviews-avro-topic` topic. ksqlDB uses the most
recent schema at the time the statement is first executed.

!!! note
    The schema must be registered in {{ site.sr }} under the subject
    `pageviews-avro-topic-value`.

### Create a new table

#### With key and value schema inference

The following statement shows how to create a new `users` table by reading
from a {{ site.ak }} topic that has Avro-formatted key and message values.

```sql
CREATE TABLE users (
    userId BIGINT PRIMARY KEY
  ) WITH (
    KAFKA_TOPIC='users-avro-topic',
    KEY_FORMAT='AVRO',
    VALUE_FORMAT='AVRO'
  );
```

In the previous example, ksqlDB infers the key and value columns automatically from the latest
registered schemas for the `users-avro-topic` topic. ksqlDB uses the most
recent schemas at the time the statement is first executed.

!!! note
    The key and value schemas must be registered in {{ site.sr }} under the subjects
    `users-avro-topic-key` and `users-avro-topic-value`, respectively.

#### With partial schema inference

The following statement shows how to create a new `users` table by reading
from a {{ site.ak }} topic that has Avro-formatted message values and a
`KAFKA`-formatted `BIGINT` message key.

```sql
CREATE TABLE users (
    userId BIGINT PRIMARY KEY
  ) WITH (
    KAFKA_TOPIC='users-avro-topic',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='AVRO'
  );
```

In the previous example, only the key column is supplied in the CREATE
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
    The value schema will be registered in {{ site.sr }} under the subject
    `PAGEVIEWS_BY_URL-value`.

### Converting formats

ksqlDB enables you to change the underlying key and value formats of streams and tables. 
This means that you can easily mix and match streams and tables with different
data formats and also convert between formats. For example, you can join
a stream backed by Avro data with a table backed by JSON data.

The example below converts a topic into JSON-formatted values into Avro. Only the
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
    The value schema will be registered in {{ site.sr }} under the subject
    `PAGEVIEWS_AVRO-value`.

For more information, see
[Changing Data Serialization Format from JSON to Avro](https://www.confluent.io/stream-processing-cookbook/ksql-recipes/changing-data-serialization-format-json-avro)
in the [Stream Processing Cookbook](https://www.confluent.io/product/ksql/stream-processing-cookbook).

You can convert between different key formats in an analogous manner by specifying the
`KEY_FORMAT` property instead of `VALUE_FORMAT`.

### Schema Inference Details
The schema in {{ site.sr }} is a "physical schema", and the schema in ksqlDB is
a "logical schema". The physical schema, not the logical schema, is registered
under the subject `<topic-name>-key` or `<topic-name>-value` if a corresponding key schema or value
schema is inferred.

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
registered with {{ site.sr }} under subject `pageviews-value`:

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
and the value schema will be inferred from {{ site.sr }}.

```sql hl_lines="7"
CREATE STREAM pageviews (
    pageId INT KEY
  ) WITH (
    KAFKA_TOPIC='pageviews-avro-topic',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='AVRO',
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
 PAGE_NAME | VARCHAR(STRING)
 TS        | INTEGER
------------------------------------
```

!!! important
    - ksqlDB ignores unsupported types in the physical schema and continues
    translating supported types to the logical schema. You should verify that
    the logical schema is translated as expected.
    - During schema translation from a physical schema to a logical schema,
    `struct` type field names are used as column names in the logical schema.
    Field names are translated to uppercase, in contrast with schema
    inference with a schema id, which _does not_ translate field names to
    uppercase.

If `WRAP_SINGLE_VALUE` is `false` in the statement, and if the key schema is
inferred, `ROWKEY` is used as the key's column name.

If value schema is inferred, `ROWVAL` is used as the value's column name. The physical
schema is used as the column data type.

For example, the following physical schema is `AVRO` and is defined in {{ site.sr }} under
subject name `pageview_count-value`:

```json
{"schema": "int"}
```

The following `CREATE` statement defines a table on the `pageview-count` topic
and the value schema will be inferred from {{ site.sr }}:

```sql hl_lines="7"
CREATE TABLE pageview_count (
    pageId INT PRIMARY KEY
  ) WITH (
    KAFKA_TOPIC='pageview-count',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='AVRO',
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
[Single Field (un)wrapping](/reference/serialization/#single-field-unwrapping).