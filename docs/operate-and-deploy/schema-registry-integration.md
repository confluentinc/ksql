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
=======
---
layout: page
title: Schemas in ksqlDB
tagline: Defining the structure of your data
description: Learn how schemas work with ksqlDB
keywords: ksqldb, schema, evolution, avro, protobuf, json, csv
---

Data sources like streams and tables have an associated schema. This schema defines the columns
available in the data, just like the columns in a traditional SQL database table.

## Key vs Value columns

ksqlDB supports both key and value columns. These map to the data held in the
key and value of the underlying {{ site.ak }} topic message.

A column is defined by a combination of its [name](#valid-identifiers), its [SQL data type](#sql-data-types),
and possibly a namespace.

Key columns for a stream have a `KEY` suffix. Key columns for a table have a `PRIMARY KEY` suffix. Value 
columns have no namespace suffix. There can be multiple columns in either namespace, if
the underlying serialization format supports it.

!!! note 
    The `KAFKA` format doesn't support multi-column keys.

For example, the following statement declares a stream with multiple

key and value columns:

```sql
CREATE STREAM USER_UPDATES (
   ORGID BIGINT KEY,
   USERID BIGINT KEY, 
   STRING NAME, 
   ADDRESS ADDRESS_TYPE
 ) WITH (
   ...
 );
```

This statement declares a table with a primary key and value columns:

```sql
CREATE TABLE USERS (
   ORGID BIGINT PRIMARY KEY, 
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
    ksqlDB handles the `JSON` and `JSON_SR` formats differently. `JSON_SR` reads and 
    registers new schemas with {{ site.sr }} as necessary, while `JSON` requires you
    to specify the schema. Data formatted with `JSON_SR` is not binary compatible with
    the `JSON` format.

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
