---
layout: page
title: Configure ksqlDB for Avro, Protobuf, and JSON schemas
tagline: Set up Schema Registry to enable reading and writing messages in Avro, Protobuf, and JSON formats
description: Learn how read and write messages in Avro, Protobuf, and JSON by integrating ksqlDB with Confluent Schema Registry
keywords: ksqldb, schema, avro, protobuf, json, json_sr
---

ksqlDB can read and write messages that have Avro, Protobuf, or JSON schemas
by integrating with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).
ksqlDB automatically retrieves (reads) and registers (writes) schemas as
needed, which saves you from having to manually define columns
and data types in SQL and from manual interaction with {{ site.sr }}.

Supported functionality
=======================

ksqlDB supports Avro, Protobuf, and JSON data in the {{ site.aktm }} message
values.

Schemas with nested fields are supported. You can read nested data,
in Avro, Protobuf, and JSON formats, by using the STRUCT type. Also, you can
create new nested STRUCT data as the result of a query. For more info, see
[STRUCT](../../../developer-guide/syntax-reference.md#struct).

The following functionality is not supported:

-   Message *keys* in Avro, Protobuf, or JSON formats are not supported. Message
    keys in ksqlDB are always interpreted as KAFKA format, which means ksqlDB
    ignores schemas that have been registered for message keys.

While ksqlDB does not support loading the message key's schema from the {{ site.sr }},
you can provide the key column definition within the `CREATE TABLE` or `CREATE STREAM`
statement. Where a `CREATE TABLE` or `CREATE STREAM` statement does not provide an
explicit key columns an implicit `ROWKEY STRING` column will be added.

Configure ksqlDB for Avro, Protobuf, and JSON
=============================================

You must configure the REST endpoint of {{ site.sr }} by setting
`ksql.schema.registry.url` (default: `http://localhost:8081`) in the
ksqlDB Server configuration file
(`<path-to-confluent>/etc/ksqldb/ksql-server.properties`). For more
information, see
[Installation Instructions](../installing.md#installation-instructions).

!!! important
      Don't use the SET statement in the ksqlDB CLI to configure the registry
      endpoint.

Use Avro, Protobuf, or JSON in ksqlDB
=====================================

Before using Avro, Protobuf, or JSON in ksqlDB, make sure that {{ site.sr }} is up and
running and that `ksql.schema.registry.url` is set correctly in the ksqlDB
properties file (defaults to `http://localhost:8081`). {{ site.sr }} is
[included by default](https://docs.confluent.io/current/quickstart/index.html) with
{{ site.cp }}.

!!! important
      By default, ksqlDB-registered schemas have the same name
      (`KsqlDataSourceSchema`) and the same namespace
      (`io.confluent.ksql.avro_schemas`). You can override this behavior by
      providing a `VALUE_AVRO_SCHEMA_FULL_NAME` property in the `WITH` clause,
      where you set the `VALUE_FORMAT` to `'AVRO'`. As the name suggests, this
      property overrides the default name/namespace with the provided one.
      For example, `com.mycompany.MySchema` registers a schema with the
      `MySchema` name and the `com.mycompany` namespace.

Here's what you can do with Avro, Protobuf, and JSON in ksqlDB:

-   Declare streams and tables on {{ site.ak }} topics with Avro- or Protobuf-
    formatted data by using `CREATE STREAM` and `CREATE TABLE` statements.
-   Read from and write into Avro- or Protobuf-formatted data by using
    `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT` statements.
-   Create derived streams and tables from existing streams and tables
    with `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT`
    statements.
-   Convert data to different formats with `CREATE STREAM AS SELECT` and
    `CREATE TABLE AS SELECT` statements. For example, you can convert a
    stream from Avro to JSON.

Example SQL statements with Avro
--------------------------------

The following statements show how to create streams and tables that have 
Avro-formatted data. If you want to use Protobuf- or JSON-formatted data,
substitute `PROTOBUF` or `JSON_SR` for `AVRO` in each statement.

!!! note
    ksqlDB handles the `JSON` and `JSON_SR` formats differently. Use `JSON_SR`
    when you need schema validation by {{ site.sr }}. If you don't need schema
    validation, you can use `JSON`.

### Create a new stream by reading Avro-formatted data

The following statement shows how to create a new `pageviews` stream by
reading from a {{ site.ak }} topic that has Avro-formatted message values and
a Kafka-formatted INT message key.

```sql
CREATE STREAM pageviews
  (pageId INT KEY)
  WITH (KAFKA_TOPIC='pageviews-avro-topic',
        VALUE_FORMAT='AVRO');
```

If the key schema is not provided, the key of the data will be assumed to be
a single KAFKA serialized `STRING` named `ROWKEY`.

### Create a new table by reading Avro-formatted data

The following statement shows how to create a new `users` table by
reading from a {{ site.ak }} topic that has Avro-formatted message values.

```sql
CREATE TABLE users
  WITH (KAFKA_TOPIC='users-avro-topic',
        VALUE_FORMAT='AVRO');
```

In this example, you don't need to define any columns or data types in
the CREATE statement. ksqlDB infers this information automatically from
the latest registered schema for the `pageviews-avro-topic` topic.
ksqlDB uses the most recent schema at the time the statement is first
executed.

!!! note
    The schema must be registered in {{ site.sr }} under the subject
    `users-avro-topic-value`.

By default, the key of the data will be assumed to be a single `KAFKA`
serialized `STRING` named `ROWKEY`. If the key schema differs, then you
can provide just the key column in the statement. For example, the following
statement creates the `users` table with a 64-bit integer key and infers the
value columns from the Avro schema.

```sql
CREATE TABLE users (userId BIGINT PRIMARY KEY)
  WITH (KAFKA_TOPIC='users-avro-topic',
        VALUE_FORMAT='AVRO');
```

### Create a new stream with selected fields of Avro-formatted data

If you want to create a STREAM or TABLE with only a subset of all the
available fields in the Avro schema, you must explicitly define the
columns and data types.

The following statement shows how to create a new `pageviews_reduced`
stream, which is similar to the previous example, but with only a few of
the available fields in the Avro data. In this example, only the
`viewtime` and `pageid` columns are picked.

```sql
CREATE STREAM pageviews_reduced (pageid VARCHAR KEY, viewtime BIGINT)
  WITH (KAFKA_TOPIC='pageviews-avro-topic',
        VALUE_FORMAT='AVRO');
```

### Convert a JSON Stream to an Avro stream

ksqlDB enables you to work with streams and tables regardless of their
underlying data format. This means that you can easily mix and match
streams and tables with different data formats and also convert between
data formats. For example, you can join a stream backed by Avro data
with a table backed by JSON data.

In this example, only the `VALUE_FORMAT` is required for Avro to achieve
the data conversion. ksqlDB automatically generates an appropriate Avro
schema for the new `pageviews_avro` stream, and it registers the schema
with {{ site.sr }}.

```sql
CREATE STREAM pageviews_json (pageid VARCHAR KEY, viewtime BIGINT, userid VARCHAR)
  WITH (KAFKA_TOPIC='pageviews_kafka_topic_json', VALUE_FORMAT='JSON');

CREATE STREAM pageviews_avro
  WITH (VALUE_FORMAT = 'AVRO') AS
  SELECT * FROM pageviews_json
  EMIT CHANGES;
```

For more information, see
[Changing Data Serialization Format from JSON to Avro](https://www.confluent.io/stream-processing-cookbook/ksql-recipes/changing-data-serialization-format-json-avro)
in the [Stream Processing Cookbook](https://www.confluent.io/product/ksql/stream-processing-cookbook).
