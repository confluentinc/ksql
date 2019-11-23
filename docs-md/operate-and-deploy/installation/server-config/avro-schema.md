---
layout: page
title: Configure Avro and Schema Registry for ksqlDB
tagline: Read and write messages in Avro format
description: Learn how read and write messages in Avro format by integrating ksqlDB with Confluent Schema Registry
keywords: ksql, schema, avro
---

ksqlDB can read and write messages in Avro format by integrating with
[Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).
ksqlDB automatically retrieves (reads) and registers (writes) Avro schemas as
needed, which saves you from having to manually define columns
and data types in SQL and from manual interaction with {{ site.sr }}.

Supported functionality
=======================

ksqlDB currently supports Avro data in the {{ site.aktm }} message
values.

Avro schemas with nested fields are supported. You can read nested data,
in Avro and JSON formats, by using the STRUCT type. You can't create new
nested STRUCT data as the result of a query, but you can copy existing STRUCT
fields as-is. For more info, see [STRUCT](../../../developer-guide/syntax-reference.md#struct).

The following functionality is not supported:

-   Message keys in Avro format are not supported. Message keys in ksqlDB
    are always interpreted as STRING format, which means ksqlDB ignores
    Avro schemas that have been registered for message keys, and
    the key is read by using `StringDeserializer`.

Configure ksqlDB for Avro
=========================

You must configure the REST endpoint of {{ site.sr }} by setting
`ksql.schema.registry.url` (default: `http://localhost:8081`) in the
ksqlDB Server configuration file
(`<path-to-confluent>/etc/ksql/ksql-server.properties`). For more
information, see
[Installation Instructions](../installing.md#installation-instructions).

!!! important
      Don't use the SET statement in the ksqlDB CLI to configure the registry
      endpoint.

Use Avro in ksqlDB
==================

Before using Avro in ksqlDB, make sure that {{ site.sr }} is up and
running and that `ksql.schema.registry.url` is set correctly in the ksqlDB
properties file (defaults to `http://localhost:8081`). {{ site.sr }} is
[included by
default](https://docs.confluent.io/current/quickstart/index.html) with
{{ site.cp }}.

!!! important
      By default, ksqlDB-registered Avro schemas have the same name
      (`KsqlDataSourceSchema`) and the same namespace
      (`io.confluent.ksql.avro_schemas`). You can override this behavior by
      providing a `VALUE_AVRO_SCHEMA_FULL_NAME` property in the `WITH` clause,
      where you set the `VALUE_FORMAT` to `'AVRO'`. As the name suggests, this
      property overrides the default name/namespace with the provided one.
      For example, `com.mycompany.MySchema` registers a schema with the
      `MySchema` name and the `com.mycompany` namespace.

Here's what you can do with Avro in ksqlDB:

-   Declare streams and tables on {{ site.ak }} topics with Avro-formatted data
    by using `CREATE STREAM` and `CREATE TABLE` statements.
-   Read from and write into Avro-formatted data by using
    `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT` statements.
-   Create derived streams and tables from existing streams and tables
    with `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT`
    statements.
-   Convert data to different formats with `CREATE STREAM AS SELECT` and
    `CREATE TABLE AS SELECT` statements. For example, you can convert a
    stream from Avro to JSON.

Example SQL Statements with Avro
--------------------------------

### Create a New Stream by Reading Avro-formatted Data

The following statement shows how to create a new `pageviews` stream by
reading from a {{ site.ak }} topic that has Avro-formatted messages.

```sql
CREATE STREAM pageviews
  WITH (KAFKA_TOPIC='pageviews-avro-topic',
        VALUE_FORMAT='AVRO');
```

### Create a New Table by Reading Avro-formatted Data

The following statement shows how to create a new `users` table by
reading from a {{ site.ak }} topic that has Avro-formatted messages.

```sql
CREATE TABLE users
  WITH (KAFKA_TOPIC='users-avro-topic',
        VALUE_FORMAT='AVRO',
        KEY='userid');
```

In this example, you don't need to define any columns or data types in
the CREATE statement. ksqlDB infers this information automatically from
the latest registered Avro schema for the `pageviews-avro-topic` topic.
ksqlDB uses the most recent schema at the time the statement is first
executed.

### Create a New Stream with Selected Fields of Avro-formatted Data

If you want to create a STREAM or TABLE with only a subset of all the
available fields in the Avro schema, you must explicitly define the
columns and data types.

The following statement shows how to create a new `pageviews_reduced`
stream, which is similar to the previous example, but with only a few of
the available fields in the Avro data. In this example, only the
`viewtime` and `pageid` columns are picked.

```sql
CREATE STREAM pageviews_reduced (viewtime BIGINT, pageid VARCHAR)
  WITH (KAFKA_TOPIC='pageviews-avro-topic',
        VALUE_FORMAT='AVRO');
```

### Convert a JSON Stream to an Avro Stream

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
CREATE STREAM pageviews_json (viewtime BIGINT, userid VARCHAR, pageid VARCHAR)
  WITH (KAFKA_TOPIC='pageviews_kafka_topic_json', VALUE_FORMAT='JSON');

CREATE STREAM pageviews_avro
  WITH (VALUE_FORMAT = 'AVRO') AS
  SELECT * FROM pageviews_json
  EMIT CHANGES;
```

For more information, see
[Changing Data Serialization Format from JSON to Avro](https://www.confluent.io/stream-processing-cookbook/ksql-recipes/changing-data-serialization-format-json-avro)
in the [Stream Processing Cookbook](https://www.confluent.io/product/ksql/stream-processing-cookbook).

Page last revised on: {{ git_revision_date }}
