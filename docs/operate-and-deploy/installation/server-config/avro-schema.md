---
layout: page
title: Configure ksqlDB for Avro, Protobuf, and JSON schemas
tagline: Set up Schema Registry to enable reading and writing messages in Avro, Protobuf, and JSON formats
description: Learn how integrate ksqlDB with Confluent Schema Registry
keywords: ksqldb, schema, avro, protobuf, json, json_sr
---

For supported [serialization formats](/reference/serialization),
ksqlDB can integrate with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).
ksqlDB automatically retrieves (reads) and registers (writes) schemas as needed,
which spares you from defining columns and data types  manually in `CREATE`
statements and from manual interaction with {{ site.sr }}. For more information,
see [Schema Inference](/operate-and-deploy/schema-registry-integration/#schema-inference).

Configure ksqlDB for Avro, Protobuf, and JSON
=============================================

You must configure the REST endpoint of {{ site.sr }} by setting
`ksql.schema.registry.url` in the ksqlDB Server configuration file
(`<path-to-confluent>/etc/ksqldb/ksql-server.properties`). For more
information, see [Configure ksqlDB for Secured Confluent Schema Registry](/operate-and-deploy/installation/server-config/security/#configure-ksqldb-for-secured-confluent-schema-registry).
{{ site.sr }} is [included by default](https://docs.confluent.io/current/quickstart/index.html) with
{{ site.cp }}.

!!! important
      Don't use the SET statement in the ksqlDB CLI to configure the registry
      endpoint.
