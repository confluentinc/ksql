---
layout: page
title: CREATE CONNECTOR
tagline:  ksqlDB CREATE CONNECTOR statement
description: Syntax for the CREATE CONNECTOR statement in ksqlDB
keywords: ksqlDB, create, connector, connect
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/create-connector.html';
</script>

CREATE CONNECTOR
================

Synopsis
--------

```sql
CREATE SOURCE | SINK CONNECTOR [IF NOT EXISTS] connector_name WITH( property_name = expression [, ...]);
```

Description
-----------

Create a new connector in the {{ site.kconnectlong }} cluster with the
configuration passed in the WITH clause. Some connectors have ksqlDB templates
that simplify configuring them. For more information, see
[Natively Supported Connectors](../../concepts/connectors.md#natively-supported-connectors).

If the IF NOT EXISTS clause is present, the statement does not fail if a connector with the supplied name
already exists

!!! note
    CREATE CONNECTOR works only in interactive mode. 

Example
-------

```sql
CREATE SOURCE CONNECTOR `jdbc-connector` WITH(
    "connector.class"='io.confluent.connect.jdbc.JdbcSourceConnector',
    "connection.url"='jdbc:postgresql://localhost:5432/my.db',
    "mode"='bulk',
    "topic.prefix"='jdbc-',
    "table.whitelist"='users',
    "key"='username');
```

!!! Tip "See CREATE CONNECTOR in action"
    - [Materialized cache - Start the Debezium source connector](/tutorials/materialized/#start-the-debezium-connector)
    - [Streaming ETL pipeline - Start the source connectors](/tutorials/etl#start-the-postgres-and-mongodb-debezium-source-connectors)
    - [Streaming ETL pipeline- Start the Elasticsearch sink connector](/tutorials/etl/#start-the-elasticsearch-sink-connector)
