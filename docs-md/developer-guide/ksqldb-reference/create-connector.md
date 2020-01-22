---
layout: page
title: CREATE CONNECTOR
tagline:  ksqlDB CREATE CONNECTOR statement
description: Syntax for the CREATE CONNECTOR statement in ksqlDB
keywords: ksqlDB, create, connector, connect
---

CREATE CONNECTOR
================

Synopsis
--------

```sql
CREATE SOURCE | SINK CONNECTOR connector_name WITH( property_name = expression [, ...]);
```

Description
-----------

Create a new connector in the {{ site.kconnectlong }} cluster with the
configuration passed in the WITH clause. Some connectors have ksqlDB templates
that simplify configuring them. For more information, see
[Natively Supported Connectors](../../concepts/connectors.md#natively-supported-connectors).

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

Page last revised on: {{ git_revision_date }}