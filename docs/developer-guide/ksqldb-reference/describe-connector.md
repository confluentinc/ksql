---
layout: page
title: DESCRIBE CONNECTOR
tagline:  ksqlDB DESCRIBE CONNECTOR statement
description: Syntax for the DESCRIBE CONNECTOR statement in ksqlDB
keywords: ksqlDB, describe, connector, connect
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/describe-connector.html';
</script>

DESCRIBE CONNECTOR
==================

Synopsis
--------

```sql
DESCRIBE CONNECTOR connector_name;
```

Description
-----------

Describe a connector. If the connector is one of the supported
connectors, this statement also lists the tables and streams that were
automatically imported to ksqlDB.

Example
-------

```sql
DESCRIBE CONNECTOR "my-jdbc-connector";
```

Your output should resemble:

```
Name                 : jdbc-connector
Class                : io.confluent.connect.jdbc.JdbcSourceConnector
Type                 : source
State                : RUNNING
WorkerId             : 10.200.7.69:8083

 Task ID | State   | Error Trace
---------------------------------
 0       | RUNNING |
---------------------------------

 ksqlDB Source Name     | Kafka Topic | Type
--------------------------------------------
 JDBC_CONNECTOR_USERS | jdbc-users  | TABLE
--------------------------------------------

 Related Topics
----------------
 jdbc-users
----------------
```
