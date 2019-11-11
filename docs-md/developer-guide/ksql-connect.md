---
layout: page
title: Integrate Kafka Connect with ksqlDB
tagline: Connect your ksqlDB applications to data
description: Learn how to integrate your ksqlDB applications with Kafka connectors
---

{{ site.kconnectlong }} is an open source component of {{ site.aktm }} that
simplifies loading and exporting data between Kafka and external systems.
KSQL provides functionality to manage and integrate with {{ site.kconnect }}:

-   Create Connectors
-   Describe Connectors
-   Import topics created by {{ site.kconnect }} to KSQL

Setup KSQL-Connect Integration
------------------------------

There are two ways to deploy the KSQL-Connect integration:

1.  **External**: If a {{ site.kconnect }} cluster is available, set the
    `ksql.connect.url` property in your KSQL server configuration file.
    The default value for this is `localhost:8083`.
2.  **Embedded**: KSQL can double as a {{ site.kconnect }} server and
    will run a [Distributed
    Mode](https://docs.confluent.io/current/connect/userguide.html#distributed-mode)
    cluster co-located on the KSQL server instance. To do this, supply a
    connect properties configuration file to the server and specify this
    file in the `ksql.connect.worker.config` property.

!!! note
	For environments that need to share connect clusters and provide
    predictable workloads, running {{ site.kconnect }} externally is the
    recommended deployment option.

### Plugins

KSQL doesn't ship with connectors pre-installed, so you must download and
install connectors. A good way to install connectors is by using
[Confluent Hub](https://www.confluent.io/hub/).

Natively Supported Connectors
-----------------------------

While it is possible to create, describe and list connectors of all
types, KSQL supports a few connectors natively. KSQL provides templates
to ease creation of connectors and custom code to explore topics created by
these connectors into KSQL:

-   [Kafka Connect JDBC Connector (Source and
    Sink)](https://docs.confluent.io/current/connect/kafka-connect-jdbc/index.html):
    because the JDBC connector doesn't populate the key automatically for
    the Kafka messages that it produces, KSQL supplies the ability to
    pass in `"key"='<column_name>'` in the `WITH` clause to extract a
    column from the value and make it the key.

### Syntax

#### CREATE CONNECTOR

**Synopsis**

```sql
CREATE SOURCE | SINK CONNECTOR connector_name WITH( property_name = expression [, ...]);
```

**Description**

Create a new Connector in the {{ site.kconnectlong }} cluster with the
configuration passed in the WITH clause. Some connectors have KSQL templates
that simplify the configuration. For more information, see
[Natively Supported Connectors](#natively-supported-connectors).

Example:

```sql
CREATE SOURCE CONNECTOR `jdbc-connector` WITH(
    "connector.class"='io.confluent.connect.jdbc.JdbcSourceConnector',
    "connection.url"='jdbc:postgresql://localhost:5432/my.db',
    "mode"='bulk',
    "topic.prefix"='jdbc-',
    "table.whitelist"='users',
    "key"='username');
```

#### DROP CONNECTOR

**Synopsis**

```sql
DROP CONNECTOR connector_name;
```

**Description**

Drop a connector and delete it from the {{ site.kconnect }} cluster. The
topics associated with this cluster are not deleted by this command.

#### DESCRIBE CONNECTOR

**Synopsis**

```sql
DESCRIBE CONNECTOR connector_name;
```

Describe a connector. If the connector is one of the supported
connectors, this statement also lists the tables and streams that were
automatically imported to KSQL.

Example:

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

 KSQL Source Name     | Kafka Topic | Type
--------------------------------------------
 JDBC_CONNECTOR_USERS | jdbc-users  | TABLE
--------------------------------------------

 Related Topics
----------------
 jdbc-users
----------------
```

#### SHOW CONNECTORS

**Synopsis**

```sql
SHOW | LIST CONNECTORS;
```

**Description**

List all connectors in the {{ site.kconnect }} cluster.

!!! note
	The SHOW and LIST statements don't distinguish connectors that are created by
    using the KSQL from connectors that are created independently bu using the
    {{ site.kconnect }} API.

Page last revised on: {{ git_revision_date }}
