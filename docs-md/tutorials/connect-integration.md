---
layout: page
title: Integrate with PostgresDB
tagline: Integrate KSQL with Kafka Connect
description: Learn how to use KSQL and Kafka Connect to integrate with PostgresDB
keywords: ksql
---

Integrating with PostgresDB
===========================

This tutorial demonstrates a simple workflow to integrate KSQL with an
instance of PostgresDB.

Prerequisites:

-   [Confluent
    Platform](https://docs.confluent.io/current/installation/installing_cp/index.html)
    is installed an running. This installation includes a Kafka broker,
    KSQL, {{ site.zk }}, {{ site.sr }} and {{ site.kconnect }}.
-   If you installed {{ site.cp }} via TAR or ZIP, navigate into the
    installation directory. The paths and commands used throughout this
    tutorial assume that you are in this installation directory.
-   Consider
    [installing](https://docs.confluent.io/current/cli/installing.html)
    the {{ site.confluentcli }} to start a local installation of {{
    site.cp }}.
-   Java: Minimum version 1.8. Install Oracle Java JRE or JDK >= 1.8 on
    your local machine

Installing JDBC Source Connector Plugin
---------------------------------------

If you installed {{ site.kconnectlong }} via {{ site.cp }}, then it
comes with an installation of the JDBC source connector. Otherwise,
install it from Confluent Hub.

Installing Postgres via Docker
------------------------------

If you are experimenting with the KSQL-Connect integration and don't
have a PostgresDB instance locally, you can install it by using Docker and
populate some data:

1.  Install PostgresDB by using the `docker pull postgres` command.
    Start the database and expose the JDBC port:
    ```
    docker run -p 5432:5432 --name some-postgres -e POSTGRES_USER=$USER -e POSTGRES_DB=$USER -d postgres
    ```
2.  Run PSQL to generate some data:
3.  
    ```bash
    docker exec -it some-postgres psql -U $USER
    psql (11.5 (Debian 11.5-1.pgdg90+1))
    Type "help" for help.

    postgres=# CREATE TABLE users (username VARCHAR, popularity INT);
    CREATE TABLE
    postgres=# INSERT INTO users (username, popularity) VALUES ('user1', 100);
    INSERT 0 1
    postgres=# INSERT INTO users (username, popularity) VALUES ('user2', 5);
    INSERT 0 1
    postgres=# INSERT INTO users (username, popularity) VALUES ('user3', 75);
    INSERT 0 1
    ```

When you're done, clear your local state by using the
`docker kill` command.

```bash
docker kill some-postgres && docker rm some-postgres
```

Create a JDBC Source Connector
------------------------------

Now that Postgres is up and running with a database for your user, you
can connect to it via KSQL. If you're using the default configurations,
KSQL connects automatically to your {{ site.kconnect }} cluster.
Otherwise, you must change the `ksql.connect.url` property to point to
your {{ site.kconnect }} deployment.

```sql
CREATE SOURCE CONNECTOR `jdbc-connector` WITH(
  "connector.class"='io.confluent.connect.jdbc.JdbcSourceConnector',
  "connection.url"='jdbc:postgresql://localhost:5432/YOUR_USERNAME',
  "mode"='bulk',
  "topic.prefix"='jdbc-',
  "key"='username');
```

Profit
------

At this point, data should automatically start flowing in from Postgres
to KSQL. Confirm this by running the following statement.

```sql
DESCRIBE CONNECTOR "jdbc-connector";
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

 Related Topics
----------------
 jdbc-users
----------------
```

Import this topic as a table to KSQL by using the following command.

```sql
CREATE TABLE JDBC_USERS WITH(value_format='AVRO', kafka_topic='jdbc-users');
```

Select everything from the topic to see how it gets auto populated:

```sql
SELECT * FROM JDBC_USERS EMIT CHANGES;
```

You output should resemble:

```
+------------------+------------------+------------------+------------------+
|ROWTIME           |ROWKEY            |USERNAME          |POPULARITY        |
+------------------+------------------+------------------+------------------+
|1566336783102     |user1             |user1             |100               |
|1566336783102     |user2             |user2             |5                 |
|1566336783102     |user3             |user3             |75                |
|1566336788106     |user1             |user1             |100               |
|1566336788106     |user2             |user2             |5                 |
|1566336788106     |user3             |user3             |75                |
```

Note that users are repeated multiple times. This means that `bulk` mode is
specified, which re-imports the entire database every time. Obviously, this
isn't appropriate for production. For more information on changelog capture,
see [Incremental Query
Modes](https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/index.html#incremental-query-modes).

Page last revised on: {{ git_revision_date }}
