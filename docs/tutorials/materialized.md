---
layout: page
title: Materialized cache 
tagline: Learn how to create and query a set of materialized views by using ksqlDB
description: Use ksqlDB to create and query a set of materialized views about phone calls made to the call center
keywords: materialized view, cache, changelog
---

# Materialized cache

## What is it?

A materialized view, sometimes called a "[materialized cache](https://www.confluent.io/blog/build-materialized-cache-with-ksqldb/)", is an approach to precomputing the results of a query and storing them for fast read access. In contrast with a regular database query, which does all of its work at read-time, a materialized view does nearly all of its work at write-time. This is why materialized views can offer highly performant reads.

A standard way of building a materialized cache is to capture the changelog of a database and process it as a stream of events. This enables creating multiple distributed materializations that best suit each application's query patterns.

![hard](../img/mv-hard.png){: class="centered-img" style="width: 100%"}

One way you might do this is to capture the changelog of MySQL using the [Debezium](https://debezium.io) {{ site.ak }} connector. The changelog is stored in {{ site.ak }} and processed by a stream processor. As the materialization updates, it's updated in Redis so that applications can query the materializations. This can work, but is there a better way?

## Why ksqlDB?

Running all of the above systems is a lot to manage. In addition to your database, you end up managing clusters for {{ site.ak }}, connectors, the stream processor, and another data store. It's challenging to monitor, secure, and scale all of these systems as one. ksqlDB helps to consolidate this complexity by slimming the architecture down to two things: storage ({{ site.ak }}) and compute (ksqlDB).

![easy](../img/mv-easy.png){: class="centered-img" style="width: 70%"}

Using ksqlDB, you can run any {{ site.kconnectlong }} connector by embedding it in ksqlDB's servers. You can also directly query ksqlDB's tables of state, eliminating the need to sink your data to another data store. This gives you one mental model, in SQL, for managing your materialized views end-to-end.

## Implement it

Imagine that you work at a company with a call center. People frequently call in about purchasing a product, to ask for a refund, and other things. Because the volume of calls is rather high, it isn't practical to run queries over the database storing all the calls every time someone calls in.

This tutorial shows how to create and query a set of materialized views about phone calls made to the call center. It demonstrates capturing changes from a MySQL database, forwarding them into {{ site.ak }}, creating materialized views with ksqlDB, and querying them from your applications.

### Get the Debezium connector

To get started, download the Debezium connector to a fresh directory. The easiest way to do this is by using [confluent-hub](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html).

Create a directory for your components:

```
mkdir confluent-hub-components
```

And run:

```
confluent-hub install --component-dir confluent-hub-components --no-prompt debezium/debezium-connector-mysql:1.1.0
```

After running this, `confluent-hub-components` should have some jar files in it.

### Start the stack

To set up and launch the services in the stack, a few files need to be created first.

MySQL requires some custom configuration to play well with Debezium, so take care of this first. Debezium has dedicated [documentation](https://debezium.io/documentation/reference/1.1/connectors/mysql.html) if you're interested, but this guide covers just the essentials. Create a new file at `mysql/custom-config.cnf` with the following content:

```
[mysqld]
server-id                = 223344 
log_bin                  = mysql-bin 
binlog_format            = ROW 
binlog_row_image         = FULL 
expire_logs_days         = 10
gtid_mode                = ON
enforce_gtid_consistency = ON
```

This sets up MySQL's transaction log so that Debezium can watch for changes as they occur.

With this file in place, create a `docker-compose.yml` file that defines the services to launch:

```yaml
---
version: '2'

services:
  mysql:
    image: mysql:8.0.19
    hostname: mysql
    container_name: mysql
    ports:
      - "3306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: mysql-pw
      MYSQL_DATABASE: call-center
      MYSQL_USER: example-user
      MYSQL_PASSWORD: example-pw
    volumes:
      - "./mysql/custom-config.cnf:/etc/mysql/conf.d/custom-config.cnf"

  zookeeper:
    image: confluentinc/cp-zookeeper:{{ site.cprelease }}
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-kafka:{{ site.cprelease }}
    hostname: broker
    container_name: broker
    depends_on:
      - zookeeper
    ports:
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

  schema-registry:
    image: confluentinc/cp-schema-registry:{{ site.cprelease }}
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      - zookeeper
      - broker
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "PLAINTEXT://broker:9092"

  ksqldb-server:
    image: confluentinc/ksqldb-server:{{ site.ksqldbversion }}
    hostname: ksqldb-server
    container_name: ksqldb-server
    depends_on:
      - broker
      - schema-registry
    ports:
      - "8088:8088"
    volumes:
      - "./confluent-hub-components/:/usr/share/kafka/plugins/"
    environment:
      KSQL_LISTENERS: "http://0.0.0.0:8088"
      KSQL_BOOTSTRAP_SERVERS: "broker:9092"
      KSQL_KSQL_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
      # Configuration to embed Kafka Connect support.
      KSQL_CONNECT_GROUP_ID: "ksql-connect-cluster"
      KSQL_CONNECT_BOOTSTRAP_SERVERS: "broker:9092"
      KSQL_CONNECT_KEY_CONVERTER: "org.apache.kafka.connect.storage.StringConverter"
      KSQL_CONNECT_VALUE_CONVERTER: "io.confluent.connect.avro.AvroConverter"
      KSQL_CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_CONNECT_CONFIG_STORAGE_TOPIC: "_ksql-connect-configs"
      KSQL_CONNECT_OFFSET_STORAGE_TOPIC: "_ksql-connect-offsets"
      KSQL_CONNECT_STATUS_STORAGE_TOPIC: "_ksql-connect-statuses"
      KSQL_CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      KSQL_CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      KSQL_CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      KSQL_CONNECT_PLUGIN_PATH: "/usr/share/kafka/plugins"

  ksqldb-cli:
    image: confluentinc/ksqldb-cli:{{ site.ksqldbversion }}
    container_name: ksqldb-cli
    depends_on:
      - broker
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true
```

There are a few things to notice here. The MySQL image mounts the custom configuration file that you wrote. MySQL merges these configuration settings into its system-wide configuration. The environment variables you gave it also set up a blank database called `call-center` along with a user named `example-user` that can access it.

Also note that the ksqlDB server image mounts the `confluent-hub-components` directory, too. The jar files that you downloaded need to be on the classpath of ksqlDB when the server starts up.

Bring up the entire stack by running:

```
docker-compose up
```

### Configure MySQL for Debezium

MySQL requires just a bit more modification before it can work with Debezium. Debezium needs to connect to MySQL as a user that has a specific set of privileges to replicate its changelog. You already set up the `example-user` by default in the Docker Compose file. Now you just need to give it the right privileges. You can do this by logging in to the MySQL container:

```
docker exec -it mysql /bin/bash
```

And then logging into MySQL as root:

```
mysql -u root -p
```

The root password, as specified in the Docker Compose file, is `mysql-pw`.

For simplicity, this tutorial grants all privileges to `example-user` connecting from any host. In the real world, you'd want to manage your permissions much more tightly.

Grant the privileges for replication by executing the following statement at the MySQL prompt:

```sql
GRANT ALL PRIVILEGES ON *.* TO 'example-user' WITH GRANT OPTION;
ALTER USER 'example-user'@'%' IDENTIFIED WITH mysql_native_password BY 'example-pw';
FLUSH PRIVILEGES;
```

### Create the calls table in MySQL

Seed your blank database with some initial state. In the same MySQL CLI, switch into the `call-center` database:

```sql
USE call-center;
```

Create a table that represents phone calls that were made. Keep this table simple: the columns represent the name of the person calling, the reason that they called, and the duration in seconds of the call.

```sql
CREATE TABLE calls (name TEXT, reason TEXT, duration_seconds INT);
```

And now add some initial data. You'll add more later, but this will suffice for now:

```sql
INSERT INTO calls (name, reason, duration_seconds) VALUES ("michael", "purchase", 540);
INSERT INTO calls (name, reason, duration_seconds) VALUES ("michael", "help", 224);
INSERT INTO calls (name, reason, duration_seconds) VALUES ("colin", "help", 802);
INSERT INTO calls (name, reason, duration_seconds) VALUES ("derek", "purchase", 10204);
INSERT INTO calls (name, reason, duration_seconds) VALUES ("derek", "help", 600);
INSERT INTO calls (name, reason, duration_seconds) VALUES ("colin", "refund", 105);
INSERT INTO calls (name, reason, duration_seconds) VALUES ("michael", "help", 2030);
INSERT INTO calls (name, reason, duration_seconds) VALUES ("colin", "purchase", 800);
INSERT INTO calls (name, reason, duration_seconds) VALUES ("derek", "help", 2514);
INSERT INTO calls (name, reason, duration_seconds) VALUES ("derek", "refund", 325);
```

### Start the Debezium connector

With MySQL ready to go, connect to ksqlDB's server using its interactive CLI. Run the following command from your host:

```
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

Before you issue more commands, tell ksqlDB to start all queries from earliest point in each topic:

```sql
SET 'auto.offset.reset' = 'earliest';
```

Now you can connect to Debezium to stream MySQL's changelog into {{ site.ak }}. Invoke the following command in ksqlDB, which creates a Debezium source connector and writes all of its changes to {{ site.ak }} topics:

```sql
CREATE SOURCE CONNECTOR calls_reader WITH (
    'connector.class' = 'io.debezium.connector.mysql.MySqlConnector',
    'database.hostname' = 'mysql',
    'database.port' = '3306',
    'database.user' = 'example-user',
    'database.password' = 'example-pw',
    'database.allowPublicKeyRetrieval' = 'true',
    'database.server.id' = '184054',
    'database.server.name' = 'call-center-db',
    'database.whitelist' = 'call-center',
    'database.history.kafka.bootstrap.servers' = 'broker:9092',
    'database.history.kafka.topic' = 'call-center',
    'table.whitelist' = 'call-center.calls',
    'include.schema.changes' = 'false'
);
```

After a few seconds, it should create a topic named `call-center-db.call-center.calls`. Confirm that by running:

```sql
SHOW TOPICS;
```

Print the raw topic contents to make sure it captured the initial rows that you seeded the calls table with:

```sql
PRINT 'call-center-db.call-center.calls' FROM BEGINNING;
```

If nothing prints out, the connector probably failed to launch. You can check ksqlDB's logs with:

```
docker logs -f ksqldb-server
```

You can also show the status of the connector in the ksqlDB CLI with:

```
DESCRIBE CONNECTOR calls_reader;
```

### Create the ksqlDB calls stream

For ksqlDB to be able to use the topic that Debezium created, you must declare a stream over it. Because you configured {{ site.kconnectlong }} with {{ site.sr }}, you don't need to declare the schema of the data for the streams. It is simply inferred from the schema that Debezium writes with. Run the following at the ksqlDB CLI:

```sql
CREATE STREAM calls WITH (
    kafka_topic = 'call-center-db.call-center.calls',
    value_format = 'avro'
);
```

### Create the materialized views

A common situation in call centers is the need to know what the current caller has called about in the past. Create a simple materialized view that keeps track of the distinct number of reasons that a user called for, and what the last reason was that they called for, too. This gives you an idea of how many kinds of inquiries the caller has raised and also gives you context based on the last time they called.

You do this by declaring a table called `support_view`. Keeping track of the distinct number of reasons a caller raised is as simple as grouping by the user name, then aggregating with `count_distinct` over the `reason` value. Similarly, you can retain the last reason the person called for with the `latest_by_offset` aggregation.

Notice that Debezium writes events to the topic in the form of a map with "before" and "after" keys to make it clear what changed in each operation. That is why each column uses arrow syntax to drill into the nested `after` key.


In the ksqlDB CLI, run the following statement:

```sql
CREATE TABLE support_view AS
    SELECT after->name AS name,
           count_distinct(after->reason) AS distinct_reasons,
           latest_by_offset(after->reason) AS last_reason
    FROM calls
    GROUP BY after->name
    EMIT CHANGES;
```

You have your first materialized view in place. Now create one more.

It's useful to have an idea of the lifetime behavior of each caller. Rather than issuing a query over all the data every time there is a question about a caller, a materialized view makes it easy to update the answer incrementally as new information arrives over time. The following materialized view counts the total number of times each person has called and computes the total number of minutes spent on the phone with this person.

```sql
CREATE TABLE lifetime_view AS
    SELECT after->name AS name,
           count(after->reason) AS total_calls,
           (sum(after->duration_seconds) / 60) as minutes_engaged
    FROM calls
    GROUP BY after->name
    EMIT CHANGES;
```

### Query the materialized views

Now you can query our materialized views to look up the values for keys with low latency. How many reasons has Derek called for, and what was the last thing he called about? In the ksqlDB CLI, run the following statement:

```sql
SELECT name, distinct_reasons, last_reason
FROM support_view
WHERE name = 'derek';
```

Your output should resemble:

```
+---------+-------------------+------------+
|NAME     |DISTINCT_REASONS   |LAST_REASON |
+---------+-------------------+------------+
|derek    |3                  |refund      |
```

How many times has Michael called us, and how many minutes has he spent on the line?

```sql
SELECT name, total_calls, minutes_engaged
FROM lifetime_view
WHERE name = 'michael';
```

Your output should resemble:

```
+-----------+---------------+----------------+
|NAME       |TOTAL_CALLS    |MINUTES_ENGAGED |
+-----------+---------------+----------------+
|michael    |3              |46              |
```

Try inserting more rows into the MySQL prompt. Query ksqlDB and watch the results propagate in real-time.

### Tear down the stack

When you're done, tear down the stack by running:

```
docker-compose down
```

### Running this in production

In practice, you won't want to query your materialized views from the ksqlDB prompt.
It's much more useful to query them from within your applications. To do that, you can
use the [Java client for ksqlDB](../../developer-guide/ksqldb-clients/java-client/) or
submit queries to ksqlDB's servers through its [REST API](../../developer-guide/api/).

## Next steps

Want to learn more? Try another use case tutorial:

- [Streaming ETL pipeline](etl.md)
- [Event-driven microservice](event-driven-microservice.md)
