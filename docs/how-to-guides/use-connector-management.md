---
layout: page
title: How to use connector management
tagline: Use connectors to get external data for ksqlDB apps
description: Use SQL syntax to create connectors in your ksqlDB application 
keywords: connector
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/how-to-guides/use-connector-management.html';
</script>

# How to use connector management

## Context

You have external data stores that you want to read from and write to with ksqlDB, but you don’t want to write custom glue code to do it. ksqlDB is capable of using the [vast ecosystem](https://www.confluent.io/hub/) of [{{ site.kconnectlong }}](https://kafka.apache.org/documentation/#connect) connectors through its SQL syntax. This functionality is called *connector management*.

## In action

```sql
CREATE SOURCE CONNECTOR s WITH (
  'connector.class' = 'io.mdrogalis.voluble.VolubleSourceConnector',

  'genkp.people.with' = '#{Internet.uuid}',
  'genv.people.name.with' = '#{Name.full_name}',
  'genv.people.creditCardNumber.with' = '#{Finance.credit_card}',

  'global.throttle.ms' = '500'
);
```

## Modes

Before you can use connector management, you need to decide what mode you want to run connectors in. ksqlDB can run connectors in two different modes: **embedded** or **external**. This controls how and where the connectors are executed. The way in which you configure ksqlDB's server determines which mode it will use. All nodes in a single ksqlDB cluster must use the same mode.

Regardless of which mode you use, the syntax to create and use connectors is the same.

## Embedded mode

In embedded mode, ksqlDB runs connectors directly on its servers. This is convenient because it reduces the number of moving parts that you need to manage in your infrastructure. Embedded mode is highly useful for development, testing, and production workloads that have light/moderate data volumes. Use this mode when you don't need to scale your ingest/egress capacity independently from your processing capacity. When you use embedded mode, ksqlDB server is actually running a {{ site.kconnectlong }} server in distributed mode.

### Downloading connectors

Before you can use an embedded connector, you need to download it prior to starting ksqlDB. A downloaded connector package is essentially a set of jars that contain the code for interacting with the target data store.

The easiest way to download a connector is to use [`confluent-hub`](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html), a utility program distributed by Confluent.

Create a directory for your connectors:

```
mkdir confluent-hub-components
```

Run the following command to get the [Voluble](https://github.com/MichaelDrogalis/voluble) data generator connector:

```
confluent-hub install --component-dir confluent-hub-components --no-prompt mdrogalis/voluble:{{ site.voluble_version }}
```

After running this command, `confluent-hub-components` should contain the Voluble jars. If you are running in clustered mode, you must install the connector on every server.

When you have all the connectors that you need, configure ksqlDB to find them.

!!! important
    You must restart all of the ksqlDB servers to finish installing the new connectors.

### Configuring ksqlDB

You control whether ksqlDB uses embedded mode by supplying the server configuration property `ksql.connect.worker.config` with the path to a {{ site.kconnectlong }} configuration file. Although embedded mode eases the operational burden of running a full {{ site.kconnectlong }} cluster, it doesn't dilute {{ site.kconnectlong }}'s power. Any property that you can configure a regular {{ site.kconnectlong }} cluster with can also be applied to embedded mode.

There are a number of properties that you must set to have a valid {{ site.kconnect }} setup. Refer to the [Kafka Connect documentation](https://docs.confluent.io/current/connect/index.html) to learn about the right properties to set. One critical property is `ksql.connect.plugin.path`, which specifies the path to find the connector jars. If you're using Docker, use a volume to mount your connector jars from your host into the container.

!!! note
    If you're deploying with Docker, you can skip setting `ksql.connect.worker.config`. ksqlDB will look for environment variables prefixed with `KSQL_CONNECT_`. If it finds any, it will remove the `KSQL_` prefix and place them into a {{ site.kconnect }} configuration file. Embedded mode will use that configuration file. This is a convenience to avoid creating and mounting a separate configuration file.

To get started, here is a Docker Compose example with a server configured for embedded mode. All `KSQL_` environment variables are converted automatically to server configuration properties. Any connectors installed on your host at `confluent-hub-components` are loaded. Save this in a file named `docker-compose.yml`:

```yaml
---
version: '2'

services:
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
    image: confluentinc/cp-enterprise-kafka:{{ site.cprelease }}
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
      SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL: 'zookeeper:2181'

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
      - "./confluent-hub-components:/usr/share/kafka/plugins"
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
      KSQL_CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
      KSQL_CONNECT_CONFIG_STORAGE_TOPIC: "ksql-connect-configs"
      KSQL_CONNECT_OFFSET_STORAGE_TOPIC: "ksql-connect-offsets"
      KSQL_CONNECT_STATUS_STORAGE_TOPIC: "ksql-connect-statuses"
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

Bring up the stack with:

```bash
docker-compose up
```

### Launching a connector

Now that ksqlDB has a connector and is configured to run it in embedded mode, you can launch it. Start by running ksqlDB's CLI:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

Starting a connector is as simple as giving it a name and properties. In this example, you launch the Voluble connector to source random events into a {{ site.ak }} topic. Run the following SQL statement:

```sql
CREATE SOURCE CONNECTOR s WITH (
  'connector.class' = 'io.mdrogalis.voluble.VolubleSourceConnector',

  'genkp.people.with' = '#{Internet.uuid}',
  'genv.people.name.with' = '#{Name.full_name}',
  'genv.people.creditCardNumber.with' = '#{Finance.credit_card}',

  'global.throttle.ms' = '500'
);
```

Here is what this ksqlDB statement does:

- ksqlDB interacts with {{ site.kconnectlong }} to create a new source connector named `s`.
- {{ site.kconnectlong }} infers that `s` is a Voluble connector because of the value of `connector.class`. {{ site.kconnectlong }} searches its plugin path to find a connector that matches the specified class.
- ksqlDB passes the remaining properties directly to the Voluble connector so that it can configure itself.
- Voluble publishes a new event to topic `people` every `500` milliseconds with a UUID key and a map value of two keys, `name` and `creditCardNumber`.

The properties are the same that you would pass to a connector if it was running in a dedicated {{ site.kconnect }} cluster. You can pass any properties that the connector or {{ site.kconnectlong }} respects, like `max.tasks` to scale the number of instances of the connector.

Check that the connector is working by printing the contents of the `people` topic, which connector `s` created.

```sql
PRINT 'people' FROM BEGINNING;
```

Because the data is random, your output should look roughly like the following:

```
Key format: HOPPING(KAFKA_STRING) or TUMBLING(KAFKA_STRING) or KAFKA_STRING
Value format: AVRO or KAFKA_STRING
rowtime: 2020/05/18 17:03:38.020 Z, key: [8a9f5f18-f389-480e-9022-4fa0@7162241151841559604/-], value: {"name": "Robert Macejkovic", "creditCardNumber": "4753792478828"}
rowtime: 2020/05/18 17:03:38.023 Z, key: [96e3c6ff-60e2-4985-b962-4278@7365413101558183730/-], value: {"name": "Evelyne Schroeder", "creditCardNumber": "3689-911575-9931"}
rowtime: 2020/05/18 17:03:38.524 Z, key: [c865dd33-f854-4ad6-a95f-a9ee@7147828756964729958/-], value: {"name": "Barbar Roberts", "creditCardNumber": "6565-5340-0407-5224"}
rowtime: 2020/05/18 17:03:39.023 Z, key: [d29bb1e9-a8b0-4bdd-a76b-6fc1@7004895543925224502/-], value: {"name": "Rosetta Swift", "creditCardNumber": "5019-5129-1138-1079"}
rowtime: 2020/05/18 17:03:39.524 Z, key: [c7d74a03-ff21-4dd3-a60c-566d@7089291673502049328/-], value: {"name": "Amado Leuschke", "creditCardNumber": "6771-8942-4365-4019"}
```

When you're done, you can drop the connector by running:

```sql
DROP CONNECTOR s;
```

You can confirm that the connector is no longer running by looking at the output of `SHOW CONNECTORS;`.

### Logging

Embedded {{ site.kconnectlong }} logs messages inline with ksqlDB's server's log messages. View them by running the following command:

```bash
docker logs -f ksqldb-server
```

### Introspecting embedded mode

Sometimes you might need a little more power to introspect how your connectors are behaving by interacting directly with the embedded {{ site.kconnectlong }} server. First, notice that ksqlDB is really just wrapping a regular {{ site.kconnectlong }} server. You can curl it and interact with its [REST API](https://docs.confluent.io/current/connect/references/restapi.html) just like any other {{ site.kconnect }} server.

```bash
docker exec -it ksqldb-server curl http://localhost:8083/
```

Your output should resemble:

```json
{"version":"5.5.0-ccs","commit":"785a156634af5f7e","kafka_cluster_id":"bfz7rsyJRtOx5fs-2l4W4A"}
```

This can be really useful if you're having trouble getting a connector to load or need more insight into how connector tasks are behaving.

## External mode

In external mode, ksqlDB communicates with an external {{ site.kconnectlong }} cluster. It's able to create and destroy connectors as needed. Use external mode when you have high volumes of input and output, or need to scale your ingest/egress capacity independently from your processing capacity.

External mode essentially works the same way as embedded mode, except connectors run outside of ksqlDB's servers. All that is needed is to configure ksqlDB server with the `ksql.connect.url` property, indicating the address of the {{ site.kconnect }} server. Beyond that, you can manage connectors exactly as you would in embedded mode. No other configuration is needed.

This guide omits an example of setting up an external {{ site.kconnectlong }} cluster. Many great examples, [like Apache Kafka's](https://kafka.apache.org/documentation/#connect), have already been published.

## Tear down the stack

When you're done, tear down the stack by running:

```bash
docker-compose down
```
