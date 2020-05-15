# How to use connector management

## Context

You have external data stores that you want to read from and write to with ksqlDB, but you don’t want to write custom glue code to do it. ksqlDB is capable of using the vast ecosystem of Kafka Connect connectors and interacting with it through SQL syntax. This functionality is called "connector management".

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

Before you can use connector management, you need to decide what mode you want to run it in. ksqlDB can run connectors in two different modes: **embedded** or **external**. This controls how and where the connectors are executed. The way in which you configure ksqlDB's server determines which mode it will use. All nodes in a single ksqlDB cluster must use the same mode.

Regardless of which mode you use, the syntax to create and use connectors is the same.

## Embedded mode

In embedded mode, ksqlDB runs connectors directly on its servers. This is convenient because it reduces the number of moving parts that you need to manage in your infrastructure. Embedded mode is highly useful for development, testing, and production workloads that have light/moderate data volumes. Use this mode when you don't need to scale your ingest/egress capacity independently from your processing capacity. When you use embedded mode, ksqlDB server is actually running a Kafka Connect server in distributed mode.

### Dowloading connectors

Before you can use a connector, you need to download it prior to starting ksqlDB. A downloaded connector package is essentially a set of jars that contain the code for interacting with the target data store.

The easiest way to download a connector is to use `confluent-hub`, a utility program distributed Confluent. As a convenience, `confluent-hub` is available in ksqlDB's Docker image, which you may want to alias. For example, to obtain the Voluble connector, run the following.

```
docker run --rm -v $PWD/confluent-hub-components:/share/confluent-hub-components confluentinc/ksqldb-server:{{ site.release }} confluent-hub install --no-prompt mdrogalis/voluble:0.3.0
```

After running this command, you should have a directory named `confluent-hub-components` which contains the Voluble jars. If you are running in clustered mode, you must install the connector on every server.

When you have all the connectors that you would like to use, configure ksqlDB to find them in the next step. ksqlDB cannot add new connectors without restarting its servers.

### Configuring ksqlDB

You control whether or not ksqlDB uses embedded mode by the environment variables that you supply to ksqlDB. If any Connect related environment variables are present (variables prefixed with `KSQL_CONNECT_`), ksqlDB will use those and apply them to the embedded Connect server. Although embedded mode eases the operational burden of running a full Kafka Connector cluster, it doesn't dilute Connect's power. Any property that can be configured for a regular Kafka Connect cluster can also be configured for embedded mode.

There are a number of environment variables that you must set to have a valid Connect setup. Refer to Kafka Connect's documentation to learn what the right properties to set are. One critical property is `KSQL_CONNECT_PLUGIN_PATH`, which specifies the path to find the connector jars. Use a volume to mount your connector jars from your host into the container.

To get started, here is a Docker Compose example a server configured for embedded mode. Any connectors installed on your host at `confluent-hub-components` will be loaded. Place this in a file named `docker-compose.yml`:

```yaml
---
version: '2'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:5.5.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-enterprise-kafka:5.5.0
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
    image: confluentinc/cp-schema-registry:5.5.0
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
    image: confluentinc/ksqldb-server:0.9.0
    hostname: ksqldb-server
    container_name: ksqldb-server
    depends_on:
      - broker
      - schema-registry
    ports:
      - "8088:8088"
    volumes:
      - "./confluent-hub-components/debezium-debezium-connector-mysql:/usr/share/kafka/plugins/debezium-mysql"
    environment:
      KSQL_LISTENERS: "http://0.0.0.0:8088"
      KSQL_BOOTSTRAP_SERVERS: "broker:9092"
      KSQL_KSQL_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
      # Configuration to embed Kafka Connect support.
      KSQL_CONNECT_GROUP_ID: "ksql-connect-cluster"
      KSQL_CONNECT_BOOTSTRAP_SERVERS: "broker:9092"
      KSQL_CONNECT_KEY_CONVERTER: "io.confluent.connect.avro.AvroConverter"
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
    image: confluentinc/ksqldb-cli:0.9.0
    container_name: ksqldb-cli
    depends_on:
      - broker
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true
```

Bring up the stack with:

```
docker-compose up
```

### Launching a connector

Now that ksqlDB has a connector and is configured to run it in embedded mode, you can launch it. Start by running ksqlDB's CLI:

```
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```





- Launching a connector
- Stopping it
- Logging

## External mode

In external mode, ksqlDB communicates with an external Kafka Connect cluster. It's able to create and destroy connectors as needed. Use external mode when you have high volumes of input and output.

- List of connectors. Explanation of KConnect