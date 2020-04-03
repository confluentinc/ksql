What is it?
----------

A streaming ETL pipeline (sometimes called a “streaming data pipeline”) is a set of software services that ingests events, transforms them, and loads them into destination storage systems. It’s often the case that you have data in one place and want to move it to another as soon as you receive it, but you need to make some changes to the data as you transfer it. Maybe you need to do something simple, like transform the events to strip out any personally identifiable information. Sometimes you may need to do something more complex, like enrich the events by joining it with data from another system. Or perhaps you want to pre-aggregate the events to reduce how much data you send to the downstream systems. A streaming ETL pipeline lets you stream events between arbitrary sources and sinks helps you make changes to the data while it’s in-flight.

<< img >>

Why ksqlDB
----------

One way you might do this would be to capture the changelogs of upstream Postgres and MongoDB databases. The changelog can be stored in Kafka where a series of deployed programs transform, aggregate, and join the data together. The data can finally be streamed out to ElasticSearch for indexing. Many people build this sort of architecture, but could it be made simpler?

<< img >>

Using ksqlDB, you can run any Kafka Connect connector by embedding them in ksqlDB's servers. You can transform, join, and aggregate all of your streams together using a coherent, powerful SQL language. This gives you a slender architecture for managing the end-to-end flow of your data pipeline.

Implement it
------------

### Get the connectors

```
docker run --rm -v $PWD/confluent-hub-components:/share/confluent-hub-components confluentinc/ksqldb-server:0.8.1 confluent-hub install --no-prompt debezium/debezium-connector-postgresql:1.1.0
```

```
docker run --rm -v $PWD/confluent-hub-components:/share/confluent-hub-components confluentinc/ksqldb-server:0.8.1 confluent-hub install --no-prompt debezium/debezium-connector-mongodb:1.1.0
```

```
docker run --rm -v $PWD/confluent-hub-components:/share/confluent-hub-components confluentinc/ksqldb-server:0.8.1 confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:5.4.1
```

### Start the stack

```yaml
---
version: '2'

services:
  mongo:
    image: mongo:4.2.5
    hostname: mongo
    container_name: mongo
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_ROOT_USERNAME: mongo-user
      MONGO_INITDB_ROOT_PASSWORD: mongo-pw
      MONGO_REPLICA_SET_NAME: my-replica-set
    command: --replSet my-replica-set --bind_ip_all

  postgres:
    image: debezium/postgres:12
    hostname: postgres
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: postgres-user
      POSTGRES_PASSWORD: postgres-pw
      POSTGRES_DB: customers
    volumes:
      - ./postgres/custom-config.conf:/etc/postgresql/postgresql.conf
    command: postgres -c config_file=/etc/postgresql/postgresql.conf

  elastic:
    image: elasticsearch:7.6.2
    hostname: elastic
    container_name: elastic
    ports:
      - "9200:9200"
      - "9300:9300"
    environment:
      discovery.type: single-node

  zookeeper:
    image: confluentinc/cp-zookeeper:5.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-enterprise-kafka:5.4.0
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
    image: confluentinc/cp-schema-registry:5.4.1
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
    image: confluentinc/ksqldb-server:0.8.0
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
    image: confluentinc/ksqldb-cli:0.8.0
    container_name: ksqldb-cli
    depends_on:
      - broker
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true
```

### Create the customers table in Postgres

```
docker exec -it postgres /bin/bash
```

```
psql -U postgres-user customers
```

```sql
CREATE TABLE customers (id TEXT PRIMARY KEY, name TEXT, age INT);
```

```sql
INSERT INTO customers (id, name, age) VALUES ('5', 'fred', 34);
INSERT INTO customers (id, name, age) VALUES ('7', 'sue', 25);
INSERT INTO customers (id, name, age) VALUES ('2', 'bill', 51);
```

### Configure MongoDB for Debezium

```
docker exec -it mongo /bin/bash
```


```
mongo -u mongo-user -p mongo-pw admin
```

```javascript
rs.initiate()
```

```
use config
```

```javascript
db.createRole({
    role: "dbz-role",
    privileges: [
        {
            resource: { db: "config", collection: "system.sessions" },
            actions: [ "find", "update", "insert", "remove" ]
        }
    ],
    roles: [
       { role: "dbOwner", db: "config" },
       { role: "dbAdmin", db: "config" },
       { role: "readWrite", db: "config" }
    ]
})
```

```
use admin
```

```javascript
db.createUser({                                                                                                                 
  "user" : "dbz-user",
  "pwd": "dbz-pw",
  "roles" : [
    {
      "role" : "root",
      "db" : "admin"
    },
    {
      "role" : "readWrite",
      "db" : "logistics"
    },
    {
      "role" : "dbz-role",
      "db" : "config"
    }
  ]
})
```

```
exit
```

### Create the logistics collections in MongoDB

```
mongo -u dbz-user -p dbz-pw --authenticationDatabase admin logistics
```

```javascript
db.createCollection("orders")
```

```javascript
db.createCollection("shipments")
```

```javascript
db.orders.insert({"customer_id": "2", "order_id": "13", "price": 50.50, "currency": "usd", "ts": "2020-04-03T11:20:00"})
db.orders.insert({"customer_id": "7", "order_id": "29", "price": 15.00, "currency": "aud", "ts": "2020-04-02T12:36:00"})
db.orders.insert({"customer_id": "5", "order_id": "17", "price": 25.25, "currency": "eur", "ts": "2020-04-02T17:22:00"})
db.orders.insert({"customer_id": "5", "order_id": "15", "price": 13.75, "currency": "usd", "ts": "2020-04-03T02:55:00"})
db.orders.insert({"customer_id": "7", "order_id": "22", "price": 29.71, "currency": "aud", "ts": "2020-04-04T00:12:00"})
```

```javascript
db.shipments.insert({"order_id": "17", "shipment_id": "75", "origin": "texas", "ts": "2020-04-04T19:20:00"})
db.shipments.insert({"order_id": "22", "shipment_id": "71", "origin": "iowa", "ts": "2020-04-04T12:25:00"})
db.shipments.insert({"order_id": "29", "shipment_id": "89", "origin": "california", "ts": "2020-04-05T13:21:00"})
db.shipments.insert({"order_id": "13", "shipment_id": "92", "origin": "maine", "ts": "2020-04-04T06:13:00"})
db.shipments.insert({"order_id": "15", "shipment_id": "95", "origin": "florida", "ts": "2020-04-04T01:13:00"})
```


### Start the Postgres and MongoDB Debezium source connectors

```sql
CREATE SOURCE CONNECTOR customers_reader WITH (
    'connector.class' = 'io.debezium.connector.postgresql.PostgresConnector',
    'database.hostname' = 'postgres',
    'database.port' = '5432',
    'database.user' = 'postgres-user',
    'database.password' = 'postgres-pw',
    'database.dbname' = 'customers',
    'database.server.name' = 'customers',
    'database.history.kafka.bootstrap.servers' = 'broker:9092',
    'database.history.kafka.topic' = 'customers',
    'table.whitelist' = 'public.customers',
    'transforms' = 'unwrap',
    'transforms.unwrap.type' = 'io.debezium.transforms.ExtractNewRecordState',
    'transforms.unwrap.drop.tombstones' = 'false',
    'transforms.unwrap.delete.handling.mode' = 'rewrite'
);
```

```sql
CREATE SOURCE CONNECTOR logistics_reader WITH (
    'connector.class' = 'io.debezium.connector.mongodb.MongoDbConnector',
    'mongodb.hosts' = 'mongo:27017',
    'mongodb.name' = 'my-replica-set',
    'mongodb.authsource' = 'admin',
    'mongodb.user' = 'dbz-user',
    'mongodb.password' = 'dbz-pw',
    'database.history.kafka.bootstrap.servers' = 'broker:9092',
    'database.history.kafka.topic' = 'logistics',
    'collection.whitelist' = 'logistics.*',
    'transforms' = 'unwrap',
    'transforms.unwrap.type' = 'io.debezium.connector.mongodb.transforms.ExtractNewDocumentState',
    'transforms.unwrap.drop.tombstones' = 'false',
    'transforms.unwrap.delete.handling.mode' = 'drop',
    'transforms.unwrap.operation.header' = 'true'
);
```

### Create the ksqlDB source streams

```sql
SET 'auto.offset.reset' = 'earliest';
```

```sql
CREATE STREAM customers WITH (
    kafka_topic = 'customers.public.customers',
    value_format = 'avro'
);
```

```sql
CREATE STREAM orders WITH (
    kafka_topic = 'my-replica-set.logistics.orders',
    value_format = 'avro',
    timestamp = 'ts',
    timestamp_format = 'yyyy-MM-dd''T''HH:mm:ss'
);
```

```sql
CREATE STREAM shipments WITH (
    kafka_topic = 'my-replica-set.logistics.shipments',
    value_format = 'avro',
    timestamp = 'ts',
    timestamp_format = 'yyyy-MM-dd''T''HH:mm:ss'
);
```

### Join the streams together

```sql
CREATE TABLE customers_by_key AS
    SELECT id,
           latest_by_offset(name) AS name,
           latest_by_offset(age) AS age
    FROM customers
    GROUP BY id
    EMIT CHANGES;
```

```sql
CREATE STREAM enriched_orders AS
    SELECT o.order_id,
           o.price,
           o.currency,
           c.id AS customer_id,
           c.name AS customer_name,
           c.age AS customer_age
    FROM orders AS o
    LEFT JOIN customers_by_key c
    ON o.customer_id = c.ROWKEY
    EMIT CHANGES;
```

```sql
CREATE STREAM shipped_orders WITH (
    kafka_topic = 'shipped_orders'
) AS
    SELECT o.order_id,
           s.shipment_id,
           o.customer_id,
           o.customer_name,
           o.customer_age,
           s.origin,
           o.price,
           o.currency
    FROM enriched_orders AS o
    INNER JOIN shipments s
    WITHIN 7 DAYS
    ON s.order_id = o.order_id
    EMIT CHANGES;
```

### Start the Elasticsearch sink connector

```sql
CREATE SINK CONNECTOR enriched_writer WITH (
    'connector.class' = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
    'connection.url' = 'http://elastic:9200',
    'type.name' = 'kafka-connect',
    'topics' = 'shipped_orders'
);
```

```
curl http://localhost:9200/shipped_orders/_search?pretty
```

### Running this in production
