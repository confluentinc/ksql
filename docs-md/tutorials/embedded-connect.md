---
layout: page
title: ksqlDB with Embedded Connect
tagline: Run Kafka Connect embedded within ksqlDB
description: Learn how to use ksqlDB with embedded Connect to integrate with external data sources and sinks
keywords: ksqlDB, connect, PostgreSQL, jdbc
---

Overview
==============

This tutorial will demonstrate how to integrate ksqlDB with an external data source to power a simple ride sharing app. Our external source will be a PostgreSQL database containing relatively static data describing each driver’s vehicle. By combining this human-friendly static data with a continuous stream of computer-friendly driver and rider location events, we derive an enriched output stream that the ride sharing app may use to facilitate a rendezvous in real time.

1. Get ksqlDB
--------------

Since ksqlDB runs natively on Apache Kafka®, you'll need to have a Kafka installation running that ksqlDB is configured to use. The docker-compose files to the right will run everything for you via Docker, including ksqlDB running [Kafka Connect](https://docs.confluent.io/current/connect/index.html) in embedded mode. Embedded Connect enables you to leverage the power of Connect without having to manage a separate Connect cluster--ksqlDB will manage one for you. Additionally, we will be using PostgreSQL as an external datastore to integrate with ksqlDB.

Copy and paste the below ``docker-compose`` content into a file named ``docker-compose.yml`` in an empty local working directory. We will create and add a number of other files to this directory during this tutorial.

```bash
---
version: '2'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:5.3.1
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-enterprise-kafka:5.3.1
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

  ksqldb-server:
    image: confluentinc/ksqldb-server:0.6.0
    hostname: ksqldb-server
    container_name: ksqldb-server
    depends_on:
      - broker
    ports:
      - "8088:8088"
    environment:
      KSQL_LISTENERS: http://0.0.0.0:8088
      KSQL_BOOTSTRAP_SERVERS: broker:9092
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
      KSQL_KSQL_CONNECT_WORKER_CONFIG: "/connect/connect.properties"
    volumes:
      - ${PWD}/confluentinc-kafka-connect-jdbc-5.3.1:/usr/share/kafka/plugins/jdbc
      - ${PWD}/connect.properties:/connect/connect.properties

  ksqldb-cli:
    image: confluentinc/ksqldb-cli:0.6.0
    container_name: ksqldb-cli
    depends_on:
      - broker
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true
  
  postgres:
    image: postgres:12
    hostname: postgres
    container_name: postgres
    ports:
      - "5432:5432"
```

2. Get the JDBC connector
-------------------------

[Download the JDBC connector](https://www.confluent.io/hub/confluentinc/kafka-connect-jdbc) to your local working directory. Next, unzip the downloaded archive:

```bash
unzip confluentinc-kafka-connect-jdbc-5.3.1.zip
```

3. Configure Connect
--------------------

In order to tell ksqlDB to run Connect in embedded mode, we must point ksqlDB to a separate Connect configuration file. In our docker-compose file, this is done via the ``KSQL_KSQL_CONNECT_WORKER_CONFIG`` environment variable. From within your local working directory, run this command to generate the Connect configuration file:

```bash
cat << EOF > ./connect.properties
bootstrap.servers=broker:9092
plugin.path=/usr/share/kafka/plugins
group.id=ksql-connect-cluster
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
internal.key.converter.schemas.enable=false
config.storage.topic=ksql-connect-configs
offset.storage.topic=ksql-connect-offsets
status.storage.topic=ksql-connect-statuses
config.storage.replication.factor=1
offset.storage.replication.factor=1
status.storage.replication.factor=1
EOF
```

4. Start ksqlDB and PostgreSQL
------------------------------

From a directory containing the ``docker-compose.yml`` file created in the first step, run this command in order to start all services in the correct order:

```bash
docker-compose up
```

5. Connect to PostgreSQL
------------------------

Establish an interactive session with PostgreSQL by running this command:

```bash
docker exec -it postgres psql -U postgres
```

6. Populate PostgreSQL with vehicle/driver data
-----------------------------------------------

From within the session opened in the previous step, run these SQL statements to set up our driver data. We will ultimately join this PostgreSQL data with our event streams in ksqlDB:

```sql
CREATE TABLE drivers (
  driver_id integer PRIMARY KEY,
  make text,
  model text,
  year integer,
  license_plate text,
  rating float
);

INSERT INTO drivers (driver_id, make, model, year, license_plate, rating) VALUES
  (0, 'Toyota', 'Prius',   2019, 'KAFKA-1', 5.00),
  (1, 'Kia',    'Sorento', 2017, 'STREAMS', 4.89),
  (2, 'Tesla',  'Model S', 2019, 'CNFLNT',  4.92),
  (3, 'Toyota', 'Camry',   2018, 'ILVKSQL', 4.85);
```

7. Start ksqlDB's interactive CLI
---------------------------------

ksqlDB runs as a server which clients connect to in order to issue queries.

Run this command to connect to the ksqlDB server and enter an interactive command-line interface (CLI) session:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

8. Create source connector
--------------------------

Next, we are going to make our PostgreSQL data accessible to ksqlDB by creating a *source* connector. From within the ksqlDB session opened in the previous step, run this command:

```sql
CREATE SOURCE CONNECTOR jdbc_source WITH (
  'connector.class'          = 'io.confluent.connect.jdbc.JdbcSourceConnector',
  'connection.url'           = 'jdbc:postgresql://postgres:5432/postgres',
  'connection.user'          = 'postgres',
  'topic.prefix'             = 'jdbc_',
  'table.whitelist'          = 'drivers',
  'mode'                     = 'incrementing',
  'numeric.mapping'          = 'best_fit',
  'incrementing.column.name' = 'driver_id',
  'key'                      = 'driver_id',
  'value.converter.schemas.enable' = false);

```

Once the source connector is created, it will import any PostgreSQL tables matching the specified ``table.whitelist``. Tables are imported via Kafka topics, one topic per imported table. Once these topics are created, we may interact with them just like any other Kafka topic used by ksqlDB.

9. View imported topic
----------------------

From within your ksqlDB CLI session, run this command to verify that the drivers table has been imported. Since we’ve specified ``jdbc_`` as our topic prefix, you should see a ``jdbc_drivers`` topic in the output of this command:

```bash
SHOW TOPICS;
```

10. Create drivers table in ksqlDB
----------------------------------

While our driver data is now integrated as a Kafka topic, we’ll want to create a ksqlDB table over this topic in order to begin referencing it from any ksqlDB queries. Streams and tables in ksqlDB essentially associate a schema with a Kafka topic, breaking each message in the topic into strongly typed columns:

```sql
CREATE TABLE drivers (
  driver_id INTEGER,
  make STRING,
  model STRING,
  year INTEGER,
  license_plate STRING,
  rating DOUBLE
)
WITH (kafka_topic='jdbc_drivers', value_format='json', partitions=1, key='driver_id');
```

Tables in ksqlDB support update semantics, where each message in the underlying topic represents a row in the table. For messages in the topic with the same key, the latest message associated with a given key represents the latest value for the corresponding row in the table.

11. Create driverLocations and riderLocations streams
-----------------------------------------------------

Next we’ll create streams to encapsulate location pings sent every few seconds by drivers’ and riders’ phones. In contrast to tables, ksqlDB streams are append-only collections of events, and therefore suitable for a continuous stream of location updates.

```sql
CREATE STREAM driverLocations (
  driver_id INTEGER,
  latitude DOUBLE,
  longitude DOUBLE,
  speed DOUBLE
)
WITH (kafka_topic='driver_locations', value_format='json', partitions=1, key='driver_id');

CREATE STREAM riderLocations (
  driver_id INTEGER,
  latitude DOUBLE,
  longitude DOUBLE
)
WITH (kafka_topic='rider_locations', value_format='json', partitions=1, key='driver_id');
```

12. Enrich driverLocations stream by joining with PostgreSQL data
-----------------------------------------------------------------

The ``driverLocations`` stream has a relatively compact schema, and doesn’t contain much data that a human would find particularly useful. We’d therefore like to *enrich* our stream of driver location events by joining them with the human-friendly vehicle information stored in our PostgreSQL database. This enriched data may then be presented by the rider’s mobile application, ultimately helping the rider to safely identify the driver’s vehicle.

We can easily achieve this result using ksqlDB by simply joining the ``driverLocations`` stream with the ``drivers`` table stored in PostgreSQL:

```sql
CREATE STREAM enrichedDriverLocations AS
  SELECT
    dl.driver_id       AS driver_id,
    dl.latitude        AS latitude,
    dl.longitude       AS longitude,
    dl.speed           AS speed,
    jdbc.make          AS make,
    jdbc.model         AS model,
    jdbc.year          AS year,
    jdbc.license_plate AS license_plate,
    jdbc.rating AS rating
  FROM driverLocations dl JOIN drivers jdbc
    ON dl.driver_id = jdbc.driver_id
  EMIT CHANGES; 
```

13. Create rendezvous stream
----------------------------

Putting all of this together, we will now create a final stream that the ride sharing app can use to facilitate a driver-rider rendezvous in real time. This stream is defined by a query that joins together rider and driver location updates, resulting in a contextualized output that the app may use to show the rider their driver’s position as the rider waits to be picked up.

Our rendezvous stream also includes human-friendly information describing the driver’s vehicle for the rider, and even computes (albeit naively) the driver’s estimated time of arrival (ETA) at the rider’s location:

```sql
CREATE STREAM rendezvous AS
  SELECT
    e.license_plate AS license_plate,
    e.make          AS make,
    e.model         AS model,
    e.year          AS year,
    e.latitude      AS vehicle_lat,
    e.longitude     AS vehicle_long,
    GEO_DISTANCE(e.latitude, e.longitude, r.latitude, r.longitude) / e.speed AS eta
  FROM enrichedDriverLocations e JOIN riderLocations r WITHIN 1 MINUTE
    ON e.driver_id = r.driver_id
  EMIT CHANGES;
```

14. Start two ksqlDB CLI sessions
---------------------------------

Run this command to twice to open two separate ksqlDB CLI sessions. Will we use both of these sessions in the steps to follow. Note that if you still have a CLI session open from a previous step, you may reuse that session.

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

15. Run a continuous query
--------------------------

We’re now going to run a continuous query over the rendezvous stream.

This is the first thing that may feel a bit unfamiliar to you, because the query will never return until it's terminated. It will perpetually push output rows to the client as events are written to the rendezvous stream. Leave the query running in your CLI session for now. It will begin producing output as soon as we write events into ksqlDB:

```sql
SELECT * FROM rendezvous EMIT CHANGES;
```

16. Write data to input streams
-------------------------------

Our continuous query is reading from the ``rendezvous`` stream, which takes its input from the ``enrichedDriverLocations`` and ``riderLocations`` streams. And ``enrichedDriverLocations`` takes its input from the ``driverLocations`` stream, so we'll need to write data into ``driverLocations`` and ``riderLocations`` before ``rendezvous`` will produce the joined output that our continuous query will read:

```sql
INSERT INTO driverLocations (driver_id, latitude, longitude, speed) VALUES (0, 37.3965, -122.0818, 23.2);
INSERT INTO riderLocations (driver_id, latitude, longitude) VALUES (0, 37.3952, -122.0813);

INSERT INTO driverLocations (driver_id, latitude, longitude, speed) VALUES (1, 37.7850, -122.40270, 12.0);
INSERT INTO riderLocations (driver_id, latitude, longitude) VALUES (1, 37.7887,-122.4074);

INSERT INTO driverLocations (driver_id, latitude, longitude, speed) VALUES (2, 37.7925, -122.4148, 11.2);
INSERT INTO riderLocations (driver_id, latitude, longitude) VALUES (2, 37.7876,-122.4235);

INSERT INTO driverLocations (driver_id, latitude, longitude, speed) VALUES (3, 37.4471, -122.1625, 14.7);
INSERT INTO riderLocations (driver_id, latitude, longitude) VALUES (3, 37.4442, -122.1658);
```

As soon as you start writing rows to the input streams, your continuous query from the previous step will begin producing joined output: the rider's location pings are joined with their inbound driver's location pings in real time, providing the rider with driver ETA, rating, as well as additional describing the driver's vehicle.

Next steps
-------------

This tutorial has demonstrated how to run ksqlDB in embedded Connect mode using Docker. We have used the JDBC connector to integrate ksqlDB with PostgreSQL data, but this is just one of many connectors that are available to help you integrate ksqlDB with external systems. Check out [Confluent Hub](https://www.confluent.io/hub/) to learn more about all of the various connectors that enable integration with a wide variety of external systems.

You may also want to take a look at our [examples](https://ksqldb.io/examples.html) to better understand how you can use ksqlDB for your specific workload.
