---
layout: page
title: ksqlDB with Embedded Connect
tagline: Run Kafka Connect embedded within ksqlDB
description: Learn how to use ksqlDB with embedded Connect to integrate with external data sources and sinks
keywords: ksqlDB, connect, PostgreSQL, jdbc
---

Overview
==============

This tutorial will demonstrate how to integrate ksqlDB with an external data
source to power a simple ride sharing app. Our external source will be a
PostgreSQL database containing relatively static data describing each driver’s
vehicle. By combining this human-friendly static data with a continuous stream
of computer-friendly driver and rider location events, we derive an enriched
output stream that the ride sharing app may use to facilitate a rendezvous in
real time.

When to use embedded Connect
------------------------------

ksqlDB natively integrates with {{ site.kconnect }} by either communicating
with an external {{ site.kconnect }} cluster or by running {{ site.kconnect }}
embedded within the ksqlDB server process. Each of these modes is best suited
for the following environments:

* **Embedded** - Suitable for development, testing, and simpler production
workloads at lower throughputs when there is no need to scale ksqlDB
independently of {{ site.kconnect }}.
* **External** - Suitable for all production workloads.

!!! note
	The {{ site.kconnect }} integration mode is a deployment configuration
	option. The {{ site.kconnect }} integration interface is identical for both
	modes, so your `CREATE SOURCE` and `CREATE SINK` statements are independent
	of the integration mode.

1. Get ksqlDB
--------------

Since ksqlDB runs natively on {{ site.aktm }}, you need a running {{ site.ak }}
installation that ksqlDB is configured to use. The following docker-compose
files run everything for you via Docker, including ksqlDB running
[Kafka Connect](https://docs.confluent.io/current/connect/index.html) in
embedded mode. Embedded Connect enables you to leverage the power of
{{ site.kconnect }} without having to manage a separate {{ site.kconnect }}
cluster, because ksqlDB manages one for you. Also, this tutorial use PostgreSQL
as an external datastore to integrate with ksqlDB.

In an empty local working directory, copy and paste the following
`docker-compose` content into a file named `docker-compose.yml`. You will
create and add a number of other files to this directory during this tutorial.

```yaml
---
version: '2'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-enterprise-kafka:latest
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

  ksqldb-server:
    image: confluentinc/ksqldb-server:{{ site.release }}
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
      KSQL_CONNECT_GROUP_ID: "ksql-connect-cluster"
      KSQL_CONNECT_BOOTSTRAP_SERVERS: broker:9092
      KSQL_CONNECT_KEY_CONVERTER: "org.apache.kafka.connect.storage.StringConverter"
      KSQL_CONNECT_VALUE_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
      KSQL_CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
      KSQL_CONNECT_CONFIG_STORAGE_TOPIC: "ksql-connect-configs"
      KSQL_CONNECT_OFFSET_STORAGE_TOPIC: "ksql-connect-offsets"
      KSQL_CONNECT_STATUS_STORAGE_TOPIC: "ksql-connect-statuses"
      KSQL_CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      KSQL_CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      KSQL_CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      KSQL_CONNECT_PLUGIN_PATH: "/usr/share/kafka/plugins"
    volumes:
      - ./confluent-hub-components/confluentinc-kafka-connect-jdbc:/usr/share/kafka/plugins/jdbc

  ksqldb-cli:
    image: confluentinc/ksqldb-cli:{{ site.release }}
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
    environment:
      POSTGRES_PASSWORD: password
```

2. Get the JDBC connector
-------------------------

The easiest way to download connectors for use in ksqlDB with embedded {{ site.kconnect }}
is via [Confluent Hub Client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html).

To download the JDBC connector, use the following command, ensuring that the `confluent-hub-components` directory exists first:

```bash
confluent-hub install --component-dir confluent-hub-components --no-prompt confluentinc/kafka-connect-jdbc:{{ site.cprelease }}
```
This command downloads the JDBC connector into the directory `./confluent-hub-components`.

3. Start ksqlDB and PostgreSQL
------------------------------

In the directory containing the `docker-compose.yml` file you created in the
first step, run the following command to start all services in the correct
order.

```bash
docker-compose up
```

4. Connect to PostgreSQL
------------------------

Run the following command to establish an interactive session with PostgreSQL.

```bash
docker exec -it postgres psql -U postgres
```

5. Populate PostgreSQL with vehicle/driver data
-----------------------------------------------

In the PostgreSQL session, run the following SQL statements to set up the
driver data. You will join this PostgreSQL data with event streams in ksqlDB.

```sql
CREATE TABLE driver_profiles (
  driver_id integer PRIMARY KEY,
  make text,
  model text,
  year integer,
  license_plate text,
  rating float
);

INSERT INTO driver_profiles (driver_id, make, model, year, license_plate, rating) VALUES
  (0, 'Toyota', 'Prius',   2019, 'KAFKA-1', 5.00),
  (1, 'Kia',    'Sorento', 2017, 'STREAMS', 4.89),
  (2, 'Tesla',  'Model S', 2019, 'CNFLNT',  4.92),
  (3, 'Toyota', 'Camry',   2018, 'ILVKSQL', 4.85);
```

6. Start ksqlDB's interactive CLI
---------------------------------

ksqlDB runs as a server which clients connect to in order to issue queries.

Run the following command to connect to the ksqlDB server and start an
interactive command-line interface (CLI) session.

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

7. Create source connector
--------------------------

Make your PostgreSQL data accessible to ksqlDB by creating a *source*
connector. In the ksqlDB CLI, run the following command.

```sql
CREATE SOURCE CONNECTOR jdbc_source WITH (
  'connector.class'          = 'io.confluent.connect.jdbc.JdbcSourceConnector',
  'connection.url'           = 'jdbc:postgresql://postgres:5432/postgres',
  'connection.user'          = 'postgres',
  'connection.password'      = 'password',
  'topic.prefix'             = 'jdbc_',
  'table.whitelist'          = 'driver_profiles',
  'mode'                     = 'incrementing',
  'numeric.mapping'          = 'best_fit',
  'incrementing.column.name' = 'driver_id',
  'key'                      = 'driver_id',
  'key.converter'            = 'org.apache.kafka.connect.converters.IntegerConverter');
```

When the source connector is created, it imports any PostgreSQL tables matching
the specified `table.whitelist`. Tables are imported via {{ site.ak }} topics,
with one topic per imported table. Once these topics are created, you can
interact with them just like any other {{ site.ak }} topic used by ksqlDB.

8. View imported topic
----------------------

In the ksqlDB CLI session, run the following command to verify that the
`driver_profiles` table has been imported as a Kafka topic. Because you specified `jdbc_` as the topic
prefix, you should see a `jdbc_driver_profiles` topic in the output.

```bash
SHOW TOPICS;
```

9. Create drivers table in ksqlDB
----------------------------------

The driver data is now integrated as a {{ site.ak }} topic, but you need to
create a ksqlDB table over this topic to begin referencing it from ksqlDB
queries. Streams and tables in ksqlDB essentially associate a schema with a
{{ site.ak }} topic, breaking each message in the topic into strongly typed
columns.

```sql
CREATE TABLE driverProfiles (
  driver_id INTEGER PRIMARY KEY,
  make STRING,
  model STRING,
  year INTEGER,
  license_plate STRING,
  rating DOUBLE
)
WITH (kafka_topic='jdbc_driver_profiles', value_format='json');
```

Tables in ksqlDB support update semantics, where each message in the
underlying topic represents a row in the table. For messages in the topic with
the same key, the latest message associated with a given key represents the
latest value for the corresponding row in the table.

!!! note
    When the data is ingested from the database, it's being written
    to the {{ site.ak }} topic using JSON serialization. Since JSON itself doesn't
    declare a schema, you need to declare it again when you run `CREATE TABLE`. 
    In practice, you would normally use Avro or Protobuf, since this supports the retention
    of schemas, ensuring compatibility between producers and consumers. This means
    that you don't have to enter it each time you want to use the data in ksqlDB.

10. Create streams for driver locations and rider locations
-----------------------------------------------------

In this step, you create streams over new topics to encapsulate location pings that are sent
every few seconds by drivers’ and riders’ phones. In contrast to tables,
ksqlDB streams are append-only collections of events, so they're suitable for a
continuous stream of location updates.

```sql
CREATE STREAM driverLocations (
  driver_id INTEGER KEY,
  latitude DOUBLE,
  longitude DOUBLE,
  speed DOUBLE
)
WITH (kafka_topic='driver_locations', value_format='json', partitions=1);

CREATE STREAM riderLocations (
  driver_id INTEGER KEY,
  latitude DOUBLE,
  longitude DOUBLE
)
WITH (kafka_topic='rider_locations', value_format='json', partitions=1);
```

11. Enrich driverLocations stream by joining with PostgreSQL data
-----------------------------------------------------------------

The `driverLocations` stream has a relatively compact schema, and it doesn’t
contain much data that a human would find particularly useful. You can *enrich*
the stream of driver location events by joining them with the human-friendly
vehicle information stored in the PostgreSQL database. This enriched data can
be presented by the rider’s mobile application, ultimately helping the rider to
safely identify the driver’s vehicle.

You can achieve this result easily by joining the `driverLocations` stream with
the `driver_profiles` table stored in PostgreSQL.

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
    jdbc.rating        AS rating
  FROM driverLocations dl JOIN driverProfiles jdbc
    ON dl.driver_id = jdbc.driver_id
  EMIT CHANGES;
```

12. Create the rendezvous stream
----------------------------

To put all of this together, create a final stream that the ridesharing app can
use to facilitate a driver-rider rendezvous in real time. This stream is
defined by a query that joins together rider and driver location updates,
resulting in a contextualized output that the app can use to show the rider
their driver’s position as the rider waits to be picked up.

The rendezvous stream includes human-friendly information describing the
driver’s vehicle for the rider. Also, the rendezvous stream computes
(albeit naively) the driver’s estimated time of arrival (ETA) at the rider’s
location.

```sql
CREATE STREAM rendezvous AS
  SELECT
    e.driver_id     AS driver_id,
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

13. Start two ksqlDB CLI sessions
---------------------------------

Run the following command twice to open two separate ksqlDB CLI sessions. If
you still have a CLI session open from a previous step, you can reuse that
session.

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

14. Run a continuous query
--------------------------

In this step, you run a continuous query over the rendezvous stream.

This may feel a bit unfamiliar, because the query never returns until you
terminate it. The query perpetually pushes output rows to the client as events
are written to the rendezvous stream. Leave the query running in your CLI
session for now. It will begin producing output as soon as events are written
into ksqlDB.

```sql
SELECT * FROM rendezvous EMIT CHANGES;
```

15. Write data to input streams
-------------------------------

Your continuous query reads from the `rendezvous` stream, which takes its input
from the `enrichedDriverLocations` and `riderLocations` streams. And
`enrichedDriverLocations` takes its input from the `driverLocations` stream,
so you need to write data into `driverLocations` and `riderLocations` before
`rendezvous` produces the joined output that the continuous query reads.

```sql
INSERT INTO driverLocations (driver_id, latitude, longitude, speed) VALUES (0, 37.3965, -122.0818, 23.2);
INSERT INTO riderLocations (driver_id, latitude, longitude) VALUES (0, 37.3952, -122.0813);

INSERT INTO driverLocations (driver_id, latitude, longitude, speed) VALUES (1, 37.7850, -122.40270, 12.0);
INSERT INTO riderLocations (driver_id, latitude, longitude) VALUES (1, 37.7887, -122.4074);

INSERT INTO driverLocations (driver_id, latitude, longitude, speed) VALUES (2, 37.7925, -122.4148, 11.2);
INSERT INTO riderLocations (driver_id, latitude, longitude) VALUES (2, 37.7876, -122.4235);

INSERT INTO driverLocations (driver_id, latitude, longitude, speed) VALUES (3, 37.4471, -122.1625, 14.7);
INSERT INTO riderLocations (driver_id, latitude, longitude) VALUES (3, 37.4442, -122.1658);
```

As soon as you start writing rows to the input streams, your continuous query
from the previous step starts producing joined output. The rider's location
pings are joined with their inbound driver's location pings in real time,
providing the rider with driver ETA, rating, and additional information
describing the driver's vehicle.

Next steps
-------------

This tutorial shows how to run ksqlDB in embedded {{ site.kconnect }} mode
using Docker. It uses the JDBC connector to integrate ksqlDB with PostgreSQL
data, but this is just one of many connectors that are available to help you
integrate ksqlDB with external systems. Check out
[Confluent Hub](https://www.confluent.io/hub/) to learn more about all of the
various connectors that enable integration with a wide variety of external
systems.

You may also want to take a look at our
[examples](https://ksqldb.io/examples.html) to better understand how you can
use ksqlDB for your specific workload.
