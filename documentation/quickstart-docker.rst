.. _ksql_quickstart:


Docker Setup for KSQL
=====================

**Table of Contents**

.. contents::
  :local:


This part of the quickstart will guide you through the steps to setup a Kafka cluster and start KSQL for Docker environments. Once you complete these steps, you can start using KSQL to query the Kafka cluster.


Start a Kafka cluster
---------------------

Do not run KSQL against a production cluster, since KSQL is in tech preview.

As a pre-requisite, you will need Docker Compose.  If you are new to Docker, you can get a general overview of Kafka on Docker: http://docs.confluent.io/current/cp-docker-images/docs/quickstart.html

1. Clone the Confluent KSQL Docker demo repository:

<TODO: insert link>

2. Change into the directory for this tutorial

<TODO: cd>

3. Launch the KSQL demo in Docker

.. sourcecode:: bash

   $ docker-compose up -d

4. Verify five Docker containers were created: ZooKeeper, Kafka Broker, Schema Registry, KSQL, Data Generator.

.. sourcecode:: bash

   $ docker-compose ps
   <TODO: update with expected output>

   CONTAINER ID        IMAGE                                           COMMAND                  CREATED             STATUS              PORTS                                                    NAMES

5. Verify Kafka topics were pre-generated.

.. sourcecode:: bash

   $ docker-compose exec kafka kafka-topics --zookeeper zookeeper:32181 --list
   <TODO: insert expected output>




Start KSQL
----------

1. From the host machine, connect to a shell on the Docker container ``<TOOD: container with KSQL application>``.

.. sourcecode:: bash

   host$ docker-compose exec <TODO: container with KSQL application> sh

2. From the container, start KSQL connecting to broker running on remote container

.. sourcecode:: bash

   container$ java -jar ksql-cli-1.0-SNAPSHOT-standalone.jar remote --bootstrap-server kafka:29092
   ...
   ksql> 

3. KSQL accepts command line options, see ``java -jar ksql-cli-1.0-SNAPSHOT-standalone.jar help local`` for usage.
If you have any Kafka properties that you want to override when starting KSQL, you can start KSQL with a properties file.
For example, if your broker is listening on ``broker1:9092`` and you want to set ``auto.offset.reset=earliest``, you can override these settings as fo
llows. NOTE: set ``auto.offset.reset=earliest`` if you want the STREAM or TABLE to process data already in the Kafka topic. Here is a sample propertie
s file, you need to create your own if you want to override defaults.

   .. sourcecode:: bash

   $ cat cluster.properties
   application.id=ksql_app
   bootstrap.servers=kafka:29092
   auto.offset.reset=earliest

   $ java -jar ksql-cli-1.0-SNAPSHOT-standalone.jar local --properties-file cluster.properties

4. Return to the [main KSQL quickstart](quickstart.rst) and follow those steps to start querying the Kafka cluster.



Produce more topic data
-----------------------

KSQL creates STREAMS and TABLES that queries Kafka topics, so first you need to make sure you have Kafka topics to read from.  Our docker-compose file already runs a data generator that pre-populates Kafka topics with data, so no action is required if you want to use just the data available there.

However, if you want to produce additional data.

1. You can produce additional Kafka data using the provided container with the data generator. The following example generates data to a topic called ``user_topic_json``.

   .. sourcecode:: bash

   $ docker-compose exec ksql-application java -jar ./ksql-examples/target/ksql-examples-1.0-SNAPSHOT-standalone.jar quickstart=users format=json topic=user_topic_json maxInterval=1000

2. You can also produce additional Kafka data with the Kafka commandline ``kafka-console-producer``. The following example generates data to a topic called ``ksqlString2``, with value of type String.

.. sourcecode:: bash

   $ docker-compose exec kafka kafka-console-producer --topic ksqlString2 --broker-list kafka:29092  --property parse.key=true --property key.separator=,
   key1,value1
   key2,value2
   key3,value3
   key1,value4

3. For Json format, using the same Kafka commandline ``kafka-console-producer``, produce messages to a topic called ``ksqlJson``.

.. sourcecode:: bash

   $ docker-compose exec kafka kafka-console-producer --topic ksqlJson --broker-list kafka:29092
   {"name":"value1","id":"key1"}
   {"name":"value2","id":"key2"}
   {"name":"value3","id":"key3"}
   {"name":"value4","id":"key1"}

4. From Avro format, using the same Kafka commandline, use the ``kafka-avro-console-producer`` to produce messages to a topic called ``ksqlAvro``.

.. sourcecode:: bash

   $ docker-compose exec kafka kafka-avro-console-producer --broker-list kafka:29092 --topic ksqlAvro  --property value.schema='{"type":"record","name":"myavro","fields":[{"name":"name","type":"string"},{"name":"id","type":"string"}]}' --property schema.registry.url=http://schemaregistry:28081
   {"name":"value1","id":"key1"}
   {"name":"value2","id":"key2"}
   {"name":"value3","id":"key3"}
   {"name":"value4","id":"key1"}


Extra (To be Removed)
---------------------

Until KSQL-172 is done, I need to manually pre-create topics, produce, consume:

.. sourcecode:: bash
docker-compose exec kafka kafka-topics --zookeeper zookeeper:32181 --create --topic ksqlString --partitions 1 --replication-factor 1
docker-compose exec kafka kafka-console-producer --topic ksqlString --broker-list kafka:29092  --property parse.key=true --property key.separator=,
docker-compose exec kafka kafka-console-consumer --topic ksqlString --bootstrap-server kafka:29092 --from-beginning

docker-compose exec kafka kafka-topics --zookeeper zookeeper:32181 --create --topic order_json --partitions 1 --replication-factor 1
java -jar ksql-examples-1.0-SNAPSHOT-standalone-4.jar bootstrap-server=localhost:9092 quickstart=orders format=json topic=order_json
docker-compose exec kafka kafka-console-consumer --topic order_json --bootstrap-server kafka:29092 --from-beginning

