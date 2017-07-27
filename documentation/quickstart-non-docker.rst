.. _ksql_quickstart:


Non-Docker Setup for KSQL
=========================

**Table of Contents**

.. contents::
  :local:


This part of the quickstart will guide you through the steps to setup a Kafka cluster and start KSQL for non-Docker environments. Once you complete these steps, you can start using KSQL to query the Kafka cluster.


Start a Kafka cluster
---------------------

Do not run KSQL against a production cluster, since KSQL is in tech preview.

You will need to download and install a Kafka cluster on your local machine.  This cluster consists of a single Kafka broker along with a single-node ZooKeeper ensemble and a single Schema Registry instance.

1. Install Oracle Java JRE or JDK >= 1.7 on your local machine

2. Download and install Confluent Platform 3.3.0, which includes a Kafka broker, ZooKeeper, Schema Registry, REST Proxy, and Kafka Connect.
We recommend running the latest version of Confluent Platform, but the minimum version compatible with KSQL is <TODO: INSERT VERSION>.  Install Confluent Platform directly onto a Linux server: http://docs.confluent.io/current/installation.html

3. If you installed Confluent Platform via tar or zip, change into the installation directory. The paths and commands used throughout this quickstart assume that your are in this installation directory:

.. sourcecode:: bash

  $ cd confluent-3.3.0/

4.  Start the Confluent Platform using the new Confluent CLI (part of the free Confluent Open Source distribution). ZooKeeper is listening on ``localhost:2181``, Kafka broker is listening on ``localhost:9092``, and Confluent Schema Registry is listening on ``localhost:8081``.

.. sourcecode:: bash

   $ ./bin/confluent start
   Starting zookeeper
   zookeeper is [UP]
   Starting kafka
   kafka is [UP]
   Starting schema-registry
   schema-registry is [UP]
   Starting kafka-rest
   kafka-rest is [UP]
   Starting connect
   connect is [UP]


Start KSQL
----------

1. Download the KSQL jar file <TODO: insert download link>. Then you can start KSQL. Use the keyword ``local`` if the broker is running on your local machine.

.. sourcecode:: bash

   $ java -jar ksql-cli-1.0-SNAPSHOT-standalone.jar local
   ...
   ksql>

2. KSQL accepts command line options, see ``java -jar ksql-cli-1.0-SNAPSHOT-standalone.jar help local`` for usage.
If you have any Kafka properties that you want to override when starting KSQL, you can start KSQL with a properties file.
For example, if your broker is listening on ``broker1:9092`` and you want to set ``auto.offset.reset=earliest``, you can override these settings as fo
llows. NOTE: set ``auto.offset.reset=earliest`` if you want the STREAM or TABLE to process data already in the Kafka topic. Here is a sample propertie
s file, you need to create your own if you want to override defaults.

   .. sourcecode:: bash

   $ cat cluster.properties
   application.id=ksql_app
   bootstrap.servers=broker1:9092
   auto.offset.reset=earliest

   $ java -jar ksql-cli-1.0-SNAPSHOT-standalone.jar local --properties-file cluster.properties

3. Refer to the steps below to produce some topic data to the Kafka cluster.


Produce topic data
------------------

KSQL creates STREAMS and TABLES that queries Kafka topics, so first you need to make sure you have Kafka topics to read from. Choose any of the following options:

1. Produce Kafka data with the Kafka commandline ``kafka-console-producer``. The following example generates data to a topic called ``ksqlString``, with value of type String.

.. sourcecode:: bash

   $ kafka-console-producer --topic ksqlString --broker-list localhost:9092  --property parse.key=true --property key.separator=,
   key1,value1
   key2,value2
   key3,value3
   key1,value4

2. Return to the [main KSQL quickstart](quickstart.rst#query-and-transform-ksql-data) and follow those steps to start using KSQL to query this topic.

3. You can produce additional Kafka data using the provided data generator. The following example generates data to a topic called ``user_topic_json``.

   .. sourcecode:: bash

   $ java -jar ./ksql-examples/target/ksql-examples-1.0-SNAPSHOT-standalone.jar quickstart=users format=json topic=user_topic_json maxInterval=1000

4. For Json format, using the same Kafka commandline ``kafka-console-producer``, produce messages to a topic called ``ksqlJson``.

.. sourcecode:: bash

   $ kafka-console-producer --topic ksqlJson --broker-list localhost:9092
   {"name":"value1","id":"key1"}
   {"name":"value2","id":"key2"}
   {"name":"value3","id":"key3"}
   {"name":"value4","id":"key1"}

5. From Avro format, using the same Kafka commandline, use the ``kafka-avro-console-producer`` to produce messages to a topic called ``ksqlAvro``.

.. sourcecode:: bash

   $ kafka-avro-console-producer --broker-list localhost:9092 --topic ksqlAvro  --property value.schema='{"type":"record","name":"myavro","fields":[{"name":"name","type":"string"},{"name":"id","type":"string"}]}' --property schema.registry.url=http://localhost:8081
   {"name":"value1","id":"key1"}
   {"name":"value2","id":"key2"}
   {"name":"value3","id":"key3"}
   {"name":"value4","id":"key1"}

