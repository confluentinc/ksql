.. _ksql_quickstart:

Quickstart
==========

**Table of Contents**

.. contents::
  :local:


Goal of this KSQL quickstart
----------------------------

Welcome to Confluent and Kafka Structured Query Language (KSQL)!

The goal of this quickstart guide is to provide you with a first hands-on look at KSQL. This quickstart
will guide you through a simple workflow to be able to query and transform KSQL data.

Start the Kafka cluster
-----------------------

In this quickstart, you will run KSQL on the same machine as the Kafka cluster.  If you already have an existing running Kafka cluster that you can connect to locally, you can skip this section.  On the other hand, if you want to spin up a development Kafka environment for KSQL, you have two options:

Option 1: Docker
^^^^^^^^^^^^^^^^

This assumes Docker on Mac.  If you are still using Docker Machine, please upgrade <TODO: INSERT LINKS>.  If you are new to Docker, get a general overview of Kafka on Docker: http://docs.confluent.io/current/cp-docker-images/docs/quickstart.html

<TODO: Docker>

1. Clone the Confluent KSQL Docker demo repository:

<TODO: insert link>

2. Change into the directory for this tutorial

<TODO: cd>

3. Launch the KSQL demo in Docker

.. sourcecode:: bash

   $ docker-compose up -d

4. Verify five Docker containers were created: ZooKeeper, Kafka Broker, Schema Registry, KSQL, Data Generator.

.. sourcecode:: bash

   $ docker ps
   <TODO: update with expected output>

   CONTAINER ID        IMAGE                                           COMMAND                  CREATED             STATUS              PORTS                                                    NAMES
   4ccd999c5be9        confluentinc/cp-kafka-ksql-standalone:latest    "bash -c 'echo Wai..."   7 minutes ago       Up 7 minutes        0.0.0.0:7070->7070/tcp                                   demo_kafka-ksql-standalone_1
   bba4889c1002        confluentinc/cp-kafka-ksql-datagen:latest       "bash -c 'echo Wai..."   7 minutes ago       Up 7 minutes        0.0.0.0:7071->7071/tcp                                   demo_kafka-ksql-datagen_1
   c5e586669ed7        confluentinc/cp-schema-registry:latest          "/etc/confluent/do..."   7 minutes ago       Up 7 minutes        0.0.0.0:8081->8081/tcp                                   demo_schema-registry_1
   342a056f024e        confluentinc/cp-enterprise-kafka:latest         "/etc/confluent/do..."   7 minutes ago       Up 7 minutes        0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp         demo_kafka_1
   5f2917b2bed4        confluentinc/cp-zookeeper:latest                "/etc/confluent/do..."   9 minutes ago       Up 7 minutes        2181/tcp, 2888/tcp, 3888/tcp, 0.0.0.0:32181->32181/tcp   demo_zookeeper_1


5. Verify Kafka topics were pre-generated.

.. sourcecode:: bash

   $ docker-compose exec kafka kafka-topics --zookeeper zookeeper:32181 --list
   <TODO: insert expected output>



Option 2: Non-Docker
^^^^^^^^^^^^^^^^^^^^

This section is for users who are not using Docker. You will need to download and install a Kafka cluster on your local machine.  This cluster consists of a single Kafka broker alongside a single-node ZooKeeper ensemble.  

1. Install Oracle Java JRE or JDK >= 1.7 on your local machine

2. Download and install Confluent Platform 3.3.0, which includes a Kafka broker, ZooKeeper, Schema Registry, REST Proxy, and Kafka Connect.
We recommend running the latest version of Confluent Platform, but the minimum version compatible with KSQL is <TODO: INSERT VERSION>.  Install Confluent Platform directly onto a Linux server: http://docs.confluent.io/current/installation.html

3. If you installed Confluent Platform via tar or zip, change into the installation directory. The paths and commands used throughout this quickstart
   assume that your are in this installation directory:

.. sourcecode:: bash

  # Change to the installation directory (if you installed via tar or zip)
  $ cd confluent-3.3.0/

4. Start the ZooKeeper instance, which will listen on ``localhost:2181``.  Since this is a long-running service, you should run it in its own terminal.

.. sourcecode:: bash

  # Start ZooKeeper.  Run this command in its own terminal.
  $ ./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties

5. Start the Kafka broker, which will listen on ``localhost:9092`` and connect to the ZooKeeper instance we just started.  Since this is also a long-running service, you should run it in its own terminal.

.. sourcecode:: bash

  # Start Kafka.  Run this command in its own terminal
  $ ./bin/kafka-server-start ./etc/kafka/server.properties

6. Start the Confluent Schema Registry, which will listen on ``localhost:8081`` and connect to the ZooKeeper instance we just started.  Since this is also a long-running service, you should run it in its own terminal.

.. sourcecode:: bash

  # Start Schema Registry.  Run this command in its own terminal
  $ ./bin/schema-registry-start ./etc/schema-registry/schema-registry.properties

7. Download the KSQL jar file <TODO: insert download link>.

8. Download the demo data generation jar file <TODO: insert download link>.



Start KSQL
----------

Option 1: Docker
^^^^^^^^^^^^^^^^

.. sourcecode:: bash

   # From the host machine: connect to Docker container
   host$ docker-compose exec <container with KSQL application> sh

   # From the Docker container: start KSQL connecting to broker running on remote container/host
   container$ java -jar ksql-cli-1.0-SNAPSHOT-standalone.jar remote --bootstrap-server kafka:29092
   ...
   ksql> 


Option 2: non-Docker
^^^^^^^^^^^^^^^^^^^^

.. sourcecode:: bash

   # Start KSQL connecting to broker running on local host
   $ java -jar ksql-cli-1.0-SNAPSHOT-standalone.jar local
   ...
   ksql> 

Cluster properties
^^^^^^^^^^^^^^^^^^

KSQL accepts command line options, see ``java -jar ksql-cli-1.0-SNAPSHOT-standalone.jar help local`` for usage.
If you have any Kafka properties that you want to override when starting KSQL, you can start KSQL with a properties file.
For example, if your broker is listening on ``broker1:9092`` and you want to set ``auto.offset.reset=earliest``: <TODO: call out earliest>

   .. sourcecode:: bash

   # Create ``cluster.properties`` file
   $ cat cluster.properties
   application.id=ksql_app
   bootstrap.servers=broker1:9092
   auto.offset.reset=earliest

   # Start KSQL and pass in the properties file
   $ java -jar ksql-cli-1.0-SNAPSHOT-standalone.jar local --properties-file cluster.properties


Produce data to topics in the Kafka cluster
-------------------------------------------

Docker image already generates data into Kafka topics.  If you want to generate additional data, you can use the provided data generator jar.  Or you can use the commandline tools to write messages, as shown below.

1. Use the ``kafka-console-producer`` to produce messages to a topic called ``ksqlString``, with value of type String.

.. sourcecode:: bash

   # Produce messages to a topic called ``ksqlString``, with a key of type String and value of type String
   $ ./bin/kafka-console-producer --topic ksqlString --broker-list localhost:9092  --property parse.key=true --property key.separator=,
   key1,value1
   key2,value2
   key3,value3
   key1,value4

2. Verify messages were written to this topic ``ksqlString``. Press ``ctrl-c`` to exit ``kafka-console-consumer``.

.. sourcecode:: bash

   # Consume messages from the topic called ``ksqlString``
   $ ./bin/kafka-console-consumer --topic ksqlString --bootstrap-server localhost:9092 --from-beginning --property print.key=true
   key1,value1
   key2,value2
   key3,value3
   key1,value4

3. Run the data generator to produce additional data to the Kafka cluster.

.. sourcecode:: bash

   $ java -jar ./ksql-examples/target/ksql-examples-1.0-SNAPSHOT-standalone.jar quickstart=users format=json topic=topic_json maxInterval=1000



Read Kafka topic data into KSQL
-------------------------------

1. Register the ``ksqlString`` topic into KSQL, specifying the ``value_format`` of ``DELIMITED``, and view the contents of topic.

.. sourcecode:: bash

   ksql> REGISTER TOPIC ksqlStringTopic WITH (kafka_topic='ksqlString', value_format='DELIMITED');

2. Print contents of this topic. Press ``<Ctrl-c>`` to exit.

   ksql> PRINT ksqlStringTopic;
   <TODO: KSQL-165 earliest problem getting all values. Also KSQL-132, ctrl-c does not work>
1500990793064 , key1 , value1
1500990796384 , key2 , value2
1500990798954 , key3 , value3
1500990800506 , key1 , value4

3. List all the Kafka topics on the Kafka broker. You should see a topic in the Kafka cluster called ``ksqlString``. It is marked as "registered" in KSQL.

.. sourcecode:: bash

   ksql> show topics;
   <TODO: INSERT show topics command when KSQL-115 is implemented>

4. Create a KSQL stream from the registered Kafka topic, and describe and view the stream. <TODO: Can we not REGISTER And CREATE STREAM in one command? KSQL-137>

.. sourcecode:: bash

   ksql> CREATE STREAM ksqlStringStream (value string) WITH (registered_topic='ksqlStringTopic');

5. Create a KSQL table from the registered Kafka topic, and describe and view the stream. Notice that you now need to specify the state store name (i.e. Kafka topic) that will be used for backup. <TODO: link to KSQL concepts guide to explain difference between Stream and Table> <TODO: link to KSQL concepts guide to explain why tables need state store and streams don't>

.. sourcecode:: bash

   ksql> CREATE TABLE ksqlStringTable (value string) WITH (registered_topic='ksqlStringTopic', statestore='ksqlStringStore');

6. View the schemas of the newly created STREAM and TABLE. Notice that the key corresponds to column ``ROWKEY`` and the value corresponds to column ``VALUE``. <TODO: ROWTIME corresponds to...message timestamp?>

.. sourcecode:: bash

   ksql> DESCRIBE ksqlStringStream;
      Field |   Type 
   ------------------
    ROWTIME |  INT64 
     ROWKEY | STRING 
      VALUE | STRING 

   ksql> DESCRIBE ksqlStringTable;
      Field |   Type 
   ------------------
    ROWTIME |  INT64 
     ROWKEY | STRING 
      VALUE | STRING 

7. View all the KSQL STREAMS and TABLES.

.. sourcecode:: bash

   ksql> show streams;

    Stream Name |       Ksql Topic 
   --------------------------------
       COMMANDS | __COMMANDS_TOPIC 
     KSQLSTREAM |  KSQLSTRINGTOPIC 

.. sourcecode:: bash

   ksql> show tables;

         Table Name |      Ksql Topic |      Statestore | Windowed 
   ----------------------------------------------------------------
    KSQLSTRINGTABLE | KSQLSTRINGTOPIC | ksqlStringStore |    false 


Query and transform KSQL data
-----------------------------

1. Create a non-persistent query to select rows where the key is ``key1``. Press ``ctrl-c`` to exit this query.

.. sourcecode:: bash

   ksql> SELECT * FROM ksqlStringStream WHERE rowkey LIKE '%key1%';
   <TODO: select * hangs, due to KSQL-130?  LIMIT still has issues like KSQL-140. And Ctrl-c doesn't work KSQL-132>

2. Create a persistent query to select rows where the key is ``key1``, and persist it by sending the query results to a new KSQL stream called ``newksqlStringStream`` and to a Kafka topic called ``ksqlOutput-key1``. <TODO: explain why do we need a stream?  Why can't we write directly to just a topic?>

.. sourcecode:: bash

   ksql> CREATE STREAM newksqlStringStream WITH (kafka_topic='ksqlOutput-key1', value_format='DELIMITED') AS SELECT * FROM ksqlStringStream WHERE rowkey LIKE '%key1%';
   <TODO: discuss/resolve KSQL-145, "show queries" connection to "create stream">

3. Print the contents of the newly created topic ``ksqlOutput-key1``, which should show only those rows where value is ``key``. Backticks are required around the name of the topic because of SQL standard rules for hyphens.

.. sourcecode:: bash

   ksql> PRINT `ksqlOutput-key1`;

4. Provide example with "PARTITION BY" to assign key, if ROWKEY is null.  <TODO: discuss/resolve KSQL-146 in case this changes the keywords>

5. <TODO: INSERT JOIN example, requires KSQL-152>

6. <TODO: WINDOW example, requires KSQL-152>


Use JSON and Avro formats
-------------------------

When we registered the Kafka topic ``ksqlString`` in KSQL, we specified a value format ``DELIMITED``. This is because the messages were written to the Kafka topic as plain Strings. You can also register Kafka topics with other formats, including ``JSON`` and ``avro``.

JSON
^^^^

1. From the command line, use the ``kafka-console-producer`` to produce messages to a topic called ``ksqlJson``, with value of type JSON.

.. sourcecode:: bash

   # Produce messages to a topic called ``ksqlJson``, with a key of type String and value of type Vro
   $ ./bin/kafka-console-producer --topic ksqlJson --broker-list localhost:9092
   {"name":"value1","id":"key1"}
   {"name":"value2","id":"key2"}
   {"name":"value3","id":"key3"}
   {"name":"value4","id":"key1"}

2. Verify messages were written to this topic ``ksqlJson``

.. sourcecode:: bash

   # Consume messages from the topic called ``ksqlJson``
   $ ./bin/kafka-console-consumer --topic ksqlJson --bootstrap-server localhost:9092 --from-beginning
   {"name":"value1","id":"key1"}
   {"name":"value2","id":"key2"}
   {"name":"value3","id":"key3"}
   {"name":"value4","id":"key1"}

3. In the KSQL application, register the ``ksqlJson`` topic into KSQL, specifying the ``value_format`` of ``JSON``.

.. sourcecode:: bash

   ksql> REGISTER TOPIC ksqlJsonTopic WITH (kafka_topic='ksqlJson', value_format='JSON');

4. Create a KSQL stream from the registered Kafka topic, and describe and view the stream. 

.. sourcecode:: bash

   ksql> CREATE STREAM ksqlJsonStream (name varchar, id varchar) WITH (registered_topic='ksqlJsonTopic', key='id');

5. View the schemas of the newly created STREAM. Notice that now there are columns ``NAME`` and ``ID``. <TODO: explain why ROWKEY has empty values>

.. sourcecode:: bash

   ksql> DESCRIBE ksqlJsonStream;

      Field |   Type 
   ------------------
    ROWTIME |  INT64 
     ROWKEY | STRING 
       NAME | STRING 
         ID | STRING 

6. Create a non-persistent query to select all rows. Press ``ctrl-c`` to exit this query.

.. sourcecode:: bash

   ksql> SELECT * from ksqlJsonStream;
   <TODO: select * hangs, due to KSQL-130?  HOW DOES LIMIT WORK TO MAKE SURE THIS RETURNS? Ctrl-c doesn't work>


Now you can proceed with any computations and transformations as described earlier.


Avro
^^^^

Use an Avro schema file for a given topic to read. Avro records are written using Schema Registry, but use a local schema file to deserialize the Avro message

<TODO: Need KSQL-133 and KSQL-125>

1. From the command line, use the ``kafka-avro-console-producer`` to produce messages to a topic called ``ksqlAvro``, writing schemas to Schema Registry.

.. sourcecode:: bash

   # Produce messages to a topic called ``ksqlAvro``, with a key of type String and value of type Avro
   $ ./bin/kafka-avro-console-producer --broker-list localhost:9092 --topic ksqlAvro  --property value.schema='{"type":"record","name":"myavro","fields":[{"name":"name","type":"string"},{"name":"id","type":"string"}]}' --property schema.registry.url=http://localhost:8081
   {"name":"value1","id":"key1"}
   {"name":"value2","id":"key2"}
   {"name":"value3","id":"key3"}
   {"name":"value4","id":"key1"}

2. Verify messages were written to this topic ``ksqlAvro``

.. sourcecode:: bash

   # Consume messages from the topic called ``ksqlAvro``
   $ ./bin/kafka-avro-console-consumer --topic ksqlAvro --bootstrap-server localhost:9092 --from-beginning --property schema.registry.url=http://localhost:8081
   {"name":"value1","id":"key1"}
   {"name":"value2","id":"key2"}
   {"name":"value3","id":"key3"}
   {"name":"value4","id":"key1"}

3. In the KSQL application, register the ``ksqlAvro`` topic into KSQL, specifying the ``value_format`` of ``Avro``.

.. sourcecode:: bash

   ksql> REGISTER TOPIC ksqlAvroTopic WITH (kafka_topic='ksqlAvro', value_format='Avro', avroschemafile='myavro.avsc');

4. Create a KSQL stream from the registered Kafka topic, and describe and view the stream. 

.. sourcecode:: bash

   ksql> CREATE STREAM ksqlAvroStream (name varchar, id varchar) WITH (registered_topic='ksqlAvroTopic', key='id');

5. View the schemas of the newly created STREAM. Notice that now there are columns ``NAME`` and ``ID``. <TODO: explain why ROWKEY has empty values>

.. sourcecode:: bash

   ksql> DESCRIBE ksqlAvroStream;

      Field |   Type 
   ------------------
    ROWTIME |  INT64 
     ROWKEY | STRING 
       NAME | STRING 
         ID | STRING 

6. Create a non-persistent query to select all rows. Press ``ctrl-c`` to exit this query.

.. sourcecode:: bash

   ksql> SELECT * from ksqlAvroStream;
   <TODO: Need KSQL-133 and KSQL-125>


Exit KSQL
---------

1. <TODO: INSERT TERMINATE EXAMPLE>  <TODO: link to KSQL concepts guide, when is terminate relevant...is it only with workers?>

2. From the KSQL prompt ``ksql>``, type 'exit'.

.. sourcecode:: bash

  ksql> exit


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

