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

In this section we download and install a Kafka cluster on your local machine.  This cluster consists of a single
Kafka broker alongside a single-node ZooKeeper ensemble.  

.. note::
  In this quickstart, you will run KSQL on the same machine as the Kafka cluster. In production, you would
  typically run KSQL on client machines separate from the Kafka cluster

1. Install Oracle Java JRE or JDK >= 1.7 on your local machine

2. Download and install Confluent Platform 3.3.0, which includes a Kafka broker, ZooKeeper, Schema Registry, REST Proxy, and Kafka Connect.
We recommend running the latest version of Confluent Platform, but the minimum version compatible with KSQL is <TODO: INSERT VERSION>

You have two installation choices:
* Install directly onto a Linux server: http://docs.confluent.io/current/installation.html
* Install with Docker: http://docs.confluent.io/current/cp-docker-images/docs/quickstart.html

.. note::
   This quickstart assumes you have installed directly onto a Linux server. <TODO: Docker version?>

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


Produce data to topics in the Kafka cluster
-------------------------------------------

1. Use the ``kafka-console-producer`` to produce messages to a topic called ``ksqlString``, with value of type String.

.. sourcecode:: bash

   # Produce messages to a topic called ``ksqlString``, with a key of type String and value of type String
   $ ./bin/kafka-console-producer --topic ksqlString --broker-list localhost:9092  --property parse.key=true --property key.separator=,
   key1,value1
   key2,value2
   key3,value3
   key1,value4

2. Verify messages were written to this topic ``ksqlString``. Press ``ctrl-c`` to exit ``kafka-console-consumer``.

   # Consume messages from the topic called ``ksqlString``
   $ ./bin/kafka-console-consumer --topic ksqlString --bootstrap-server localhost:9092 --from-beginning --property print.key=true
   key1,value1
   key2,value2
   key3,value3
   key1,value4


Start KSQL and read topic data into KSQL
----------------------------------------

1. Download the KSQL jar file <TODO: insert download link>.

2. Start KSQL. In this example, we use ``local`` mode to connect to the Kafka broker running on the local machine that is listening on ``localhost:9092``.

.. sourcecode:: bash

   # Start KSQL on the local host
   $ java -jar ksql-cli-1.0-SNAPSHOT-standalone.jar local
   ...
   ksql> 

.. note::
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


3. Register the ``ksqlString`` topic into KSQL, specifying the ``value_format`` of ``DELIMITED``, and view the contents of topic.

.. sourcecode:: bash

   ksql> REGISTER TOPIC ksqlStringTopic WITH (kafka_topicname='ksqlString', value_format='DELIMITED');
   <TODO: kafka_topicname is becoming kafka_topic with KSQL-111>

   ksql> PRINT ksqlStringTopic;
   <TODO: THIS DOES NOT OUTPUT ANYTHING even with earliest set KSQL-130, plus error on new messages KSQL-129>

4. List all the Kafka topics on the Kafka broker. You should see a topic in the Kafka cluster called ``ksqlString``. It is marked as "registered" in KSQL.

.. sourcecode:: bash

   ksql> show topics;
   <TODO: INSERT show topics command when KSQL-115 is implemented>

5. Create a KSQL stream from the registered Kafka topic, and describe and view the stream. <TODO: Can we not REGISTER And CREATE STREAM in one command? KSQL-137>

.. sourcecode:: bash

   ksql> CREATE STREAM ksqlStringStream (value string) WITH (registered_topicname='ksqlStringTopic');
   <TODO: registered_topicname is becoming registered_topic with KSQL-111?>

6. Create a KSQL table from the registered Kafka topic, and describe and view the stream. Notice that you now need to specify the state store name (i.e. Kafka topic) that will be used for backup. <TODO: link to KSQL concepts guide to explain difference between Stream and Table> <TODO: link to KSQL concepts guide to explain why tables need state store and streams don't>

.. sourcecode:: bash

   ksql> CREATE TABLE ksqlStringTable (value string) WITH (registered_topicname='ksqlStringTopic', statestore='ksqlStringStore');

7. View the schemas of the newly created STREAM and TABLE. Notice that the key corresponds to column ``ROWKEY`` and the value corresponds to column ``VALUE``. <TODO: ROWTIME corresponds to...message timestamp?>

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

8. View all the KSQL STREAMS and TABLES.

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

   ksql> SELECT * from ksqlStringStream WHERE rowkey LIKE '%key1%';
   <TODO: select * hangs, due to KSQL-130?  HOW DOES LIMIT WORK TO MAKE SURE THIS RETURNS? Ctrl-c doesn't work>

2. Create a persistent query to select rows where the key is ``key1``, and persist it by sending the query results to a new KSQL stream called ``newksqlStringStream`` and to a Kafka topic called ``ksqlOutput-key1``. <TODO: explain why do we need a stream?  Why can't we write directly to just a topic?>

.. sourcecode:: bash

   ksql> CREATE STREAM newksqlStringStream WITH (kafka_topicname='ksqlOutput-key1', value_format='DELIMITED') AS SELECT * FROM ksqlStringStream WHERE rowkey LIKE '%key1%';

   <TODO: discuss/resolve KSQL-145, "show queries" connection to "create stream">

3. Print the contents of the newly created topic ``ksqlOutput-key1``, which should show only those rows where value is ``key``. Backticks are required around the name of the topic because of SQL standard rules for hyphens.

.. sourcecode:: bash

   ksql> PRINT `ksqlOutput-key1`;

4. Provide example with "PARTITION BY" to assign key, if ROWKEY is null.  <TODO: discuss/resolve KSQL-146 in case this changes the keywords>

5. <TODO: INSERT example with limit keyword, requires KSQL-140>

6. <TODO: INSERT JOIN example, requires KSQL-152>

7. <TODO: window example, requires KSQL-152>


Use JSON and Avro formats
-------------------------

When we registered the Kafka topic ``ksqlString`` in KSQL, we specified a value format ``DELIMITED``. This is because the messages were written to the Kafka topic as plain Strings. You can also register Kafka topics with other formats, including ``JSON`` and ``avro``.

JSON
^^^^

1. From the command line, use the ``kafka-console-producer`` to produce messages to a topic called ``ksqlRecord``, with value of type JSON.

.. sourcecode:: bash

   # Produce messages to a topic called ``ksqlRecord``, with a key of type String and value of type Vro
   $ ./bin/kafka-console-producer --topic ksqlRecord --broker-list localhost:9092
   {"name":"value1","id":"key1"}
   {"name":"value2","id":"key2"}
   {"name":"value3","id":"key3"}
   {"name":"value4","id":"key1"}

2. Verify messages were written to this topic ``ksqlRecord``

   # Consume messages from the topic called ``ksqlRecord``
   $ ./bin/kafka-console-consumer --topic ksqlRecord --bootstrap-server localhost:9092 --from-beginning
   {"name":"value1","id":"key1"}
   {"name":"value2","id":"key2"}
   {"name":"value3","id":"key3"}
   {"name":"value4","id":"key1"}

3. In the KSQL application, register the ``ksqlRecord`` topic into KSQL, specifying the ``value_format`` of ``JSON``.

.. sourcecode:: bash

   ksql> REGISTER TOPIC ksqlJsonTopic WITH (kafka_topicname='ksqlRecord', value_format='JSON');

4. Create a KSQL stream from the registered Kafka topic, and describe and view the stream. 

.. sourcecode:: bash

   ksql> CREATE STREAM ksqlJsonStream (name varchar, id varchar) WITH (registered_topicname='ksqlJsonTopic', key='id');

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

Use an Avro schema file for a given topic to read. Schema Registry integration is not yet supported.  

<TODO: Need KSQL-133 and KSQL-125>


Exit KSQL
---------

1. <TODO: INSERT TERMINATE EXAMPLE>  <TODO: link to KSQL concepts guide, when is terminate relevant...is it only with workers?>

2. From the KSQL prompt ``ksql>``, type 'exit'.

.. sourcecode:: bash

  ksql> exit


Examine Kafka cluster changes
-----------------------------

KSQL uses the Kafka cluster to store state. We will examine the Kafka cluster to see what happened behind the scenes.

1. View the topics in the Kafka cluster. You should see the topics you manually created, e.g. ``ksqlString``, ``ksqlRecord``, as well as other topics created by KSQL including ``ksql_app_commands``, ``ksql_bare_query_*``, etc. <TODO: link to KSQL concepts guide to explain what these other auto-generated topics are used for>

.. sourcecode:: bash

   $ kafka-topics --list --zookeeper localhost:2181
   ...
   ksql_app_commands
   ksqlString
   ksql_bare_query_6739854484049497815_1500404750526-ksqlstore-changelog
   ksql_bare_query_6739854484049497815_1500404750526-ksqlstore-repartition
   ...
   <TODO: update this list based on quickstart workflow>

2. Read the data stored in the topic called ``ksql_app_commands``

.. sourcecode:: bash

   $ ./bin/kafka-console-consumer --topic ksql_app_commands --bootstrap-server localhost:9092 --from-beginning --property print.key=true
   <TODO: INSERT OUTPUT>

3. <TODO: INSERT OTHER INTERIM TOPICS CREATED>

4. <TODO: Anything else to look at?>

