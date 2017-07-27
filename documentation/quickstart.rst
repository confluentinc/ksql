.. _ksql_quickstart:

KSQL Quickstart
===============

**Table of Contents**

.. contents::
  :local:


Welcome to Confluent and Kafka Structured Query Language (KSQL)!

The goal of this quickstart guide is to provide you with a first hands-on look at KSQL. This quickstart
will guide you through a simple workflow to be able to query and transform KSQL data.

Setup
-----

1. You will need access to a development Kafka cluster (with ZooKeeper, a Kafka broker, and Confluent Schema Registry) and then run KSQL against it. Do not run KSQL against a production Kafka cluster, since KSQL is in tech preview.

* If you are using a Docker environment, then follow [these instructions](quickstart-docker.rst) to run the Kafka cluster and start KSQL
* If you are not using a Docker environment, then follow (these instructions)[quickstart-non-docker.rst] to run the Kafka cluster and start KSQL

Once you have your Kafka cluster running and have started KSQL, you should see the KSQL prompt:

.. sourcecode:: bash

   ksql>

2. Produce data to your Kafka cluster

* If you are using our Docker Compose files, a data generator is already running and producing Kafka messages to your cluster. No further action is required
* If you are not not using our Docker environment, then follow [these instructions](quickstart-non-docker.rst#producedata) to generate data

<TODO: KSQL-205: data generator should pre-generate this data>

3. With KSQL running and data in your Kafka cluster ready for querying, you can proceed below.


Read Kafka topic data into KSQL
-------------------------------

Before proceeding with the steps below, please ensure that you have a topic in your Kafka cluster called ``ksqlString``, with messages keys and values of type String.

You should now be at your ``ksql>`` prompt.

1. Register the Kafka topic ``ksqlString`` into KSQL, specifying the ``value_format`` of ``DELIMITED``, and view the contents of topic.

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

4. Create a KSQL stream from the registered Kafka topic, assigning the topic's message value as a column called ``country``.  Describe and view the stream. <TODO: Can we not REGISTER And CREATE STREAM in one command? KSQL-137>

.. sourcecode:: bash

   ksql> CREATE STREAM ksqlStringStream (country string) WITH (registered_topic='ksqlStringTopic');

5. Create a KSQL table from the registered Kafka topic, and describe and view the stream. Notice that you now need to specify the state store name (i.e. Kafka topic) that will be used for backup. <TODO: link to KSQL concepts guide to explain difference between Stream and Table> <TODO: link to KSQL concepts guide to explain why tables need state store and streams don't>

.. sourcecode:: bash

   ksql> CREATE TABLE ksqlStringTable (country string) WITH (registered_topic='ksqlStringTopic', statestore='ksqlStringStore');

6. View the schemas of the newly created STREAM and TABLE. Notice that the key corresponds to column ``ROWKEY`` and the value corresponds to column ``COUNTRY``. <TODO: ROWTIME corresponds to...message timestamp?>

.. sourcecode:: bash

   ksql> DESCRIBE ksqlStringStream;
      Field |   Type 
   ------------------
    ROWTIME |  INT64 
     ROWKEY | STRING 
    COUNTRY | STRING 

   ksql> DESCRIBE ksqlStringTable;
      Field |   Type 
   ------------------
    ROWTIME |  INT64 
     ROWKEY | STRING 
    COUNTRY | STRING 

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



JOIN, WINDOW, PARTITION
-----------------------

1. <TODO: INSERT JOIN example, requires KSQL-152>

2. <TODO: WINDOW example, requires KSQL-152>

3. Provide example with "PARTITION BY" to assign key, if ROWKEY is null.  <TODO: discuss/resolve KSQL-146 in case this changes the keywords>


JSON and Avro
-------------

<TODO: discuss if we should omit talk of JSON/Avro completely for the first release?>

When we registered the Kafka topic ``ksqlString`` in KSQL, we specified a value format ``DELIMITED``. This is because the messages were written to the Kafka topic as plain Strings. You can also register Kafka topics with other formats, including ``JSON`` and ``Avro``.

1. Follow the corresponding (Docker)[quickstart-docker.rst] and (non-Docker)[quickstart-non-docker.rst] instructions for how to produce Json and Avro types of messages to the kafka cluster.

2. In the KSQL application, register the ``ksqlJson`` topic into KSQL, specifying the ``value_format`` of ``JSON``.

.. sourcecode:: bash

   ksql> REGISTER TOPIC ksqlJsonTopic WITH (kafka_topic='ksqlJson', value_format='JSON');

3. Create a KSQL stream from the registered Json Kafka topic.

.. sourcecode:: bash

   ksql> CREATE STREAM ksqlJsonStream (name varchar, id varchar) WITH (registered_topic='ksqlJsonTopic', key='id');

4. <TODO: Need KSQL-133 and KSQL-125> In the KSQL application, register the ``ksqlAvro`` topic into KSQL, specifying the ``value_format`` of ``Avro``.

.. sourcecode:: bash

   ksql> REGISTER TOPIC ksqlAvroTopic WITH (kafka_topic='ksqlAvro', value_format='Avro', avroschemafile='myavro.avsc');

5. Create a KSQL stream from the registered Avro Kafka topic.

.. sourcecode:: bash

   ksql> CREATE STREAM ksqlAvroStream (name varchar, id varchar) WITH (registered_topic='ksqlAvroTopic', key='id');

6. Proceed with any processing and data transformations as described earlier.



Exit KSQL
---------

1. <TODO: INSERT TERMINATE EXAMPLE>  <TODO: link to KSQL concepts guide, when is terminate relevant...is it only with workers?>

2. From the KSQL prompt ``ksql>``, type 'exit'.

.. sourcecode:: bash

  ksql> exit

