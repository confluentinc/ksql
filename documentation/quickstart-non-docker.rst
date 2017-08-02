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

You will need to download and install a Kafka cluster on your local machine.  This cluster consists of a single Kafka broker along with a single-node ZooKeeper ensemble and an optional single Schema Registry instance.

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

1. Download the KSQL jar file <TODO: insert download link>. Start KSQL, using the ``local`` argument for the tech preview KSQL release because it starts the KSQL engine locally. <TODO: update how ksql is invoked with KSQL-254>

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

The KSQL quickstart assumes you have run at least the following three steps to produce data to two Kafka topics ``pageviews`` and ``users`` in your Kafka cluster.

1. Download the data generator jar file <TODO: insert download link>

2. Produce Kafka data to a topic ``pageviews`` using the provided data generator. The following example continuously generates data with a value in DELIMITED format

.. sourcecode:: bash

   $ java -jar /app2/ksql-examples-1.0-SNAPSHOT-standalone.jar quickstart=pageview format=delimited topic=pageviews maxInterval=10000

3. Produce Kafka data to a topic ``users`` using the provided data generator. The following example continuously generates data with a value in Json format

   .. sourcecode:: bash

   $ java -jar /app2/ksql-examples-1.0-SNAPSHOT-standalone.jar quickstart=users format=json topic=users maxInterval=10000

At this point you may return to the `main KSQL quickstart page <quickstart.rst#create-a-stream-and-table>`__ to start querying the Kafka cluster. If you would like to do additional testing with topic data produced from the commandline tools: 

4. You can produce Kafka data with the Kafka commandline ``kafka-console-producer``. The following example generates data with a value in DELIMITED format

.. sourcecode:: bash

   $ kafka-console-producer --topic t1 --broker-list localhost:9092  --property parse.key=true --property key.separator=:
   key1:v1,v2,v3
   key2:v4,v5,v6
   key3:v7,v8,v9
   key1:v10,v11,v12

5. The following example generates data with a value in Json format

.. sourcecode:: bash

   $ kafka-console-producer --topic t2 --broker-list localhost:9092  --property parse.key=true --property key.separator=:
   key1:{"id":"key1","col1":"v1","col2":"v2","col3":"v3"}
   key2:{"id":"key2","col1":"v4","col2":"v5","col3":"v6"}
   key3:{"id":"key3","col1":"v7","col2":"v8","col3":"v9"}
   key1:{"id":"key1","col1":"v10","col2":"v11","col3":"v12"}
