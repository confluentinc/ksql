.. _ksql_quickstart_non_docker:

Running KSQL Locally
====================

This topic describes how to setup a Kafka cluster and start KSQL in your local environment. After you complete these steps,
you can return to the :ref:`create-a-stream-and-table` and start querying the data in the Kafka cluster.

.. contents:: Contents
    :local:
    :depth: 1

Prerequisites
    - :ref:`Confluent Platform 4.0.0 <installation>` or later is installed. This installation includes a Kafka broker, ZooKeeper, Schema Registry, REST Proxy, and Kafka Connect.
    - If you installed Confluent Platform via TAR or ZIP, navigate into the installation directory. The paths and commands used throughout this quick start assume that your are in this installation directory.
    - `Maven <https://maven.apache.org/install.html>`__
    - `Git <https://git-scm.com/downloads>`__
    - Java: Minimum version 1.8. Install Oracle Java JRE or JDK >= 1.8 on your local machine

Start Kafka
-----------

Navigate to the ``confluent-3.3.0`` directory and start the Confluent
Platform using the new Confluent CLI (part of the free Confluent Open
Source distribution). ZooKeeper is listening on ``localhost:2181``,
Kafka broker is listening on ``localhost:9092``, and Confluent Schema
Registry is listening on ``localhost:8081``.

.. code:: bash

    $ <path-to-confluent>/bin/confluent start

Your output should resemble this.

.. code:: bash

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

1. Clone the Confluent KSQL repository.

   .. code:: bash

       $ git clone git@github.com:confluentinc/ksql.git

2. Change directory to the ``ksql`` directory and compile the code.

   .. code:: bash

       $ cd ksql
       $ mvn clean compile install -DskipTests

3. Start KSQL. The ``local`` argument starts KSQL in :ref:`standalone
   mode <modes-of-operation>`.

   .. code:: bash

       $ ./bin/ksql-cli local

   After you have successfully started the Kafka cluster and started
   KSQL, you will see the KSQL prompt:

   .. code:: bash

                          ======================================
                          =      _  __ _____  ____  _          =
                          =     | |/ // ____|/ __ \| |         =
                          =     | ' /| (___ | |  | | |         =
                          =     |  <  \___ \| |  | | |         =
                          =     | . \ ____) | |__| | |____     =
                          =     |_|\_\_____/ \___\_\______|    =
                          =                                    =
                          =   Streaming SQL Engine for Kafka   =
       Copyright 2017 Confluent Inc.

       CLI v0.2, Server v0.1 located at http://localhost:9098

       Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

       ksql>

See the steps below to generate data to the Kafka cluster.

.. _produce-topic-data:

Produce topic data
------------------

Minimally, to use the :ref:`ksql_quickstart`, you must run the following
steps to produce data to the Kafka topics ``pageviews`` and ``users``.

1. Produce Kafka data to the ``pageviews`` topic using the data
   generator. The following example continuously generates data with a
   value in DELIMITED format.

   .. code:: bash

       $ java -jar ksql-examples/target/ksql-examples-0.1-SNAPSHOT-standalone.jar \
           quickstart=pageviews format=delimited topic=pageviews maxInterval=10000

2. Produce Kafka data to the ``users`` topic using the data generator.
   The following example continuously generates data with a value in
   JSON format.

   .. code:: bash

       $ java -jar ksql-examples/target/ksql-examples-0.1-SNAPSHOT-standalone.jar \
           quickstart=users format=json topic=users maxInterval=10000

Optionally, you can return to the :ref:`main KSQL quick start
page <ksql_quickstart>` to start querying the Kafka
cluster. Or you can do additional testing with topic data produced from
the command line tools.

1. You can produce Kafka data with the Kafka command line
   ``kafka-console-producer``. The following example generates data with
   a value in DELIMITED format.

   .. code:: bash

       $ kafka-console-producer --broker-list localhost:9092  \
                                --topic t1 \
                                --property parse.key=true \
                                --property key.separator=:
       key1:v1,v2,v3
       key2:v4,v5,v6
       key3:v7,v8,v9
       key1:v10,v11,v12

2. This example generates data with a value in JSON format.

   .. code:: bash

       $ kafka-console-producer --broker-list localhost:9092 \
                                --topic t2 \
                                --property parse.key=true \
                                --property key.separator=:
       key1:{"id":"key1","col1":"v1","col2":"v2","col3":"v3"}
       key2:{"id":"key2","col1":"v4","col2":"v5","col3":"v6"}
       key3:{"id":"key3","col1":"v7","col2":"v8","col3":"v9"}
       key1:{"id":"key1","col1":"v10","col2":"v11","col3":"v12"}
