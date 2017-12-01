.. _ksql_quickstart_docker:

Running KSQL Using Docker
=========================

This topic describes how to setup a Kafka cluster and start KSQL in a Docker container. After you complete these steps,
you can return to the :ref:`create-a-stream-and-table` and start querying the data in the Kafka cluster.

.. contents:: Contents
    :local:
    :depth: 1

**Prerequisites**

- Docker
     - `macOS <https://docs.docker.com/docker-for-mac/install/>`__
     - `All platforms <https://docs.docker.com/engine/installation/>`__
- `Git <https://git-scm.com/downloads>`__
- Java: Minimum version 1.8

Start a Kafka cluster
---------------------

1. Clone the Confluent KSQL repository.

   .. code:: bash

       $ git clone git@github.com:confluentinc/ksql.git

2. Change directory to the quickstart and launch the KSQL quick start in
   Docker.

   .. code:: bash

       $ cd ksql/docs/quickstart/
       $ docker-compose up -d

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

Proceed to :ref:`start-ksql`.

.. _start-ksql:

Start KSQL
----------

1. From the host machine, start KSQL CLI on the container.

   .. code:: bash

       $ docker-compose exec ksql-cli ksql-cli local --bootstrap-server kafka:29092

2. Return to the :ref:`create-a-stream-and-table` to start querying the
   data in the Kafka cluster.

Appendix
--------

The following instructions in the Appendix are not required to run the
quick start. They are optional steps to produce extra topic data and
verify the environment.

Produce more topic data
~~~~~~~~~~~~~~~~~~~~~~~

The Compose file automatically runs a data generator that continuously
produces data to two Kafka topics ``pageviews`` and ``users``. No
further action is required if you want to use just the data available.
You can return to the :ref:`main KSQL quick
start <create-a-stream-and-table>` to start querying the
data in these two topics.

However, if you want to produce additional data, you can use any of the
following methods.

-  Produce Kafka data with the Kafka command line
   ``kafka-console-producer``. The following example generates data with
   a value in DELIMITED format.

   .. code:: bash

       $ docker-compose exec kafka kafka-console-producer --topic t1 --broker-list kafka:29092  --property parse.key=true --property key.separator=:

   Your data input should resemble this.

   .. code:: bash

       key1:v1,v2,v3
       key2:v4,v5,v6
       key3:v7,v8,v9
       key1:v10,v11,v12

-  Produce Kafka data with the Kafka command line
   ``kafka-console-producer``. The following example generates data with
   a value in JSON format.

   .. code:: bash

       $ docker-compose exec kafka kafka-console-producer --topic t2 --broker-list kafka:29092  --property parse.key=true --property key.separator=:

   Your data input should resemble this.

   .. code:: bash

       key1:{"id":"key1","col1":"v1","col2":"v2","col3":"v3"}
       key2:{"id":"key2","col1":"v4","col2":"v5","col3":"v6"}
       key3:{"id":"key3","col1":"v7","col2":"v8","col3":"v9"}
       key1:{"id":"key1","col1":"v10","col2":"v11","col3":"v12"}

Verify your environment
~~~~~~~~~~~~~~~~~~~~~~~

The next three steps are optional verification steps to ensure your
environment is properly setup.

1. Verify that six Docker containers were created.

   .. code:: bash

       $ docker-compose ps

   Your output should resemble this. Take note of the ``Up`` state.

   .. code:: bash

               Name                        Command               State                           Ports                          
       -------------------------------------------------------------------------------------------------------------------------
       quickstart_kafka_1                    /etc/confluent/docker/run        Up      0.0.0.0:29092->29092/tcp, 0.0.0.0:9092->9092/tcp       
       quickstart_ksql-cli_1                 perl -e while(1){ sleep 99 ...   Up                                                             
       quickstart_ksql-datagen-pageviews_1   bash -c echo Waiting for K ...   Up                                                             
       quickstart_ksql-datagen-users_1       bash -c echo Waiting for K ...   Up                                                             
       quickstart_schema-registry_1          /etc/confluent/docker/run        Up      0.0.0.0:8081->8081/tcp                                 
       quickstart_zookeeper_1                /etc/confluent/docker/run        Up      2181/tcp, 2888/tcp, 0.0.0.0:32181->32181/tcp, 3888/tcp         

2. The docker-compose file already runs a data generator that
   pre-populates two Kafka topics ``pageviews`` and ``users`` with mock
   data. Verify that the data generator created two Kafka topics,
   including ``pageviews`` and ``users``.

   .. code:: bash

       $ docker-compose exec kafka kafka-topics --zookeeper zookeeper:32181 --list

   Your output should resemble this.

   .. code:: bash

       _confluent-metrics
       _schemas
       pageviews
       users

3. Use the ``kafka-console-consumer`` to view a few messages from each
   topic. The topic ``pageviews`` has a key that is a mock time stamp
   and a value that is in ``DELIMITED`` format. The topic ``users`` has
   a key that is the user ID and a value that is in ``Json`` format.

   .. code:: bash

       $ docker-compose exec kafka kafka-console-consumer --topic pageviews --bootstrap-server kafka:29092 --from-beginning --max-messages 3 --property print.key=true

   Your output should resemble this.

   .. code:: bash

       1491040409254    1491040409254,User_5,Page_70
       1488611895904    1488611895904,User_8,Page_76
       1504052725192    1504052725192,User_8,Page_92

   .. code:: bash

       $ docker-compose exec kafka kafka-console-consumer --topic users --bootstrap-server kafka:29092 --from-beginning --max-messages 3 --property print.key=true

   Your output should resemble this.

   .. code:: bash

       User_2   {"registertime":1509789307038,"gender":"FEMALE","regionid":"Region_1","userid":"User_2"}
       User_6   {"registertime":1498248577697,"gender":"OTHER","regionid":"Region_8","userid":"User_6"}
       User_8   {"registertime":1494834474504,"gender":"MALE","regionid":"Region_5","userid":"User_8"}
