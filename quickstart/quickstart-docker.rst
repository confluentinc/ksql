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

1. Clone the Confluent KSQL repository:

<TODO: update when KSQL-200 is resolved>

2. Change into the quickstart directory.

<TODO: update when KSQL-200 is resolved>

.. sourcecode:: bash

   $ cd ksql/quickstart

3. Launch the KSQL quickstart in Docker

.. sourcecode:: bash

   $ docker-compose up -d


The next three steps are optional verification steps to ensure your environment is properly setup.

4. Verify six Docker containers were created:

.. sourcecode:: bash

   $ docker-compose ps
   <TODO: update when KSQL-185 is resolved>

            Name                        Command               State                           Ports                          
   -------------------------------------------------------------------------------------------------------------------------
   quickstart_kafka_1                    /etc/confluent/docker/run        Up      0.0.0.0:29092->29092/tcp, 0.0.0.0:9092->9092/tcp       
   quickstart_ksql-cli_1                 bash -c echo Waiting for K ...   Up      7070/tcp                                               
   quickstart_ksql-datagen-pageviews_1   bash -c echo Waiting for K ...   Up                                                             
   quickstart_ksql-datagen-users_1       bash -c echo Waiting for K ...   Up                                                             
   quickstart_schema-registry_1          /etc/confluent/docker/run        Up      0.0.0.0:8081->8081/tcp                                 
   quickstart_zookeeper_1                /etc/confluent/docker/run        Up      2181/tcp, 2888/tcp, 0.0.0.0:32181->32181/tcp, 3888/tcp 


5. The docker-compose file already runs a data generator that pre-populates two Kafka topics ``pageviews`` and ``users`` with mock data. Verify that the data generator created two Kafka topics, including ``pageviews`` and ``users``.

.. sourcecode:: bash

   $ docker-compose exec kafka kafka-topics --zookeeper zookeeper:32181 --list
   __consumer_offsets
   _confluent-metrics
   _schemas
   pageviews
   users

6. Use the ``kafka-console-consumer`` to view a few messages from each topic.  The topic ``pageviews`` has a key that is a mock timestamp and a value that is in ``DELIMITED`` format. The topic ``users`` has a key that is the user id and a value that is in ``Json`` format.

.. sourcecode:: bash

   $ docker-compose exec zookeeper kafka-console-consumer --topic pageviews --bootstrap-server kafka:29092 --from-beginning --max-messages 3 --property print.key=true
   1512423359573	1512423359573,User_81,Page_22
   1487894860785	1487894860785,User_44,Page_61
   1492825930409	1492825930409,User_79,Page_52
   ...

   $ docker-compose exec zookeeper kafka-console-consumer --topic users --bootstrap-server kafka:29092 --from-beginning --max-messages 3 --property print.key=true
   User_44	{"registertime":1516454306672,"gender":"MALE","regionid":"Region_9","userid":"User_44"}
   User_31	{"registertime":1501062988015,"gender":"OTHER","regionid":"Region_9","userid":"User_31"}
   User_31	{"registertime":1492682761137,"gender":"FEMALE","regionid":"Region_1","userid":"User_31"}
   ...


Start KSQL
----------

1. From the host machine, connect to the container and call KSQL in one command: <TODO: update when KSQL-185, KSQL-254 is resolved>

.. sourcecode:: bash

   $ docker-compose exec ksql-cli java -jar /app2/ksql-cli-1.0-SNAPSHOT-standalone.jar local --bootstrap-server kafka:29092

2. (Optional) You can use the argument ``--properties-file`` to specify a file to override any Kafka properties when starting KSQL.
For example, if you want to set ``auto.offset.reset=earliest``, you can override these settings as follows. NOTE: set ``auto.offset.reset=earliest`` if you want the STREAM or TABLE to process data already in the Kafka topic instead of just new data. Here is a sample properties file.

   .. sourcecode:: bash

   container$ cat /app2/cluster.properties
   auto.offset.reset=earliest

3. Return to the `main KSQL quickstart <quickstart.rst#create-a-stream-and-table>`__ to start querying the data in the Kafka cluster.


Produce more topic data
-----------------------

The docker-compose file automatically runs a data generator that continuously produces data to two Kafka topics ``pageviews`` and ``users``. No further action is required if you want to use just the data available. You can return to the `main KSQL quickstart <quickstart.rst#create-a-stream-and-table>`__ to start querying the data in these two topics.

However, if you want to produce additional data, you can use any of the following methods.

* Produce Kafka data with the Kafka commandline ``kafka-console-producer``. The following example generates data with a value in DELIMITED format

.. sourcecode:: bash

   $ docker-compose exec kafka kafka-console-producer --topic t1 --broker-list kafka:29092  --property parse.key=true --property key.separator=:
   key1:v1,v2,v3
   key2:v4,v5,v6
   key3:v7,v8,v9
   key1:v10,v11,v12

* Produce Kafka data with the Kafka commandline ``kafka-console-producer``. The following example generates data with a value in Json format

.. sourcecode:: bash

   $ docker-compose exec kafka kafka-console-producer --topic t2 --broker-list kafka:29092  --property parse.key=true --property key.separator=:
   key1:{"id":"key1","col1":"v1","col2":"v2","col3":"v3"}
   key2:{"id":"key2","col1":"v4","col2":"v5","col3":"v6"}
   key3:{"id":"key3","col1":"v7","col2":"v8","col3":"v9"}
   key1:{"id":"key1","col1":"v10","col2":"v11","col3":"v12"}

* If advanced Docker users want to run the data generator with different options, edit the Docker compile file and modify how the containers ``ksql-datagen-users`` and ``ksql-datagen-pageviews`` invoke the data generator.
