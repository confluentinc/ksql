.. _ksql_quickstart:

KSQL Quickstart
===============

**Table of Contents**

.. contents::
  :local:


Welcome to Confluent and Kafka Structured Query Language (KSQL)!

KSQL provides a structured query language to do processing on data stored in a Kafka cluster.

The goal of this quickstart guide is to guide you through a simple workflow to be able to query and transform KSQL data.


Setup
-----

1. Because KSQL queries data in a Kafka cluster, you will need access to a development Kafka cluster (with ZooKeeper, a Kafka broker, and optionally Confluent Schema Registry). Do not run KSQL against a production Kafka cluster while KSQL is in tech preview.

To run a Kafka development cluster and to start KSQL:
* If you are using a Docker environment, then follow [these instructions](quickstart-docker.rst) 
* If you are not using a Docker environment, then follow (these instructions)[quickstart-non-docker.rst]

Once you have completed the above steps, you will have a running Kafka cluster and you will have started KSQL. You will see the KSQL prompt:

.. sourcecode:: bash

   ksql>

2. KSQL provides a structured query language to query Kafka data, so you need some data to query. For this quickstart, you will produce mock data to the Kafka cluster.

* If you are using our Docker Compose files, a Docker container is already running with a data generator that is producing Kafka messages to your cluster. No further action is required
* If you are not not using our Docker environment, then follow [these instructions](quickstart-non-docker.rst#producedata) to generate data

<TODO: KSQL-205: data generator should pre-generate this data>

3. With KSQL running and data in your Kafka cluster ready for querying, you can proceed below.


Read Kafka topic data into KSQL
-------------------------------

Before proceeding, please check:
* You should now be at your ``ksql>`` prompt
* If you are not using Docker, you must have run the data generator to produce topics called ``pageviews`` and ``topics``. If you haven't done this, please follow these [instructions](quickstart-non-docker.rst#producedata) to generate data. (Docker compose file automatically runs the data generator)


1. Register the Kafka topic ``pageviews`` into KSQL, specifying the ``value_format`` of ``DELIMITED``, and view the contents of topic.

.. sourcecode:: bash

   ksql> REGISTER TOPIC pageviews WITH (kafka_topic='pageviews', value_format='DELIMITED');

2. Register the Kafka topic ``users`` into KSQL, specifying the ``value_format`` of ``JSON``, and view the contents of topic.

.. sourcecode:: bash

   ksql> REGISTER TOPIC users WITH (kafka_topic='users', value_format='JSON');

3. List all the Kafka topics on the Kafka broker. You will see all topics in the Kafka cluster, including ``pageviews`` and ``users`` which are marked as "registered" in KSQL. <TODO: upate _commands topic with appropriate name>

.. sourcecode:: bash

   ksql> show topics;
           Kafka Topic | Registered | Partitions | Partition Replicas 
   -------------------------------------------------------------------
         app1_commands |       true |          1 |                  1 
             pageviews |       true |          1 |                  1 
    _confluent-metrics |      false |         12 |                  1 
              _schemas |      false |          1 |                  1 
                 users |       true |          1 |                  1 

4. Print the contents of these topics. Notice that as part of the REGISTER command, KSQL created additional columns called ``ROWTIME``, which corresponds to the Kafka message logstamp time, and ``ROWKEY``, which corresponds to the Kafka message key value.  Press ``<Ctrl-c>`` to exit.

.. sourcecode:: bash

   ksql> PRINT pageviews;
   1501522918853 , 1494635607849 , 1494635607849,User_26,Page_56
   1501522921587 , 1508208297320 , 1508208297320,User_75,Page_44
   1501522924952 , 1498506236716 , 1498506236716,User_36,Page_53
   ...

   ksql> PRINT users;
   {"ROWTIME":1501522918922,"ROWKEY":"User_28","registertime":1488473173617,"gender":"OTHER","regionid":"Region_16","userid":"User_28"}
   {"ROWTIME":1501522927631,"ROWKEY":"User_77","registertime":1497218930240,"gender":"FEMALE","regionid":"Region_70","userid":"User_77"}
   {"ROWTIME":1501522933194,"ROWKEY":"User_96","registertime":1502724600263,"gender":"FEMALE","regionid":"Region_36","userid":"User_96"}

5. Create a KSQL stream ``pageviews_original`` from the registered Kafka topic ``pageviews``. Describe the stream. <TODO: Can we not REGISTER And CREATE STREAM in one command? KSQL-137>

.. sourcecode:: bash

   ksql> CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH (registered_topic = 'pageviews');
   ksql> describe pageviews_original;

       Field |   Type 
   -------------------
     ROWTIME |  INT64 
      ROWKEY | STRING 
    VIEWTIME |  INT64 
      PAGEID | STRING 
      USERID | STRING 

5. Create a KSQL table ``users_original`` from the registered Kafka topic ``users``.  Describe the table.

.. sourcecode:: bash

   ksql> CREATE TABLE users_original (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (registered_topic = 'users');
   ksql> DESCRIBE users_original;

        Field |   Type 
   -----------------------
      ROWTIME |  INT64 
       ROWKEY | STRING 
 REGISTERTIME |  INT64 
       USERID | STRING 
     REGIONID | STRING 
       GENDER | STRING 

7. Show all the KSQL STREAMS and TABLES. <TODO: update with KSQL-253>

.. sourcecode:: bash

   ksql> show streams;
   
        Stream Name |       Ksql Topic 
   ------------------------------------
           COMMANDS | __COMMANDS_TOPIC 
 PAGEVIEWS_ORIGINAL |        PAGEVIEWS 

   ksql> show tables;
   
    Table Name | Ksql Topic |            Statestore | Windowed 
   ------------------------------------------------------------
 USERS_ORIGINAL |      USERS | USERS_ORIGINAL_statestore |    false 



Query and transform KSQL data
-----------------------------

1. Create a non-persistent query that returns three data rows from a stream. Press ``<ctrl-c>`` to stop it. <TODO: KSQL-255: this should return after 3 records are reached>

.. sourcecode:: bash

   ksql> SELECT pageid FROM pageviews_original LIMIT 3;
   User_30
   User_73
   User_96

2. Create a persistent query by using the ``CREATE STREAM`` command to precede the ``SELECT`` statement. Unlike the non-persistent case above, results from this query will be produced to a Kafka topic ``pageviews_female``. This query enriches the pageviews STREAM by doing a ``JOIN`` with data in the users_original TABLE where a condition is met. <TODO: this currently errors out...Hojjat is looking into it>

.. sourcecode:: bash

   ksql> CREATE STREAM pageviews_female AS SELECT users_original.userid AS userid, pageid, regionid, gender FROM pageviews_original LEFT JOIN users_original ON pageviews_original.userid = users_original.userid WHERE gender = 'FEMALE';


                 Command ID |    Status |             Message 
   -----------------------------------------------------------
    stream/PAGEVIEWS_FEMALE | EXECUTING | Executing statement 

3. View the results of this query. This continuous query will keep on producing results as the stream processes incoming data, until you press `<ctrl-c>`.

.. sourcecode:: bash

   ksql> SELECT * FROM pageviews_female;

4. Create a persistent query where a condition is met, using ``LIKE``. Write the query results to a Kafka topic called ``pageviews_enriched_r8_r9``.

.. sourcecode:: bash

   ksql> CREATE STREAM pageviews_female_like_89 WITH (kafka_topic='pageviews_enriched_r8_r9', value_format='DELIMITED') AS SELECT * FROM pageviews_female WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';

5. Create a persistent query that counts the views for each reagion and gender combination for tumbling window of 15 seconds when the view count is greater than 5

.. sourcecode:: bash

   ksql> CREATE TABLE pageviews_grouping AS SELECT gender, regionid , count(*) from pageviews_female window tumbling (size 15 second) group by gender, regionid having count(*) > 5;

6. Show the newly created queries.  <TODO: update output>

.. sourcecode:: bash

   ksql> show queries;



Terminate and Exit
------------------

1. List all the Kafka topics on the Kafka broker. You will see some new topics that represent the persistent queries as well as the topics that Kafka Streams uses behind-the-scenes. including <TODO: insert topics>  

.. sourcecode:: bash

   ksql> show topics;
   <TODO: INSERT show topics command when KSQL-115 is implemented>

2. Until you terminate a query, it will run continuously as a Kafka streams application. From the output of ``show queries;`` identify a query ID you would like to terminate. For example, if you wish to terminate query ID ``4``:

.. sourcecode:: bash

   ksql> terminate 4;

3. To exit from KSQL application, from the KSQL prompt ``ksql>``, type 'exit'.

.. sourcecode:: bash

  ksql> exit

