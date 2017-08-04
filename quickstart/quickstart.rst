.. _ksql_quickstart:

KSQL Quickstart
===============

**Table of Contents**

.. contents::
  :local:


Welcome to Confluent and Kafka Structured Query Language (KSQL)!  KSQL provides a structured query language to do processing on data stored in a Kafka cluster.

The goal of this quickstart guide is to guide you through a simple workflow to be able to query and transform KSQL data.


Setup
-----

Because KSQL queries data in a Kafka cluster, you will need to bring up a Kafka cluster, including ZooKeeper and a Kafka broker. Do not run KSQL against a production Kafka cluster while KSQL is in tech preview.

1. Bring up a Kafka cluster and start KSQL.

* Follow these instructions `if you are using Docker <quickstart-docker.rst>`__  (we recommend Docker for simplicity)
* Follow these instructions `if you are not using Docker <quickstart-non-docker.rst>`__

2. After you have successfully started the Kafka cluster and started KSQL, you will see the KSQL prompt:

.. sourcecode:: bash

   ksql>

3. KSQL provides a structured query language to query Kafka data, so you need some data to query. For this quickstart, you will produce mock data to the Kafka cluster.

* If you are using our Docker Compose files, a Docker container is already running with a data generator that is continuously producing Kafka messages to the Kafka cluster. No further action is required
* If you are not using our Docker environment, then follow these `instructions <quickstart-non-docker.rst#produce-topic-data>`__ to generate data to the Kafka cluster



Create a STREAM and TABLE
-------------------------

This KSQL quickstart shows examples querying data from Kafka topics called ``pageviews`` and ``users`` using the following schemas:

.. image:: https://github.com/ybyzek/ksql/blob/master/quickstart/ksql-quickstart-schemas.jpg
    :width: 200px
    
Before proceeding, please check:

* In the window where you started KSQL, you see the ``ksql>`` prompt
* If you are not using Docker, you must manually have run the data generator to produce topics called ``pageviews`` and ``users``. If you haven't done this, please follow these `instructions <quickstart-non-docker.rst#produce-topic-data>`__ to generate data. (Docker compose file automatically runs the data generator)


1. Create a STREAM ``pageviews_original`` from the Kafka topic ``pageviews``, specifying the ``value_format`` of ``DELIMITED``. Describe the new STREAM.  Notice that KSQL created additional columns called ``ROWTIME``, which corresponds to the Kafka message logstamp time, and ``ROWKEY``, which corresponds to the Kafka message key.

.. sourcecode:: bash

   ksql> CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH (kafka_topic='pageviews', value_format='DELIMITED');

   ksql> DESCRIBE pageviews_original;

       Field |   Type 
   -------------------
     ROWTIME |  INT64 
      ROWKEY | STRING 
    VIEWTIME |  INT64 
      PAGEID | STRING 
      USERID | STRING 

2. Create a TABLE ``users_original`` from the Kafka topic ``users``, specifying the ``value_format`` of ``JSON``. Describe the new TABLE.

.. sourcecode:: bash

   ksql> CREATE TABLE users_original (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (kafka_topic='users', value_format='JSON');

   ksql> DESCRIBE users_original;

        Field |   Type 
   -----------------------
      ROWTIME |  INT64 
       ROWKEY | STRING 
 REGISTERTIME |  INT64 
       USERID | STRING 
     REGIONID | STRING 
       GENDER | STRING 

3. Show all the KSQL STREAMS and TABLES. <TODO: update with KSQL-253>

.. sourcecode:: bash

   ksql> SHOW STREAMS;
   
           Stream Name |   Kafka Topic |    Format 
   ------------------------------------------------
              COMMANDS | app1_commands |      JSON 
    PAGEVIEWS_ORIGINAL |     pageviews | DELIMITED 

   ksql> SHOW TABLES;
   
        Table Name | Kafka Topic | Format | Windowed 
   --------------------------------------------------
    USERS_ORIGINAL |       USERS |   JSON |    false 


Write Queries
-------------

1. Create a non-persistent query that returns three data rows from a STREAM. Press ``<ctrl-c>`` to stop it. <TODO: KSQL-255: this should return after 3 records are reached>

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

3. Create a persistent query where a condition is met, using ``LIKE``. Write the query results to a Kafka topic called ``pageviews_enriched_r8_r9``.

.. sourcecode:: bash

   ksql> CREATE STREAM pageviews_female_like_89 WITH (kafka_topic='pageviews_enriched_r8_r9', value_format='DELIMITED') AS SELECT * FROM pageviews_female WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';

4. Create a persistent query that counts the pageviews for each region and gender combination in a `tumbling window <http://docs.confluent.io/current/streams/developer-guide.html#tumbling-time-windows>`__ of 30 seconds when the count is greater than 1.  <TODO: this does not work as expected.  Need to resolve KSQL-257, KSQL-260>

.. sourcecode:: bash

   ksql> CREATE TABLE pageviews_female_duplicates AS SELECT gender, regionid , COUNT(*) AS numusers FROM pageviews_female WINDOW TUMBLING (size 30 second) GROUP BY gender, regionid HAVING COUNT(*) > 1;

   ksql> DESCRIBE pageviews_female_duplicates;

       Field |   Type 
   -------------------
     ROWTIME |  INT64 
      ROWKEY | STRING 
      GENDER | STRING 
    REGIONID | STRING 
    NUMUSERS |  INT64 

5. Use ``SELECT`` to view the results any query as they come in. To stop viewing the query results, press `<ctrl-c>`. This stops printing to the console but it does not terminate the actual query. The query continues to run in the underyling Kafka Streams application.

.. sourcecode:: bash

   ksql> SELECT regionid, numusers FROM pageviews_female_duplicates;
   Region_3 | 4
   Region_3 | 5
   Region_6 | 5
   Region_6 | 6
   Region_3 | 8
   Region_1 | 2
   Region_1 | 3
   ...

6. Show all queries.  <TODO: update output>

.. sourcecode:: bash

   ksql> SHOW QUERIES;

   <TODO: INSERT show queries command when other issues are resolved>


Terminate and Exit
------------------

1. Until you terminate a query, it will run continuously as a Kafka Streams application. From the output of ``SHOW QUERIES;`` identify a query ID you would like to terminate. For example, if you wish to terminate query ID ``2``:

.. sourcecode:: bash

   ksql> terminate 2;

2. To exit from KSQL application, from the KSQL prompt ``ksql>``, type 'exit'.

.. sourcecode:: bash

  ksql> exit

