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

1. Register the Kafka topic ``users`` into KSQL, specifying the ``value_format`` of ``JSON``, and view the contents of topic.

.. sourcecode:: bash

   ksql> REGISTER TOPIC users WITH (kafka_topic='users', value_format='JSON');

2. Print contents of these topics. Press ``<Ctrl-c>`` to exit.

   ksql> PRINT pageviews;
   <TODO: KSQL-165 earliest problem getting all values. Also KSQL-132, ctrl-c does not work>
   <TODO: insert output>

3. List all the Kafka topics on the Kafka broker. You will see all topics in the Kafka cluster, including ``pageviews`` and ``users`` which are marked as "registered" in KSQL.

.. sourcecode:: bash

   ksql> show topics;
   <TODO: INSERT show topics command when KSQL-115 is implemented>

4. Create a KSQL stream from the registered Kafka topic ``pageviews``. Describe and view the stream. <TODO: Can we not REGISTER And CREATE STREAM in one command? KSQL-137>

.. sourcecode:: bash

   ksql> CREATE STREAM pageviewsStream (viewtime bigint, pageid varchar, userid varchar) WITH (registered_topic = 'pageviews');

5. Create a KSQL table from the registered Kafka topic ``users``.  Describe and view the table.

.. sourcecode:: bash

   ksql> CREATE TABLE usersTable (registertime bigint, userid varchar, regionid varchar, gender varchar) WITH (registered_topic = 'users');

6. View the schemas of the newly created STREAM and TABLE. Notice that KSQL creates additional columns called ``ROWTIME`` and ``ROWKEY``.

.. sourcecode:: bash

   ksql> DESCRIBE pageviewsStream;
   <TODO: insert>

   ksql> DESCRIBE usersTable;
   <TODO: insert>

7. View all the KSQL STREAMS and TABLES.
   <TODO: insert output>

.. sourcecode:: bash

   ksql> show streams;

   ksql> show tables;



Query and transform KSQL data
-----------------------------

1. Create a non-persistent query that selects data from a stream. Press ``<ctrl-c>`` to stop it.

.. sourcecode:: bash

   ksql> SELECT users.userid AS userid, pageid, regionid, gender FROM pageviewsStream;

2. Create a persistent query that enriches the pageviews STREAM by doing a ``JOIN`` with data in the usersTable TABLE where a condition is met.

.. sourcecode:: bash

   ksql> CREATE STREAM pageviews_enriched AS SELECT usersTable.userid AS userid, pageid, regionid, gender FROM pageviewsStream LEFT JOIN usersTable ON pageviewsStream.userid = usersTable.userid WHERE gender = 'FEMALE';

3. Show the newly created query

.. sourcecode:: bash

   ksql> show queries;

4. Get the results of the queries. These will continue to produce results as the streams process newly incoming data, until you press `<ctrl-c>`.

   <TODO: insert output>

5. Create a persistent query where a condition is met, using ``LIKE``. Write the query results to a Kafka topic called ``pageviews_enriched_r8_r9``.

.. sourcecode:: bash

   ksql> CREATE STREAM enrichedpv_female_r8_r9 WITH (kafka_topic='pageviews_enriched_r8_r9', value_format='DELIMITED') AS SELECT * FROM enrichedpv_female WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';

6. List all the Kafka topics on the Kafka broker. You will see some new topics including <TODO: insert names>

.. sourcecode:: bash

   ksql> show topics;
   <TODO: INSERT show topics command when KSQL-115 is implemented>

7. Create a persistent query that counts the views for each reagion and gender combination for tumbling window of 15 seconds when the view count is greater than 5

.. sourcecode:: bash

   ksql> CREATE TABLE pvcount_gender_region AS SELECT gender, regionid , count(*) from pageviews_enriched window tumbling (size 15 second) group by gender, regionid having count(*) > 5;



Exit KSQL
---------

1. <TODO: INSERT TERMINATE EXAMPLE>  <TODO: link to KSQL concepts guide, when is terminate relevant...is it only with workers?>

2. From the KSQL prompt ``ksql>``, type 'exit'.

.. sourcecode:: bash

  ksql> exit

