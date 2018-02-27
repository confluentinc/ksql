.. _ksql_quickstart:

Quick Start
===========

.. important::

    This release is a developer preview. It is strongly recommended that you test before running KSQL against a production Kafka cluster.

The goal of this quick start is to demonstrate a simple workflow using
KSQL to write streaming queries against messages in Kafka.

.. contents::
    :depth: 2

Setup
-----

.. toctree::
    :hidden:

    quickstart-non-docker
    quickstart-docker

To get started, you must start a Kafka cluster, including ZooKeeper and a Kafka broker. KSQL will then query messages from this Kafka cluster.

Launch a Kafka cluster, start KSQL, and produce messages to query. Follow the instructions based on whether you are using Docker:

:ref:`ksql_quickstart_docker` (recommended)
    The Docker container starts a Kafka cluster, starts KSQL, and automatically runs a data generator that continuously produces Kafka messages to the Kafka cluster. No additional setup is required.

:ref:`ksql_quickstart_non_docker`
    With this method you start a Kafka cluster, start KSQL, and manually run a data generator to produce topics called ``pageviews`` and ``users``

.. _create-a-stream-and-table:

Create a Stream and Table
-------------------------

These examples query messages from Kafka topics called ``pageviews`` and ``users`` using the following schemas:

.. image:: ../img/ksql-quickstart-schemas.jpg

#. Create a STREAM ``pageviews_original`` from the Kafka topic
   ``pageviews``, specifying the ``value_format`` of ``DELIMITED``.
   Describe the new STREAM. Notice that KSQL created additional columns
   called ``ROWTIME``, which corresponds to the Kafka message timestamp,
   and ``ROWKEY``, which corresponds to the Kafka message key.

   .. code:: bash 

        ksql> CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH (kafka_topic='pageviews', value_format='DELIMITED');

        ksql> DESCRIBE pageviews_original;

         Field    | Type
        ----------------------------
         ROWTIME  | BIGINT
         ROWKEY   | VARCHAR(STRING)
         VIEWTIME | BIGINT
         USERID   | VARCHAR(STRING)
         PAGEID   | VARCHAR(STRING)

#. Create a TABLE ``users_original`` from the Kafka topic ``users``,
   specifying the ``value_format`` of ``JSON``. Describe the new TABLE.

   .. code:: bash 

    ksql> CREATE TABLE users_original (registertime BIGINT, gender VARCHAR, regionid VARCHAR, userid VARCHAR) WITH (kafka_topic='users', value_format='JSON', key = 'userid');

    ksql> DESCRIBE users_original;

     Field        | Type
    --------------------------------
     ROWTIME      | BIGINT
     ROWKEY       | VARCHAR(STRING)
     REGISTERTIME | BIGINT
     GENDER       | VARCHAR(STRING)
     REGIONID     | VARCHAR(STRING)
     USERID       | VARCHAR(STRING)

#. Show all STREAMS and TABLES.

   .. code:: bash

       ksql> SHOW STREAMS;

        Stream Name              | Kafka Topic              | Format
       -----------------------------------------------------------------
        PAGEVIEWS_ORIGINAL       | pageviews                | DELIMITED 

       ksql> SHOW TABLES;

        Table Name        | Kafka Topic       | Format    | Windowed 
       --------------------------------------------------------------
        USERS_ORIGINAL    | users             | JSON      | false   

Write Queries
-------------

These examples write queries using KSQL.

**Note:** By default KSQL reads the topics for streams and tables from
the latest offset.

#. Use ``SELECT`` to create a query that returns data from a STREAM. To
   stop viewing the data, press ``<ctrl-c>``. You may optionally include
   the ``LIMIT`` keyword to limit the number of rows returned in the
   query result. Note that exact data output may vary because of the
   randomness of the data generation.

   .. code:: bash

       ksql> SELECT pageid FROM pageviews_original LIMIT 3;
       Page_24
       Page_73
       Page_78
       LIMIT reached for the partition.
       Query terminated
       ksql>

#. Create a persistent query by using the ``CREATE STREAM`` keywords to
   precede the ``SELECT`` statement. Unlike the non-persistent query
   above, results from this query are written to a Kafka topic
   ``PAGEVIEWS_ENRICHED``. The query below enriches the ``pageviews``
   STREAM by doing a ``LEFT JOIN`` with the ``users_original`` TABLE on
   the user ID.

   .. code:: bash

    ksql> CREATE STREAM pageviews_enriched AS SELECT users_original.userid AS userid, pageid, regionid, gender FROM pageviews_original LEFT JOIN users_original ON pageviews_original.userid = users_original.userid;

    ksql> DESCRIBE pageviews_enriched;

    Field    | Type
    --------------------------------------
     ROWTIME  | BIGINT           (system)
     ROWKEY   | VARCHAR(STRING)  (system)
     USERID   | VARCHAR(STRING)  (key)
     PAGEID   | VARCHAR(STRING)
     REGIONID | VARCHAR(STRING)
     GENDER   | VARCHAR(STRING)
    --------------------------------------

#. Use ``SELECT`` to view query results as they come in. To stop viewing
   the query results, press ``<ctrl-c>``. This stops printing to the
   console but it does not terminate the actual query. The query
   continues to run in the underlying KSQL application.

   .. code:: bash

       ksql> SELECT * FROM pageviews_enriched;
       1519746861328 | User_4 | User_4 | Page_58 | Region_5 | OTHER
       1519746861794 | User_9 | User_9 | Page_94 | Region_9 | MALE
       1519746862164 | User_1 | User_1 | Page_90 | Region_7 | FEMALE
       ^CQuery terminated
       ksql>

#. Create a new persistent query where a condition limits the streams content, using
   ``WHERE``. Results from this query are written to a Kafka topic called
   ``PAGEVIEWS_FEMALE``.

   .. code:: bash 

    ksql> CREATE STREAM pageviews_female AS SELECT * FROM pageviews_enriched WHERE gender = 'FEMALE';

    ksql> DESCRIBE pageviews_female;

     Field    | Type
    --------------------------------------
     ROWTIME  | BIGINT           (system)
     ROWKEY   | VARCHAR(STRING)  (system)
     USERID   | VARCHAR(STRING)  (key)
     PAGEID   | VARCHAR(STRING)
     REGIONID | VARCHAR(STRING)
     GENDER   | VARCHAR(STRING)
    --------------------------------------

#. Create a new persistent query where another condition is met, using
   ``LIKE``. Results from this query are written to a Kafka topic called
   ``pageviews_enriched_r8_r9``.

   .. code:: bash

       ksql> CREATE STREAM pageviews_female_like_89 WITH (kafka_topic='pageviews_enriched_r8_r9', value_format='DELIMITED') AS SELECT * FROM pageviews_female WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';

#. Create a new persistent query that counts the pageviews for each
   region and gender combination in a `tumbling
   window <http://docs.confluent.io/current/streams/developer-guide.html#tumbling-time-windows>`__
   of 30 seconds when the count is greater than 1. Results from this
   query are written to a Kafka topic called ``PAGEVIEWS_REGIONS`` in the Avro format.
   KSQL will register the avro schema with the configured schema registry
   when it writes the first message to the ``PAGEVIEWS_REGIONS`` topic.

   .. code:: bash 

    ksql> CREATE TABLE pageviews_regions WITH (value_format='avro') AS SELECT gender, regionid , COUNT(*) AS numusers FROM pageviews_enriched WINDOW TUMBLING (size 30 second) GROUP BY gender, regionid HAVING COUNT(*) > 1;

    ksql> DESCRIBE pageviews_regions;

     Field    | Type
    --------------------------------------
     ROWTIME  | BIGINT           (system)
     ROWKEY   | VARCHAR(STRING)  (system)
     GENDER   | VARCHAR(STRING)  (key)
     REGIONID | VARCHAR(STRING)  (key)
     NUMUSERS | BIGINT
    --------------------------------------

#. Use ``SELECT`` to view results from the above query.

   .. code:: bash

       ksql> SELECT gender, regionid, numusers FROM pageviews_regions LIMIT 5;
       FEMALE | Region_6 | 3
       FEMALE | Region_1 | 4
       FEMALE | Region_9 | 6
       MALE | Region_8 | 2
       OTHER | Region_5 | 4
       LIMIT reached for the partition.
       Query terminated
       ksql> 

#.  Show all persistent queries.

    .. code:: bash

        ksql> SHOW QUERIES;

        Query ID                      | Kafka Topic              | Query String
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        CTAS_PAGEVIEWS_REGIONS        | PAGEVIEWS_REGIONS        | CREATE TABLE pageviews_regions WITH (value_format='avro') AS SELECT gender, regionid , COUNT(*) AS numusers FROM pageviews_enriched WINDOW TUMBLING (size 30 second) GROUP BY gender, regionid HAVING COUNT(*) > 1;
        CSAS_PAGEVIEWS_ENRICHED       | PAGEVIEWS_ENRICHED       | CREATE STREAM pageviews_enriched AS SELECT users_original.userid AS userid, pageid, regionid, gender FROM pageviews_original LEFT JOIN users_original ON pageviews_original.userid = users_original.userid;
        CSAS_PAGEVIEWS_FEMALE         | PAGEVIEWS_FEMALE         | CREATE STREAM pageviews_female AS SELECT * FROM pageviews_enriched WHERE gender = 'FEMALE';
        CSAS_PAGEVIEWS_FEMALE_LIKE_89 | pageviews_enriched_r8_r9 | CREATE STREAM pageviews_female_like_89 WITH (kafka_topic='pageviews_enriched_r8_r9', value_format='DELIMITED') AS SELECT * FROM pageviews_female WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Terminate and Exit
------------------

KSQL
~~~~

**Important:** Queries will continuously run as KSQL applications until
they are manually terminated. Exiting KSQL does not terminate persistent
queries.

#. From the output of ``SHOW QUERIES;`` identify a query ID you would
   like to terminate. For example, if you wish to terminate query ID
   ``CTAS_PAGEVIEWS_REGIONS``:

   .. code:: bash

       ksql> TERMINATE CTAS_PAGEVIEWS_REGIONS;

#. To exit from KSQL, type ‘exit’.

   .. code:: bash

       ksql> exit

Docker
~~~~~~

If you are running Docker Compose, you must explicitly shut down Docker
Compose. For more information, see the `docker-compose
down <https://docs.docker.com/compose/reference/down/>`__ documentation.

**Important:** This command will delete all KSQL queries and topic data.

.. code:: bash 

    $ docker-compose down

Confluent Platform
~~~~~~~~~~~~~~~~~~

If you are running the Confluent Platform, you can stop it with this
command.

.. code:: bash

    $ confluent stop

Next steps
----------

Try the end-to-end :ref:`Clickstream Analysis
demo <ksql_clickstream>`, which shows how
to build an application that performs real-time user analytics.
