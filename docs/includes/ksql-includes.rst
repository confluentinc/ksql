.. _offsetreset_start

.. tip:: Run the following to tell KSQL to read from the `beginning` of the topic: 

    .. code:: bash

        ksql> SET 'auto.offset.reset' = 'earliest';

    `You can skip this if you have already run it within your current`
    `KSQL CLI session.`

.. _offsetreset_end

.. Avro_note_start

.. note::
    - To use Avro, you must have |sr| enabled and ``ksql.schema.registry.url`` must be set in the KSQL
      server configuration file. See :ref:`install_ksql-avro-schema`.
    - Avro field names are not case sensitive in KSQL. This matches the KSQL column name behavior.

.. Avro_note_end

.. demo_start

Learn More
    Watch the `screencast of the KSQL demo <https://www.youtube.com/embed/illEpCOcCVg>`_ on YouTube.

    .. raw:: html

          <div style="position: relative; padding-bottom: 56.25%; height: 0; overflow: hidden; max-width: 100%; height: auto;">
              <iframe src="https://www.youtube.com/embed/illEpCOcCVg" frameborder="0" allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe>
          </div>

.. demo_end

.. CLI_welcome_start

.. code:: bash

                          ===========================================
                          =        _  __ _____  ____  _             =
                          =       | |/ // ____|/ __ \| |            =
                          =       | ' /| (___ | |  | | |            =
                          =       |  <  \___ \| |  | | |            =
                          =       | . \ ____) | |__| | |____        =
                          =       |_|\_\_____/ \___\_\______|       =
                          =                                         =
                          =  Streaming SQL Engine for Apache Kafka® =
                          ===========================================

        Copyright 2018 Confluent Inc.

        CLI v|release|, Server v|release| located at http://localhost:8088

        Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

        ksql>

.. CLI_welcome_end

.. basics_tutorial_01_start

------------------------------
Create Topics and Produce Data
------------------------------

Create and produce data to the Kafka topics ``pageviews`` and ``users``. These steps use the KSQL datagen that is included
|cp|.

1. Create the ``pageviews`` topic and produce data using the data generator. The following example continuously generates data with a
   value in DELIMITED format.

   .. code:: bash

       $ <path-to-confluent>/bin/ksql-datagen quickstart=pageviews format=delimited topic=pageviews maxInterval=500

2. Produce Kafka data to the ``users`` topic using the data generator. The following example continuously generates data with a value in
   JSON format.

   .. code:: bash

       $ <path-to-confluent>/bin/ksql-datagen quickstart=users format=json topic=users maxInterval=100

.. tip:: You can also produce Kafka data using the ``kafka-console-producer`` CLI provided with |cp|.

-------------------
Launch the KSQL CLI
-------------------
To launch the CLI, run the following command. It will route the CLI logs to the ``./ksql_logs`` directory, relative to
your current directory. By default, the CLI will look for a KSQL Server running at ``http://localhost:8088``.

.. code:: bash

   $ LOG_DIR=./ksql_logs <path-to-confluent>/bin/ksql

.. basics_tutorial_01_end

.. basics_tutorial_02_start

After KSQL is started, your terminal should resemble this.

.. include:: ../includes/ksql-includes.rst
  :start-after: CLI_welcome_start
  :end-before: CLI_welcome_end

.. basics_tutorial_02_end

.. basics_tutorial_03_start

.. _create-a-stream-and-table:

-------------------------
Create a Stream and Table
-------------------------

These examples query messages from Kafka topics called ``pageviews`` and ``users`` using the following schemas:

.. image:: ../img/ksql-quickstart-schemas.jpg


#. Create a stream ``pageviews_original`` from the Kafka topic ``pageviews``, specifying the ``value_format`` of ``DELIMITED``.

   Describe the new STREAM. Notice that KSQL created additional columns called ``ROWTIME``, which corresponds to the Kafka message timestamp,
   and ``ROWKEY``, which corresponds to the Kafka message key.

   .. code:: bash

        ksql> CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH \
        (kafka_topic='pageviews', value_format='DELIMITED');

   Your output should resemble:

   .. code:: bash

         Message
        ---------------
         Stream created
        ---------------

   .. tip:: You can run ``DESCRIBE pageviews_original;`` to see the schema for the Stream.

#. Create a table ``users_original`` from the Kafka topic ``users``, specifying the ``value_format`` of ``JSON``.

   .. code:: bash

    ksql> CREATE TABLE users_original (registertime BIGINT, gender VARCHAR, regionid VARCHAR, userid VARCHAR) WITH \
    (kafka_topic='users', value_format='JSON', key = 'userid');

   Your output should resemble:

   .. code:: bash

         Message
        ---------------
         Table created
        ---------------

   .. tip:: You can run ``DESCRIBE users_original;`` to see the schema for the Table. 

#. Optional: Show all streams and tables.

   .. code:: bash

       ksql> SHOW STREAMS;

        Stream Name              | Kafka Topic              | Format
       -----------------------------------------------------------------
        PAGEVIEWS_ORIGINAL       | pageviews                | DELIMITED

       ksql> SHOW TABLES;

        Table Name        | Kafka Topic       | Format    | Windowed
       --------------------------------------------------------------
        USERS_ORIGINAL    | users             | JSON      | false

-------------
Write Queries
-------------

These examples write queries using KSQL.

**Note:** By default KSQL reads the topics for streams and tables from
the latest offset.

#. Use ``SELECT`` to create a query that returns data from a STREAM. This query includes the ``LIMIT`` keyword to limit
   the number of rows returned in the query result. Note that exact data output may vary because of the randomness of the data generation.

   .. code:: sql

       ksql> SELECT pageid FROM pageviews_original LIMIT 3;

   Your output should resemble:

   .. code:: bash

       Page_24
       Page_73
       Page_78
       LIMIT reached
       Query terminated

#. Create a *persistent query* by using the ``CREATE STREAM`` keywords to precede the ``SELECT`` statement. The continual results from this query are written to the ``PAGEVIEWS_ENRICHED`` Kafka topic. The following query enriches the ``pageviews`` STREAM by doing a ``LEFT JOIN`` with the ``users_original`` TABLE on the user ID.

   .. code:: sql

    ksql> CREATE STREAM pageviews_enriched AS \
          SELECT users_original.userid AS userid, pageid, regionid, gender \
          FROM pageviews_original \
          LEFT JOIN users_original \
            ON pageviews_original.userid = users_original.userid;

   Your output should resemble:

   .. code:: bash

         Message
        ----------------------------
         Stream created and running
        ----------------------------

   .. tip:: You can run ``DESCRIBE pageviews_enriched;`` to describe the stream.

#. Use ``SELECT`` to view query results as they come in. To stop viewing the query results, press ``<ctrl-c>``. This stops printing to the
   console but it does not terminate the actual query. The query continues to run in the underlying KSQL application.

   .. code:: sql

       ksql> SELECT * FROM pageviews_enriched;

   Your output should resemble:

   .. code:: bash

       1519746861328 | User_4 | User_4 | Page_58 | Region_5 | OTHER
       1519746861794 | User_9 | User_9 | Page_94 | Region_9 | MALE
       1519746862164 | User_1 | User_1 | Page_90 | Region_7 | FEMALE
       ^CQuery terminated

#. Create a new persistent query where a condition limits the streams content, using ``WHERE``. Results from this query
   are written to a Kafka topic called ``PAGEVIEWS_FEMALE``.

   .. code:: sql

    ksql> CREATE STREAM pageviews_female AS \
          SELECT * FROM pageviews_enriched \
          WHERE gender = 'FEMALE';

   Your output should resemble:

   .. code:: bash

         Message
        ----------------------------
         Stream created and running
        ----------------------------

   .. tip:: You can run ``DESCRIBE pageviews_female;`` to describe the stream.

#. Create a new persistent query where another condition is met, using ``LIKE``. Results from this query are written to the
   ``pageviews_enriched_r8_r9`` Kafka topic.

   .. code:: sql

       ksql> CREATE STREAM pageviews_female_like_89 \
               WITH (kafka_topic='pageviews_enriched_r8_r9') AS \
             SELECT * FROM pageviews_female \
             WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';

   Your output should resemble:

   .. code:: bash

         Message
        ----------------------------
         Stream created and running
        ----------------------------

#. Create a new persistent query that counts the pageviews for each region and gender combination in a
   :ref:`tumbling window <windowing-tumbling>` of 30 seconds when the count is greater than one. Results from this query
   are written to the ``PAGEVIEWS_REGIONS`` Kafka topic in the Avro format. KSQL will register the Avro schema with the
   configured schema registry when it writes the first message to the ``PAGEVIEWS_REGIONS`` topic.

   .. code:: sql

    ksql> CREATE TABLE pageviews_regions \
            WITH (VALUE_FORMAT='avro') AS \
          SELECT gender, regionid , COUNT(*) AS numusers \
          FROM pageviews_enriched \
            WINDOW TUMBLING (size 30 second) \
          GROUP BY gender, regionid \
          HAVING COUNT(*) > 1;

   Your output should resemble:

   .. code:: bash

         Message
        ---------------------------
         Table created and running
        ---------------------------

   .. tip:: You can run ``DESCRIBE pageviews_regions;`` to describe the table.

#. Optional: View results from the above queries using ``SELECT``.

   .. code:: sql

       ksql> SELECT gender, regionid, numusers FROM pageviews_regions LIMIT 5;

   Your output should resemble:

   .. code:: bash

       FEMALE | Region_6 | 3
       FEMALE | Region_1 | 4
       FEMALE | Region_9 | 6
       MALE | Region_8 | 2
       OTHER | Region_5 | 4
       LIMIT reached
       Query terminated
       ksql>

#.  Optional: Show all persistent queries.

    .. code:: sql

        ksql> SHOW QUERIES;

    Your output should resemble:

    .. code:: bash

        Query ID                        | Kafka Topic              | Query String
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        CSAS_PAGEVIEWS_FEMALE_1         | PAGEVIEWS_FEMALE         | CREATE STREAM pageviews_female AS       SELECT * FROM pageviews_enriched       WHERE gender = 'FEMALE';
        CTAS_PAGEVIEWS_REGIONS_3        | PAGEVIEWS_REGIONS        | CREATE TABLE pageviews_regions         WITH (VALUE_FORMAT='avro') AS       SELECT gender, regionid , COUNT(*) AS numusers       FROM pageviews_enriched         WINDOW TUMBLING (size 30 second)       GROUP BY gender, regionid       HAVING COUNT(*) > 1;
        CSAS_PAGEVIEWS_FEMALE_LIKE_89_2 | PAGEVIEWS_FEMALE_LIKE_89 | CREATE STREAM pageviews_female_like_89         WITH (kafka_topic='pageviews_enriched_r8_r9') AS       SELECT * FROM pageviews_female       WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';
        CSAS_PAGEVIEWS_ENRICHED_0       | PAGEVIEWS_ENRICHED       | CREATE STREAM pageviews_enriched AS       SELECT users_original.userid AS userid, pageid, regionid, gender       FROM pageviews_original       LEFT JOIN users_original         ON pageviews_original.userid = users_original.userid;
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        For detailed information on a Query run: EXPLAIN <Query ID>;

#.  Optional: Examine query run-time metrics and details. Observe that information including 
    the target Kafka topic is available, as well as throughput figures for the messages being processed.

    .. code:: sql

        ksql> DESCRIBE EXTENDED PAGEVIEWS_REGIONS;

    Your output should resemble:

    .. code:: bash

        Name                 : PAGEVIEWS_REGIONS
        Type                 : TABLE
        Key field            : KSQL_INTERNAL_COL_0|+|KSQL_INTERNAL_COL_1
        Key format           : STRING
        Timestamp field      : Not set - using <ROWTIME>
        Value format         : AVRO
        Kafka topic          : PAGEVIEWS_REGIONS (partitions: 4, replication: 1)

        Field    | Type
        --------------------------------------
        ROWTIME  | BIGINT           (system)
        ROWKEY   | VARCHAR(STRING)  (system)
        GENDER   | VARCHAR(STRING)
        REGIONID | VARCHAR(STRING)
        NUMUSERS | BIGINT
        --------------------------------------

        Queries that write into this TABLE
        -----------------------------------
        CTAS_PAGEVIEWS_REGIONS_3 : CREATE TABLE pageviews_regions         WITH (value_format='avro') AS       SELECT gender, regionid , COUNT(*) AS numusers       FROM pageviews_enriched         WINDOW TUMBLING (size 30 second)       GROUP BY gender, regionid       HAVING COUNT(*) > 1;

        For query topology and execution plan please run: EXPLAIN <QueryId>

        Local runtime statistics
        ------------------------
        messages-per-sec:      3.06   total-messages:      1827     last-message: 7/19/18 4:17:55 PM UTC
        failed-messages:         0 failed-messages-per-sec:         0      last-failed:       n/a
        (Statistics of the local KSQL server interaction with the Kafka topic PAGEVIEWS_REGIONS)
        ksql>

.. basics_tutorial_03_end 

.. terminate_and_exit__start

------------------
Terminate and Exit
------------------

KSQL
----

**Important:** Persisted queries will continuously run as KSQL applications until
they are manually terminated. Exiting KSQL CLI does not terminate persistent
queries.

#. From the output of ``SHOW QUERIES;`` identify a query ID you would
   like to terminate. For example, if you wish to terminate query ID
   ``CTAS_PAGEVIEWS_REGIONS``:

   .. code:: bash

       ksql> TERMINATE CTAS_PAGEVIEWS_REGIONS;

   .. tip:: The actual name of the query running may vary; refer to the output of ``SHOW QUERIES;``.

#. Run this command to exit the KSQL CLI.

   .. code:: bash

       ksql> exit

.. terminate_and_exit__end

.. enable_JMX_metrics_start

To enable JMX metrics, set ``JMX_PORT`` before starting the KSQL server:

.. code:: bash

    $ export JMX_PORT=1099 && \
      <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

.. enable_JMX_metrics_end

.. log_limitations_start

.. important:: By default KSQL attempts to store its logs in a directory called ``logs`` that is relative to the location
               of the ``ksql`` executable. For example, if ``ksql`` is installed at ``/usr/local/bin/ksql``, then it would
               attempt to store its logs in ``/usr/local/logs``. If you are running ``ksql`` from the default |cp|
               location, ``<path-to-confluent>/bin``, you must override this default behavior by using the ``LOG_DIR`` variable.
.. log_limitations_qs_end
               For example, to store your logs in the ``ksql_logs`` directory within your current working directory, run this
               command when starting the KSQL CLI:

               .. code:: bash

                    $ LOG_DIR=./ksql_logs <path-to-confluent>/bin/ksql

.. log_limitations_end

.. __struct_support_01_start

Using Nested Schemas (STRUCT) in KSQL
=====================================

Struct support enables the modeling and access of nested data in Kafka
topics, from both JSON and Avro.

Here we’ll use the ``ksql-datagen`` tool to create some sample data
which includes a nested ``address`` field. Run this in a new window, and 
leave it running. 

.. __struct_support_01_end

.. __struct_support_02_start

From the KSQL command prompt, register the topic in KSQL:

.. code:: sql

    ksql> CREATE STREAM ORDERS WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='AVRO');

Your output should resemble:

.. code:: bash

     Message
    ----------------
     Stream created
    ----------------

Use the ``DESCRIBE`` function to observe the schema, which includes a
``STRUCT``:

.. code:: sql

    ksql> DESCRIBE ORDERS;

Your output should resemble:

.. code:: bash

    Name                 : ORDERS
     Field      | Type
    ----------------------------------------------------------------------------------
     ROWTIME    | BIGINT           (system)
     ROWKEY     | VARCHAR(STRING)  (system)
     ORDERTIME  | BIGINT
     ORDERID    | INTEGER
     ITEMID     | VARCHAR(STRING)
     ORDERUNITS | DOUBLE
     ADDRESS    | STRUCT<CITY VARCHAR(STRING), STATE VARCHAR(STRING), ZIPCODE BIGINT>
    ----------------------------------------------------------------------------------
    For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
    ksql>

Query the data, using ``->`` notation to access the Struct contents:

.. code:: sql

    ksql> SELECT ORDERID, ADDRESS->CITY FROM ORDERS;

Your output should resemble:

.. code:: bash

    0 | City_35
    1 | City_21
    2 | City_47
    3 | City_57
    4 | City_17

Press Ctrl-C to cancel the ``SELECT`` query. 


.. __struct_support_02_end

.. __stream_stream_join:

.. __ss-join_01_start

Stream-Stream join
==================

Using a stream-stream join, it is possible to join two *streams* of
events on a common key. An example of this could be a stream of order
events, and a stream of shipment events. By joining these on the order
key, it is possible to see shipment information alongside the order.

First, populate the ``orders`` and ``shipments`` topics:

.. __ss-join_01_end

.. __ss-join_02_start

Register both topics with KSQL:

.. code:: sql

    ksql> CREATE STREAM NEW_ORDERS (ORDER_ID INT, TOTAL_AMOUNT DOUBLE, CUSTOMER_NAME VARCHAR) \
          WITH (KAFKA_TOPIC='new_orders', VALUE_FORMAT='JSON');

    ksql> CREATE STREAM SHIPMENTS (ORDER_ID INT, SHIPMENT_ID INT, WAREHOUSE VARCHAR) \
          WITH (KAFKA_TOPIC='shipments', VALUE_FORMAT='JSON');

After each ``CREATE STREAM`` statement you should get the message: 

.. code:: bash

     Message
    ----------------
     Stream created
    ----------------

Query the data to confirm that it is present in the topics. 

.. include:: ../includes/ksql-includes.rst
    :start-after: _offsetreset_start
    :end-before: _offsetreset_end

For the ``NEW_ORDERS`` topic, run: 

.. code:: sql

    ksql> SELECT ORDER_ID, TOTAL_AMOUNT, CUSTOMER_NAME FROM NEW_ORDERS LIMIT 3;

Your output should resemble:

.. code:: bash

    1 | 10.5 | Bob Smith
    2 | 3.32 | Sarah Black
    3 | 21.0 | Emma Turner

For the ``SHIPMENTS`` topic, run: 

.. code:: sql

    ksql> SELECT ORDER_ID, SHIPMENT_ID, WAREHOUSE FROM SHIPMENTS LIMIT 2;

Your output should resemble:

.. code:: bash

    1 | 42 | Nashville
    3 | 43 | Palo Alto

Run the following query, which will show orders with associated shipments, 
based on a join window of 1 hours. 

.. code:: sql

    ksql> SELECT O.ORDER_ID, O.TOTAL_AMOUNT, O.CUSTOMER_NAME, \
          S.SHIPMENT_ID, S.WAREHOUSE \
          FROM NEW_ORDERS O \
          INNER JOIN SHIPMENTS S \
            WITHIN 1 HOURS \
            ON O.ORDER_ID = S.ORDER_ID;

Your output should resemble:

.. code:: bash

    1 | 10.5 | Bob Smith | 42 | Nashville
    3 | 21.0 | Emma Turner | 43 | Palo Alto

Note that message with ``ORDER_ID=2`` has no corresponding
``SHIPMENT_ID`` or ``WAREHOUSE`` - this is because there is no
corresponding row on the shipments stream within the time window
specified. 

Press Ctrl-C to cancel the ``SELECT`` query and return to the KSQL prompt.


.. __ss-join_02_end

.. __table_table_join:

.. __tt-join_01_start

Table-Table join
================

Using a table-table join, it is possible to join two *tables* of on a
common key. KSQL tables provide the latest *value* for a given *key*.
They can only be joined on the *key*, and one-to-many (1:N) joins are
not supported in the current semantic model.

In this example we have location data about a warehouse from one system,
being enriched with data about the size of the warehouse from another.

First, populate the two topics:

.. __tt-join_01_end

.. __tt-join_02_start

Register both as KSQL tables:

.. code:: sql

    ksql> CREATE TABLE WAREHOUSE_LOCATION (WAREHOUSE_ID INT, CITY VARCHAR, COUNTRY VARCHAR) \
          WITH (KAFKA_TOPIC='warehouse_location', \
                VALUE_FORMAT='JSON', \
                KEY='WAREHOUSE_ID');

    ksql> CREATE TABLE WAREHOUSE_SIZE (WAREHOUSE_ID INT, SQUARE_FOOTAGE DOUBLE) \
          WITH (KAFKA_TOPIC='warehouse_size', \
                VALUE_FORMAT='JSON', \
                KEY='WAREHOUSE_ID');

For each ``CREATE TABLE`` statement, you should get the message: 

.. code:: bash


     Message
    ---------------
     Table created
    ---------------

Check both tables that the message key (``ROWKEY``) matches the declared
key (``WAREHOUSE_ID``) - the output should show that they are equal. If
they are not, the join will not succeed or behave as expected.

.. include:: ../includes/ksql-includes.rst
    :start-after: _offsetreset_start
    :end-before: _offsetreset_end

.. code:: sql

    ksql> SELECT ROWKEY, WAREHOUSE_ID FROM WAREHOUSE_LOCATION LIMIT 3;

Your output should resemble:

.. code:: bash

    1 | 1
    2 | 2
    3 | 3
    Limit Reached
    Query terminated

.. code:: sql

    ksql> SELECT ROWKEY, WAREHOUSE_ID FROM WAREHOUSE_SIZE LIMIT 3;

Your output should resemble:

.. code:: bash

    1 | 1
    2 | 2
    3 | 3
    Limit Reached
    Query terminated

Now join the two tables:

.. code:: sql

    ksql> SELECT WL.WAREHOUSE_ID, WL.CITY, WL.COUNTRY, WS.SQUARE_FOOTAGE \
          FROM WAREHOUSE_LOCATION WL \
            LEFT JOIN WAREHOUSE_SIZE WS \
              ON WL.WAREHOUSE_ID=WS.WAREHOUSE_ID \
          LIMIT 3;

Your output should resemble:

.. code:: bash

    1 | Leeds | UK | 16000.0
    2 | Sheffield | UK | 42000.0
    3 | Berlin | Germany | 94000.0
    Limit Reached
    Query terminated

.. __tt-join_02_end

.. __insert_into:

.. __insert-into_01_start

INSERT INTO
===========

The ``INSERT INTO`` syntax can be used to merge the contents of multiple
streams. An example of this could be where the same event type is coming
from different sources.

Run two datagen processes, each writing to a different topic, simulating
order data arriving from a local installation vs from a third-party:

.. __insert-into_01_end

.. __insert-into_02_start

In KSQL, register the source topic for each:

.. code:: sql

    ksql> CREATE STREAM ORDERS_SRC_LOCAL \
            WITH (KAFKA_TOPIC='orders_local', VALUE_FORMAT='AVRO');
    
    ksql> CREATE STREAM ORDERS_SRC_3RDPARTY \
            WITH (KAFKA_TOPIC='orders_3rdparty', VALUE_FORMAT='AVRO');

After each ``CREATE STREAM`` statement you should get the message: 

.. code:: bash

     Message
    ----------------
     Stream created
    ----------------

Create the output stream, using the standard ``CREATE STREAM … AS``
syntax. Because multiple sources of data are being joined into a common target, 
it is useful to add in lineage information. This can be done by simply including it 
as part of the ``SELECT``:

.. code:: sql

    ksql> CREATE STREAM ALL_ORDERS AS SELECT 'LOCAL' AS SRC, * FROM ORDERS_SRC_LOCAL;

Your output should resemble:

.. code:: bash

     Message
    ----------------------------
     Stream created and running
    ----------------------------

Use the ``DESCRIBE`` command to observe the schema of the target stream. 

.. code:: sql

    ksql> DESCRIBE ALL_ORDERS;


Your output should resemble:

.. code:: bash

    Name                 : ALL_ORDERS
     Field      | Type
    ----------------------------------------------------------------------------------
     ROWTIME    | BIGINT           (system)
     ROWKEY     | VARCHAR(STRING)  (system)
     SRC        | VARCHAR(STRING)
     ORDERTIME  | BIGINT
     ORDERID    | INTEGER
     ITEMID     | VARCHAR(STRING)
     ORDERUNITS | DOUBLE
     ADDRESS    | STRUCT<CITY VARCHAR(STRING), STATE VARCHAR(STRING), ZIPCODE BIGINT>
    ----------------------------------------------------------------------------------
    For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;

Add stream of 3rd party orders into the existing output stream:

.. code:: sql

    ksql> INSERT INTO ALL_ORDERS SELECT '3RD PARTY' AS SRC, * FROM ORDERS_SRC_3RDPARTY;


Your output should resemble:

.. code:: bash

     Message
    -------------------------------
     Insert Into query is running.
    -------------------------------

Query the output stream to verify that data from each source is being
written to it:

.. code:: sql

    ksql> SELECT * FROM ALL_ORDERS;

Your output should resemble the following. Note that there are messages from both source 
topics (denoted by ``LOCAL`` and ``3RD PARTY`` respectively). 

.. code:: bash

    1531736084879 | 1802 | 3RD PARTY | 1508543844870 | 1802 | Item_427 | 5.003326679575532 | {CITY=City_27, STATE=State_63, ZIPCODE=12589}
    1531736085016 | 1836 | LOCAL | 1489112050820 | 1836 | Item_224 | 9.561788841477156 | {CITY=City_67, STATE=State_99, ZIPCODE=28638}
    1531736085118 | 1803 | 3RD PARTY | 1516295084125 | 1803 | Item_208 | 7.984495994658404 | {CITY=City_13, STATE=State_56, ZIPCODE=23417}
    1531736085222 | 1804 | 3RD PARTY | 1503734687976 | 1804 | Item_498 | 4.8212828530483876 | {CITY=City_42, STATE=State_45, ZIPCODE=87842}
    1531736085444 | 1837 | LOCAL | 1511189492298 | 1837 | Item_183 | 1.3867306505950954 | {CITY=City_28, STATE=State_86, ZIPCODE=14742}
    1531736085531 | 1838 | LOCAL | 1497601536360 | 1838 | Item_945 | 4.825111590185673 | {CITY=City_78, STATE=State_13, ZIPCODE=59763}
    …

Press Ctrl-C to cancel the ``SELECT`` query and return to the KSQL prompt.

You can view the two queries that are running using ``SHOW QUERIES``: 

.. code:: sql

    ksql> SHOW QUERIES;


Your output should resemble:

.. code:: bash

    Query ID          | Kafka Topic | Query String
    -------------------------------------------------------------------------------------------------------------------
    CSAS_ALL_ORDERS_0 | ALL_ORDERS  | CREATE STREAM ALL_ORDERS AS SELECT 'LOCAL' AS SRC, * FROM ORDERS_SRC_LOCAL;
    InsertQuery_1     | ALL_ORDERS  | INSERT INTO ALL_ORDERS SELECT '3RD PARTY' AS SRC, * FROM ORDERS_SRC_3RDPARTY;
    -------------------------------------------------------------------------------------------------------------------

.. __insert-into_02_end

