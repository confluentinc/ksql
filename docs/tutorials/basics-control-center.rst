.. _ksql_quickstart-c3:

Writing Streaming Queries Against |ak-tm| Using KSQL and |c3|
#############################################################

You can use KSQL in |c3| to write streaming queries against messages in |ak|.

**Prerequisites:**

- :ref:`Confluent Platform <installation>` is installed and running. This
  installation includes a Kafka broker, KSQL, |c3-short|, |zk|, |sr|, REST
  Proxy, and Kafka Connect.
- If you installed |cp| via TAR or ZIP, navigate into the installation
  directory. The paths and commands used throughout this tutorial assume that
  you are in this installation directory.
- Java: Minimum version 1.8. Install Oracle Java JRE or JDK >= 1.8 on your
  local machine

.. include:: ../includes/ksql-includes.rst
      :start-after: basics_tutorial_01_start
      :end-before: basics_tutorial_01_end

.. _ksql_quickstart-c3-inspect-topics:

Inspect Topics By Using |c3-short|
**********************************

#. Open your browser to ``http://localhost:9021``. |c3| opens, showing the
   **System health** view.

#. In the navigation menu, click **Topics** to view the ``pageviews`` and ``users``
   topics that you created previously.

   .. image:: ../img/c3-topics-pageviews-users.png
      :alt: Screenshot of Confluent Control Center showing the Topics page
      :align: center

Inspect Topics By Using KSQL in |c3-short|
******************************************

#. In the navigation menu, click **KSQL** to open the **KSQL Editor**.

#. In the editing window, use the SHOW TOPICS statement to see the available
   topics on the Kafka cluster. Click **Run** to start the query.

   ::

       SHOW TOPICS;

   .. image:: ../img/c3-ksql-editor-show-topics.png
      :alt: Screenshot of Confluent Control Center showing the KSQL Editor
      :align: center

#. In the **Query Results** window, scroll to the bottom to view the ``pageviews``
   and ``users`` topics that you created previously. Your output should resemble:

   .. code:: json

       {
         "name": "pageviews",
         "registered": false,
         "replicaInfo": [
           1
         ],
         "consumerCount": 0,
         "consumerGroupCount": 0
       },
       {
         "name": "users",
         "registered": false,
         "replicaInfo": [
           1
         ],
         "consumerCount": 0,
         "consumerGroupCount": 0
       }

   The ``"registered": false`` indicator means that you haven't created a stream
   or table on top of these topics, so you can't write streaming queries against
   them yet.

#. In the editing window, use the PRINT TOPIC statement to inspect the records in
   the ``users`` topic. Click **Run** to start the query.

   ::

      PRINT 'users' FROM BEGINNING;

   Your output should resemble:

   .. image:: ../img/c3-ksql-print-topic-users.png
      :alt: Screenshot of the KSQL SHOW TOPIC statement in Confluent Control Center
      :align: center

#. The query continues until you end it explicitly. Click **Stop** to end the query.

Create a Stream and Table
*************************

To write streaming queries against the ``pageviews`` and ``users`` topics,
register the the topics with KSQL as a stream and a table. You can use the
CREATE STREAM and CREATE TABLE statements in the KSQL Editor, or you can use
the |c3-short| UI . 

These examples query records from the ``pageviews`` and ``users`` topics using
the following schema.

.. image:: ../img/ksql-quickstart-schemas.jpg
   :alt: ER diagram showing a pageviews stream and a users table with a common userid column
   :align: center

Create a Stream in the KSQL Editor
==================================

You can create a stream or table by using the CREATE STREAM and CREATE TABLE
statements in KSQL Editor, just like you use them in the KSQL CLI.

#. Copy the following code into the editing window and click **Run**.

   .. code:: sql

       CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH 
       (kafka_topic='pageviews', value_format='DELIMITED');

   Your output should resemble:

   .. image:: ../img/c3-ksql-create-stream-statement.png
      :alt: Screenshot of the KSQL CREATE STREAM statement in Confluent Control Center
      :align: center

#. In the editing window, use the SHOW TOPICS statement to inspect the status of
   the ``pageviews`` topic. Click **Run** to start the query.

   ::

       SHOW TOPICS;

#. In the **Query Results** window, scroll to the bottom to view the ``pageviews``
   topic. Your output should resemble:

   .. code:: json

       {
         "name": "pageviews",
         "registered": true,
         "replicaInfo": [
           1
         ],
         "consumerCount": 0,
         "consumerGroupCount": 0
       },

   The ``"registered": true`` indicator means that you have registered the topic
   and you can write streaming queries against it.

Create a Table in the |c3-short| UI
===================================

|c3| guides you through the process of registering a topic as a stream or a table. 

#. In the KSQL Editor, navigate to **Tables** and click **Add a table**. The
   **Create a KSQL Table** dialog opens.

   .. image:: ../img/c3-ksql-create-table-wizard-1.png
      :alt: Screenshot of the Create a KSQL Table wizard in Confluent Control Center
      :align: center

#. Click **users** to fill in the details for the table. KSQL infers the table
   schema and displays the field names and types from the topic. You need to
   choose a few more settings. 

   * In the **How are your messages encoded?** dropdown, select **JSON**.
   * In the **Key** dropdown, select **userid**.

#. Click **Save Table** to create a table on the the ``users`` topic.

   .. image:: ../img/c3-ksql-create-table-wizard-2.png
      :alt: Screenshot of the Create a KSQL Table wizard in Confluent Control Center
      :align: center

#. The KSQL Editor opens with a suggested query. Click **Run** to display the
   query results.  

   .. image:: ../img/c3-ksql-select-from-users-query.png
      :alt: Screenshot of a KSQL SELECT query in Confluent Control Center
      :align: center

   The **Query Results** pane displays query status information, like
   **Messages/sec**, and it shows the fields that the query returns.

#. The query continues until you end it explicitly. Click **Stop** to end the query.

Write Persistent Queries
========================

With the ``pageviews`` topic registered as a stream, and the ``users`` topic
registered as a table, you can write streaming queries that run until you 
end them with the TERMINATE statement.

#. Copy the following code into the editing window and click **Run**.

   .. code:: sql

      CREATE STREAM pageviews_enriched AS
      SELECT users.userid AS userid, pageid, regionid, gender
      FROM pageviews_original
      LEFT JOIN users
        ON pageviews_original.userid = users.userid;

   Your output should resemble:

   .. image:: ../img/c3-ksql-csas.png
      :alt: Screenshot of the KSQL CREATE STREAM AS SELECT statement in Confluent Control Center
      :align: center

#. To inspect your persistent queries, navigate to the **Running Queries** page,
   which shows details about the ``pageviews_enriched`` stream that you created
   in the previous query.

   .. image:: ../img/c3-ksql-running-queries.png
      :alt: Screenshot of the KSQL Running Queries page in Confluent Control Center
      :align: center

#. Click **Explain** to see the schema and query properties for the persistent
   query.

Monitor Persistent Queries
==========================

You can monitor your persistent queries visually by using |C3|.

#. In the navigation menu, click **Data streams** and find the ``pageviews_enriched``
   query, which is named ``_confluent-ksql-default_query_CSAS_PAGEVIEWS_ENRICHED_0``.
   
#. Change the time scale from **Last 4 hours** to **Last 30 minutes**.

   Your output should resemble:

   .. image:: ../img/c3-ksql-data-streams-1.png
      :alt: Screenshot of the Data Streams page in Confluent Control Center
      :align: center

   The graph is shaded red, because in the time since you started ksql-datagen, 
   records have accumulated in the ``pageviews`` and ``users`` topics, with no
   consumer groups to consume them. The green bar on the right indicates that the
   ``pageviews_enriched`` query has recently started consuming records.

#. In the navigation menu, click **Consumer lag** and find your ``pageviews_enriched``
   query, which is named ``_confluent-ksql-default_query_CSAS_PAGEVIEWS_ENRICHED_0``.
   This view shows how well your persistent query is keeping up with the incoming
   data.

   .. image:: ../img/c3-ksql-consumer-lag.png
      :alt: Screenshot of the Consumer Lag page in Confluent Control Center
      :align: center

Query Properties
================   

You can assign properties in the KSQL Editor before you run your queries.

#. In the navigation menu, click **KSQL** to open the **KSQL Editor**.

#. Click **Query properties** and set the ``auto.offset.reset`` field to **Earliest**.

#. Copy the following code into the editing window and click **Run**.

   .. code:: sql

      CREATE STREAM pageviews_female AS
      SELECT * FROM pageviews_enriched
      WHERE gender = 'FEMALE';

   The ``pageviews_female`` stream starts with the earliest record in the
   ``pageviews`` topic, which means that it consumes all of the available 
   records from the beginning.

#. Confirm that the ``auto.offset.reset`` property was applied to the 
   ``pageviews_female`` stream. In the navigation menu, click **Data streams**
   and find the ``pageviews_female`` stream, which is named
   ``_confluent-ksql-default_query_CSAS_PAGEVIEWS_FEMALE_1``.

   .. image:: ../img/c3-ksql-data-streams-2.png
      :alt: Screenshot of the Data Streams page in Confluent Control Center
      :align: center

   The graph is shaded green, because all of the records were consumed when the
   ``pageviews_female`` stream started.

Next Steps
**********

* :ref:`ksql_clickstream-docker`