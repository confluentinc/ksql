.. _ksql_quickstart-ccloud:

Writing Streaming Queries Against |ak-tm| Using KSQL and |ccloud|
#################################################################

Prerequisites
    - `Access to Confluent Cloud <https://www.confluent.io/confluent-cloud/>`__
    - :ref:`cloud-limits`

.. include:: ../../../quickstart/includes/docker-prereqs.rst

Follow these steps to set up an |ak| cluster on |ccloud|, produce data to 
Kafka topics on the cluster, and write streaming queries on cloud-hosted KSQL.

#. Set up your |ccloud| account.
#. Create an |ak| cluster.
#. Configure |sr-ccloud|. 
#. Install the |ccloud| CLI.
#. Install |cp| locally.
#. Produce data to your |ak| cluster in |ccloud|.
#. Write streaming queries by using the KSQL Editor in |ccloud|.  

.. _ksql-create-cluster-in-ccloud:

Create an |ak-tm| Cluster in |ccloud|  
*************************************

Follow steps 1 through 3 in :ref:`cloud-quickstart`.

TODO: Add anchors for these in docs/quickstart/cloud-quickstart.rst.

Step 1: Create Kafka Cluster in Confluent Cloud
Step 2: Install and Configure the Confluent Cloud CLI
Step 3: Configure |sr-ccloud|. For this demo, you don't need to set up the 
Java client.

.. _ksql-install-locally-for-ccloud:

Install |cp| Locally
********************

.. steps taken from docs/tutorials/examples/ccloud/docs/index.rst

#. Clone the `examples GitHub repository <https://github.com/confluentinc/examples>`__.

   .. code:: bash

     $ git clone https://github.com/confluentinc/examples

#. Change directory to the |ccloud| demo.

   .. code:: bash

     $ cd examples/ccloud

#. Start the demo by running a single command. You have two choices: using a
   |cp| local install or Docker Compose. This will take less than 5 minutes
   to complete.

   .. sourcecode:: bash

      # For Confluent Platform local install using Confluent CLI
      $ ./start.sh

      # For Docker Compose
      $ ./start-docker.sh

.. _ksql-write-queries-in-ccloud:

Inspect Topics By Using |ccloud|
********************************

#. Open your browser to the URL of your |ccloud| environment, which is similar to
   ``https://confluent.cloud/environments/<your-environment>``.

#. In the navigation menu, click **Topics** to view the ``pageviews`` and ``users``
   topics that you created previously.

   .. image:: ../img/ccloud-topics-pageviews-users.png
      :alt: Screenshot of Confluent Cloud showing the Topics page
      :align: center

Create a KSQL Application
*************************

ksql-app1

   .. image:: ../img/ccloud-ksql-add-application-1.png
      :alt: Screenshot of Confluent Cloud showing the KSQL Add Application page
      :align: center



   .. image:: ../img/ccloud-ksql-add-application-2.png
      :alt: Screenshot of Confluent Cloud showing the KSQL Add Application wizard
      :align: center


   .. image:: ../img/ccloud-ksql-add-application-3.png
      :alt: Screenshot of Confluent Cloud showing the KSQL Add Application wizard
      :align: center


   .. image:: ../img/ccloud-ksql-application-list.png
      :alt: Screenshot of Confluent Cloud showing the KSQL Applcations page
      :align: center
      

Inspect Topics By Using KSQL in |ccloud| UI
*******************************************

#. In the navigation menu, click **KSQL** to open the KSQL Applications page.

#. On **ksql-app1**, click *...* and select **KSQL Editor**.


   .. image:: ../img/ccloud-ksql-context-menu.png
      :alt: Screenshot of Confluent Cloud showing the KSQL Applcation context menu
      :align: center


#. In the editing window, use the SHOW TOPICS statement to see the available
   topics on the Kafka cluster. Click **Run** to start the query.

   ::

       SHOW TOPICS;

   Your output should resemble:

   .. image:: ../img/ccloud-ksql-editor-show-topics.png
      :alt: Screenshot of Confluent Cloud showing the KSQL Editor
      :align: center

#. In the **Query Results** window, scroll down to view the ``pageviews`` topic
   that you created previously. Your output should resemble:

   .. code:: json

      {
         "name": "pageviews",
         "registered": false,
         "replicaInfo": [
           3,
           3,
           3,
           3,
           3,
           3,
           3,
           3,
           3,
           3,
           3,
           3
         ],
         "consumerCount": 12,
         "consumerGroupCount": 1
       },

   The ``"registered": false`` indicator means that you haven't created a stream
   or table on top of this topic, so you can't write streaming queries against
   it yet.

#. In the editing window, use the PRINT TOPIC statement to inspect the records in
   the ``users`` topic. Click **Run** to start the query.

   ::

      PRINT 'users' FROM BEGINNING;

   Your output should resemble:

.. TODO: new screenshot -- currently, PRINT doesn't work in CCloud

   .. image:: ../img/c3-ksql-print-topic-users.png
      :alt: Screenshot of the KSQL SHOW TOPIC statement in Confluent Cloud
      :align: center

#. The query continues until you end it explicitly. Click **Stop** to end the query.


Create a Stream and Table
*************************

To write streaming queries against the ``pageviews`` and ``users`` topics,
register the the topics with KSQL as a stream and a table. Use the CREATE STREAM
and CREATE TABLE statements in the KSQL Editor.

These examples query records from the ``pageviews`` and ``users`` topics using
the following schema.

.. image:: ../img/ksql-quickstart-schemas.jpg
   :alt: ER diagram showing a pageviews stream and a users table with a common userid column
   :align: center

Create a Stream in the KSQL Editor
==================================

You can create a stream or table by using the CREATE STREAM and CREATE TABLE
statements in the KSQL Editor, just like how you use them in the KSQL CLI.

#. Copy the following code into the editing window and click **Run**.

   .. code:: sql

       CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH 
       (kafka_topic='pageviews', value_format='DELIMITED');

   Your output should resemble:

   .. image:: ../img/ccloud-ksql-create-stream-statement.png
      :alt: Screenshot of the KSQL CREATE STREAM statement in Confluent Cloud
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
        3,
        3,
        3,
        3,
        3,
        3,
        3,
        3,
        3,
        3,
        3,
        3
      ],
      "consumerCount": 12,
      "consumerGroupCount": 1
    },

   The ``"registered": true`` indicator means that you have registered the topic
   and you can write streaming queries against it.


Create a Table in the |ccloud| UI
=================================

KSQL on |ccloud| guides you through the process of registering a topic as a
stream or a table. 

#. In the KSQL Editor, navigate to **Tables** and click **Add a table**. The
   **Create a KSQL Table** dialog opens.

.. TODO: new screenshot

   .. image:: ../img/ccloud-ksql-create-table-wizard-1.png
      :alt: Screenshot of the Create a KSQL Table wizard in Confluent Cloud
      :align: center

#. Click **users** to fill in the details for the table. KSQL infers the table
   schema and displays the field names and types from the topic. You need to
   choose a few more settings. 

   * In the **How are your messages encoded?** dropdown, select **JSON**.
   * In the **Key** dropdown, select **userid**.

#. Click **Save Table** to create a table on the the ``users`` topic.

.. TODO: new screenshot

   .. image:: ../img/ccloud-ksql-create-table-wizard-2.png
      :alt: Screenshot of the Create a KSQL Table wizard in Confluent Cloud
      :align: center

#. The KSQL Editor opens with a suggested query. Click **Run** to display the
   query results.  

.. TODO: new screenshot

   .. image:: ../img/ccloud-ksql-select-from-users-query.png
      :alt: Screenshot of a KSQL SELECT query in Confluent Cloud
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

.. TODO: new screenshot

   .. image:: ../img/ccloud-ksql-csas.png
      :alt: Screenshot of the KSQL CREATE STREAM AS SELECT statement in Confluent Cloud
      :align: center

#. To inspect your persistent queries, navigate to the **Running Queries** page,
   which shows details about the ``pageviews_enriched`` stream that you created
   in the previous query.

.. TODO: new screenshot

   .. image:: ../img/ccloud-ksql-running-queries.png
      :alt: Screenshot of the KSQL Running Queries page in Confluent Cloud
      :align: center

#. Click **Explain** to see the schema and query properties for the persistent
   query.


Monitor Persistent Queries
==========================

You can monitor your persistent queries visually by using |ccloud|.

*  In the navigation menu, click **Consumer lag** and find your ``pageviews_enriched``
   stream, which is named ``_confluent-ksql-default_query_CSAS_PAGEVIEWS_ENRICHED_0``.
   This view shows how well your stream is keeping up with the incoming data.

.. TODO: new screenshot

   .. image:: ../img/ccloud-ksql-consumer-lag.png
      :alt: Screenshot of the Consumer Lag page in Confluent Cloud
      :align: center

Query Properties
================

.. TODO

Next Steps
**********