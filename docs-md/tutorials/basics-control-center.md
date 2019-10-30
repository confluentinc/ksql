---
layout: page
title: Write Streaming Queries Against Apache Kafka® Using KSQL and Confluent Control Center
tagline: Use KSQL for event streaming applications
description: Learn how to use KSQL to create event streaming applications on Kafka topics
keywords: ksql, confluent control center
---

Write Streaming Queries Against {{ site.aktm }} Using KSQL and {{ site.c3 }}
=============================================================================

You can use KSQL in {{ site.c3 }} to write streaming queries against
messages in {{ site.ak }}.

**Prerequisites:**

-   [Confluent
    Platform](https://docs.confluent.io/current/installation/installing_cp/index.html)
    is installed and running. This installation includes a Kafka broker,
    KSQL, {{ site.c3short }}, {{ site.zk }}, {{ site.sr }}, {{ site.crest }},
    and {{ site.kconnect }}.
-   If you installed {{ site.cp }} via TAR or ZIP, navigate into the
    installation directory. The paths and commands used throughout this
    tutorial assume that you are in this installation directory.
-   Consider
    [installing](https://docs.confluent.io/current/cli/installing.html)
    the {{ site.confluentcli }} to start a local installation of {{ site.cp }}.
-   Java: Minimum version 1.8. Install Oracle Java JRE or JDK >= 1.8 on
    your local machine

Create Topics and Produce Data
==============================

Create and produce data to the Kafka topics `pageviews` and `users`.
These steps use the KSQL datagen that is included {{ site.cp }}.

1.  Create the `pageviews` topic and produce data using the data
    generator. The following example continuously generates data with a
    value in DELIMITED format.

    ```bash
    <path-to-confluent>/bin/ksql-datagen quickstart=pageviews format=delimited topic=pageviews maxInterval=500
    ```

2.  Produce Kafka data to the `users` topic using the data generator.
    The following example continuously generates data with a value in
    JSON format.

    ```bash
    <path-to-confluent>/bin/ksql-datagen quickstart=users format=json topic=users maxInterval=100
    ```

!!! tip
	You can also produce Kafka data using the `kafka-console-producer` CLI
    provided with {{ site.cp }}.

Launch the KSQL CLI
===================

To launch the CLI, run the following command. It will route the CLI logs
to the `./ksql_logs` directory, relative to your current directory. By
default, the CLI will look for a KSQL Server running at
`http://localhost:8088`.

```bash
LOG_DIR=./ksql_logs <path-to-confluent>/bin/ksql
```

Inspect Topics By Using Control Center
--------------------------------------

1.  Open your browser to `http://localhost:9021`. {{ site.c3 }} opens,
    showing the **Home** page for your clusters. In the navigation bar,
    click the cluster that you want to use with KSQL.
2.  In the navigation menu, click **Topics** to view the `pageviews` and
    `users` topics that you created previously.

    ![](../img/c3-topics-pageviews-users.png)

Inspect Topics By Using KSQL in Control Center
----------------------------------------------

1.  In the cluster submenu, click **KSQL** to open the KSQL clusters
    page, and click **KSQL** to open the **KSQL Editor** on the default
    application.

    ![](../img/c3-ksql-app-list.png)

2.  In the editing window, use the SHOW TOPICS statement to see the
    available topics on the Kafka cluster. Click **Run** to start the
    query.

    ```
        SHOW TOPICS;
    ```

    ![](../img/c3-ksql-editor-show-topics.png)

3.  In the **Query Results** window, scroll to the bottom to view the
    `pageviews` and `users` topics that you created previously. Your
    output should resemble:

    ```json
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
    ```

    The `"registered": false` indicator means that you haven't created
    a stream or table on top of these topics, so you can't write
    streaming queries against them yet.

4.  In the editing window, use the PRINT TOPIC statement to inspect the
    records in the `users` topic. Click **Run** to start the query.

    ```
       PRINT 'users' FROM BEGINNING;
    ```

    Your output should resemble:

    ![](../img/c3-ksql-print-topic-users.png)

5.  The query continues until you end it explicitly. Click **Stop** to
    end the query.

Create a Stream and Table
-------------------------

To write streaming queries against the `pageviews` and `users` topics,
register the the topics with KSQL as a stream and a table. You can use
the CREATE STREAM and CREATE TABLE statements in the KSQL Editor, or you
can use the {{ site.c3short }} UI.

These examples query records from the `pageviews` and `users` topics
using the following schema.

![](../img/ksql-quickstart-schemas.jpg)

### Create a Stream in the KSQL Editor

You can create a stream or table by using the CREATE STREAM and CREATE
TABLE statements in KSQL Editor, just like you use them in the KSQL CLI.

1.  Copy the following code into the editing window and click **Run**.

    ```sql
    CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH 
    (kafka_topic='pageviews', value_format='DELIMITED');
    ```

    Your output should resemble:

    ![](../img/c3-ksql-create-stream-statement.png)

2.  In the editing window, use the SHOW TOPICS statement to inspect the
    status of the `pageviews` topic. Click **Run** to start the query.

    ```
       SHOW TOPICS;
    ```

3.  In the **Query Results** window, scroll to the bottom to view the
    `pageviews` topic. Your output should resemble:

    ```json
    {
      "name": "pageviews",
      "registered": true,
      "replicaInfo": [
        1
      ]
    },
    ```

    The `"registered": true` indicator means that you have registered
    the topic and you can write streaming queries against it.

Create a Table in the Control Center UI
=======================================

{{ site.c3 }} guides you through the process of registering a topic as a
stream or a table.

1.  In the KSQL Editor, navigate to **Tables** and click **Add a
    table**. The **Create a KSQL Table** dialog opens.

    ![](../img/c3-ksql-create-table-wizard-1.png)

2.  Click **users** to fill in the details for the table. KSQL infers
    the table schema and displays the field names and types from the
    topic. You need to choose a few more settings.
    -   In the **Encoding** dropdown, select **JSON**.
    -   In the **Key** dropdown, select **userid**.
3.  Click **Save Table** to create a table on the the `users` topic.

    ![](../img/c3-ksql-create-table-wizard-2.png)

4.  The KSQL Editor opens with a suggested query.

    ![](../img/c3-ksql-select-from-users-query.png)

    The **Query Results** pane displays query status information, like
    **Messages/sec**, and it shows the fields that the query returns.

5.  The query continues until you end it explicitly. Click **Stop** to
    end the query.

### Write Persistent Queries

With the `pageviews` topic registered as a stream, and the `users` topic
registered as a table, you can write streaming queries that run until
you end them with the TERMINATE statement.

1.  Copy the following code into the editing window and click **Run**.

    ```sql
    CREATE STREAM pageviews_enriched AS
    SELECT users.userid AS userid, pageid, regionid, gender
    FROM pageviews_original
    LEFT JOIN users
      ON pageviews_original.userid = users.userid
    EMIT CHANGES;
    ```

    Your output should resemble:

    ![](../img/c3-ksql-csas.png)

2.  To inspect your persistent queries, navigate to the **Running
    Queries** page, which shows details about the `pageviews_enriched`
    stream that you created in the previous query.

    ![](../img/c3-ksql-running-queries.png)

3.  Click **Explain** to see the schema and query properties for the
    persistent query.

### Monitor Persistent Queries

You can monitor your persistent queries visually by using {{ site.c3 }}.

1.  In the cluster submenu, click **Consumers** and find the consumer
    group for the `pageviews_enriched` query, which is named
    `_confluent-ksql-default_query_CSAS_PAGEVIEWS_ENRICHED_0`.

    Click **Consumption** to see the rate that the `pageviews_enriched`
    query is consuming records.

2.  Change the time scale from **Last 4 hours** to **Last 30 minutes**.

    Your output should resemble:

    ![](../img/c3-ksql-data-streams-1.png)

3.  In the navigation menu, click **Consumer lag** and find the consumer
    group for the `pageviews_enriched` query, which is named
    `_confluent-ksql-default_query_CSAS_PAGEVIEWS_ENRICHED_0`. This view
    shows how well your persistent query is keeping up with the incoming
    data.

    ![](../img/c3-ksql-consumer-lag.png)

### Query Properties

You can assign properties in the KSQL Editor before you run your
queries.

1.  In the cluster submenu, click **KSQL** to open the KSQL clusters
    page, and click **KSQL** to open the **KSQL Editor** on the default
    application.
2.  Click **Add query properties** and set the `auto.offset.reset` field
    to **Earliest**.
3.  Copy the following code into the editing window and click **Run**.

    ```sql
    CREATE STREAM pageviews_female AS
    SELECT * FROM pageviews_enriched
    WHERE gender = 'FEMALE'
    EMIT CHANGES;
    ```

    ![](../img/c3-ksql-set-auto-offset-reset.png)

    The `pageviews_female` stream starts with the earliest record in the
    `pageviews` topic, which means that it consumes all of the available
    records from the beginning.

4.  Confirm that the `auto.offset.reset` property was applied to the
    `pageviews_female` stream. In the cluster submenu, click
    **Consumers** and find the consumer group for the `pageviews_female`
    stream, which is named
    `_confluent-ksql-default_query_CSAS_PAGEVIEWS_FEMALE_1`.

    Click **Consumption** to see the rate that the `pageviews_female`
    query is consuming records.

    ![](../img/c3-ksql-data-streams-2.png)

    The graph is at 100 percent, because all of the records were
    consumed when the `pageviews_female` stream started.

### View streams and tables

You can see all of your persistent queries, streams, and tables in a
single, unified view.

1.  Click **KSQl Editor** and find the **All available streams and
    tables** pane on the right side of the page,
2.  Click **KSQL_PROCESSING_LOG** to open the processing log stream.
    The schema for the stream is displayed, including nested data
    structures.

    ![](../img/c3-ksql-streams-and-tables-pane.png)

### Download selected records

You can download records that you select in the query results window as
a JSON file.

1.  Copy the following code into the editing window and click **Run**.

    ```sql
    SELECT * FROM  PAGEVIEWS_FEMALE EMIT CHANGES;
    ```

2.  In the query results window, select some records and click
    **Download**.

    ![](../img/c3-ksql-download-records.png)

Next Steps
----------

-   [Clickstream Data Analysis Pipeline Using KSQL (Docker)](clickstream-docker.md)
