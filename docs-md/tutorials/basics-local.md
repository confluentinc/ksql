---
layout: page
title: Write Streaming Queries Against Apache Kafka® Using KSQL and Confluent Control Center (Local)
tagline: Use KSQL for event streaming applications
description: Learn how to use KSQL to create event streaming applications on Kafka topics
keywords: ksql
---

This tutorial demonstrates a simple workflow using KSQL to write
streaming queries against messages in Kafka.

To get started, you must start a Kafka cluster, including {{ site.zk }}
and a Kafka broker. KSQL will then query messages from this Kafka
cluster. KSQL is installed in the {{ site.cp }} by default.

Watch the [screencast of Reading Kafka Data from
KSQL](https://www.youtube.com/embed/EzVZOUt9JsU) on YouTube.

Prerequisites:

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
    <path-to-confluent>/bin/ksql-datagen quickstart=pageviews format=delimited topic=pageviews msgRate=5
    ```

2.  Produce Kafka data to the `users` topic using the data generator.
    The following example continuously generates data with a value in
    JSON format.

    ```bash
    <path-to-confluent>/bin/ksql-datagen quickstart=users format=avro topic=users msgRate=1
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

!!! important
      By default KSQL attempts to store its logs in a directory called `logs`
      that is relative to the location of the `ksql` executable. For example,
      if `ksql` is installed at `/usr/local/bin/ksql`, then it would attempt
      to store its logs in `/usr/local/logs`. If you are running `ksql` from
      the default {{ site.cp }} location, `<path-to-confluent>/bin`, you must
      override this default behavior by using the `LOG_DIR` variable.

After KSQL is started, your terminal should resemble this.

```
                  ===========================================
                  =       _              _ ____  ____       =
                  =      | | _____  __ _| |  _ \| __ )      =
                  =      | |/ / __|/ _` | | | | |  _ \      =
                  =      |   <\__ \ (_| | | |_| | |_) |     =
                  =      |_|\_\___/\__, |_|____/|____/      =
                  =                   |_|                   =
                  =  Event Streaming Database purpose-built =
                  =        for stream processing apps       =
                  ===========================================

Copyright 2017-2019 Confluent Inc.

CLI v{{ site.release }}, Server v{{ site.release }} located at http://localhost:8088

Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

ksql>
```

Inspect Kafka Topics By Using SHOW and PRINT Statements
=======================================================

KSQL enables inspecting Kafka topics and messages in real time.

-   Use the SHOW TOPICS statement to list the available topics in the
    Kafka cluster.
-   Use the PRINT statement to see a topic's messages as they arrive.

In the KSQL CLI, run the following statement:

```sql
SHOW TOPICS;
```

Your output should resemble:

```
 Kafka Topic                 | Partitions | Partition Replicas
--------------------------------------------------------------
 default_ksql_processing_log | 1          | 1
 pageviews                   | 1          | 1
 users                       | 1          | 1
--------------------------------------------------------------
```

By default, KSQL hides internal and system topics. Use the SHOW ALL TOPICS
statement to see the full list of topics in the Kafka cluster:

```sql
SHOW ALL TOPICS;
```

Your output should resemble:

```
  Kafka Topic                            | Partitions | Partition Replicas 
--------------------------------------------------------------------------
 __confluent.support.metrics            | 1          | 1                  
 _confluent-ksql-default__command_topic | 1          | 1                  
 _confluent-license                     | 1          | 1                  
 _confluent-metrics                     | 12         | 1                  
 default_ksql_processing_log            | 1          | 1                  
 pageviews                              | 1          | 1                  
 users                                  | 1          | 1                  
--------------------------------------------------------------------------
```

Inspect the `users` topic by using the PRINT statement:

```sql
PRINT users;
```

!!! note
   The PRINT statement is one of the few case-sensitive commands in ksqlDB,
   even when the topic name is not quoted.

Your output should resemble:

```json
Format:AVRO
{"ROWTIME":1540254230041,"ROWKEY":"User_1","registertime":1516754966866,"userid":"User_1","regionid":"Region_9","gender":"MALE"}
{"ROWTIME":1540254230081,"ROWKEY":"User_3","registertime":1491558386780,"userid":"User_3","regionid":"Region_2","gender":"MALE"}
{"ROWTIME":1540254230091,"ROWKEY":"User_7","registertime":1514374073235,"userid":"User_7","regionid":"Region_2","gender":"OTHER"}
^C{"ROWTIME":1540254232442,"ROWKEY":"User_4","registertime":1510034151376,"userid":"User_4","regionid":"Region_8","gender":"FEMALE"}
Topic printing ceased
```

Press Ctrl+C to stop printing messages.

Inspect the `pageviews` topic by using the PRINT statement:

```sql
PRINT pageviews;
```

Your output should resemble:

```
Format:STRING
10/23/18 12:24:03 AM UTC , 1540254243183 , 1540254243183,User_9,Page_20
10/23/18 12:24:03 AM UTC , 1540254243617 , 1540254243617,User_7,Page_47
10/23/18 12:24:03 AM UTC , 1540254243888 , 1540254243888,User_4,Page_27
^C10/23/18 12:24:05 AM UTC , 1540254245161 , 1540254245161,User_9,Page_62
Topic printing ceased
ksql>
```

Press Ctrl+C to stop printing messages.

For more information, see [KSQL Syntax Reference](../developer-guide/syntax-reference.md).

Create a Stream and Table
=========================

These examples query messages from Kafka topics called `pageviews` and
`users` using the following schemas:

![image](../img/ksql-quickstart-schemas.jpg)

1.  Create a stream, named `pageviews_original`, from the `pageviews`
    Kafka topic, specifying the `value_format` of `DELIMITED`.

    ```sql
    CREATE STREAM pageviews_original (rowkey bigint key, viewtime bigint, userid varchar, pageid varchar)
     WITH (kafka_topic='pageviews', value_format='DELIMITED');
    ```

    Your output should resemble:

    ```
     Message
    ---------------
     Stream created
    ---------------
    ```

    !!! tip
          You can run `DESCRIBE pageviews_original;` to see the schema for the
          stream. Notice that KSQL created an additional columns, named
          `ROWTIME`, which corresponds with the Kafka message timestamp.

2.  Create a table, named `users_original`, from the `users` Kafka
    topic, specifying the `value_format` of `AVRO`.

    ```sql
    CREATE TABLE users_original WITH (kafka_topic='users', value_format='AVRO', key = 'userid');
    ```

    Your output should resemble:

    ```
     Message
    ---------------
     Table created
    ---------------
    ```

    !!! note
          You may have noticed the CREATE TABLE did not define the set of columns
          like the CREATE STREAM statement did. This is because the value format
          if Avro, and the DataGen tool publishes the Avro schema to the {{ site.sr }}.
          ksqlDB retrieves the schema from the Schema Registry and uses this to build
          the SQL schema for the table. You may still provide the schema if you wish.
          Until [Github issue #4462](https://github.com/confluentinc/ksql/issues/4462)
          is complete, schema inference is only available where the key of the data
          is a STRING, as is the case here.

    !!! note
          The data generated has the same value in the Kafka record's key
          as is in the userId field in the value. Specifying `key='userId'`
          in the WITH clause above lets ksqlDB know this. This information
          will be used later allow joins against the table to use the more
          descriptive `userId` column name, rather than `ROWKEY`. Joining
          on either will yield the same results. If your data does not
          contain a copy of the key in the value simply join on `ROWKEY`.

    !!! tip
          You can run `DESCRIBE users_original;` to see the schema for the
          Table.

3.  Optional: Show all streams and tables.
    ```
    ksql> SHOW STREAMS;

     Stream Name         | Kafka Topic                 | Format    
    ---------------------------------------------------------------
     KSQL_PROCESSING_LOG | default_ksql_processing_log | JSON      
     PAGEVIEWS_ORIGINAL  | pageviews                   | DELIMITED 
    ---------------------------------------------------------------

    ksql> SHOW TABLES;

     Table Name     | Kafka Topic | Format | Windowed 
    --------------------------------------------------
     USERS_ORIGINAL | users       | AVRO   | false    
    --------------------------------------------------
    ```

!!! tip
    Notice the `KSQL_PROCESSING_LOG` stream listed in the SHOW STREAMS
    output? KsqlDB will append messages detailing any issues it
    encountered while processing your data. If things are not working
    as you expect it can be worth checking the contents of this stream
    to see if ksqlDB is encountering data errors.

Viewing your data
=================

1. Use `SELECT` to create a query that returns data from a TABLE. This
   query includes the `LIMIT` keyword to limit the number of rows
   returned in the query result, and the `EMIT CHANGES` keywords to
   indicate we wish to stream results back. This is known as a
   [pull query](../concepts/queries/pull.md). See the
   [queries](../concepts/queries/index.md) for an explanation of the
   different query types. Note that exact data output may vary because
   of the randomness of the data generation.
   ```sql
   SELECT * from users_original emit changes limit 5;
   ```

   Your output should resemble:

   ```
   +--------------------+--------------+--------------+---------+----------+-------------+
   |ROWTIME             |ROWKEY        |REGISTERTIME  |GENDER   |REGIONID  |USERID       |
   +--------------------+--------------+--------------+---------+----------+-------------+
   |1581077558655       |User_9        |1513529638461 |OTHER    |Region_1  |User_9       |
   |1581077561454       |User_7        |1489408314958 |OTHER    |Region_2  |User_7       |
   |1581077561654       |User_3        |1511291005264 |MALE     |Region_2  |User_3       |
   |1581077561857       |User_4        |1496797956753 |OTHER    |Region_1  |User_4       |
   |1581077562858       |User_8        |1489169082491 |FEMALE   |Region_8  |User_8       |
   Limit Reached
   Query terminated
   ```
   !!! note
         Push queries on tables will output the full history of the table that is stored
         in the Kafka changelog topic, i.e. it will output historic data, followed by the
         stream of updates to the table. It is therefore likely that rows with matching
         `ROWKEY` are output as existing rows in the table are updated.

2. View the data in your pageviews_original stream by issuing the following
   push query:
   ```sql
   SELECT viewtime, userid, pageid FROM pageviews_original emit changes LIMIT 3;
   ```

   Your output should resemble:

   ```
   +--------------+--------------+--------------+
   |VIEWTIME      |USERID        |PAGEID        |
   +--------------+--------------+--------------+
   |1581078296791 |User_1        |Page_54       |
   |1581078297792 |User_8        |Page_93       |
   |1581078298792 |User_6        |Page_26       |
   Limit Reached
   Query terminated
   ```

   !!! note
      By default, push queries on streams only output changes that occur
      after the query is started, i.e. historic data is not included.
      Run `set 'auto.offset.reset'='earliest';` to update your session
      properties if you want to see the historic data.

Write Queries
=============

These examples write queries using KSQL.

1.  Create query that enriches the pageviews data with the user's gender
    and regionid from the users table. The following query enriches the
    `pageviews_original` STREAM by doing a `LEFT JOIN` with the
    `users_original` TABLE on the userid column.

    ```sql
    SELECT users_original.userid AS userid, pageid, regionid, gender
      FROM pageviews_original
      LEFT JOIN users_original
        ON pageviews_original.userid = users_original.userid
      EMIT CHANGES
      LIMIT 5;
    ```

    Your output should resemble:

    ```
    +-------------------+-------------------+-------------------+-------------------+
    |USERID             |PAGEID             |REGIONID           |GENDER             |
    +-------------------+-------------------+-------------------+-------------------+
    |User_7             |Page_23            |Region_2           |OTHER              |
    |User_3             |Page_42            |Region_2           |MALE               |
    |User_7             |Page_87            |Region_2           |OTHER              |
    |User_2             |Page_57            |Region_5           |FEMALE             |
    |User_9             |Page_59            |Region_1           |OTHER              |
    Limit Reached
    Query terminated
    ```

    !!! note
      The join to the users table is on the userid column, which was identified as
      an alias for the tables primary key, `ROWKEY`, in the CREATE TABLE statement.
      `userId` and `ROWKEY` can be used interchangeably as the join criteria for
      the table. However, the data in `userid` on the stream side does not match
      the stream's key. Hence, ksqlDB will first internally repartition the stream
      by the `userId` column.

2.  Create a persistent query by using the `CREATE STREAM` keywords to
    precede the `SELECT` statement, and removing the `LIMIT` clause.
    The results from this query are written to the `PAGEVIEWS_ENRICHED` Kafka topic.

    ```sql
    CREATE STREAM pageviews_enriched AS
      SELECT users_original.userid AS userid, pageid, regionid, gender
      FROM pageviews_original
      LEFT JOIN users_original
        ON pageviews_original.userid = users_original.userid
      EMIT CHANGES;
    ```

    Your output should resemble:

    ```
     Message                                                                                                  
    ----------------------------------------------------------------------------------------------------------
     Stream PAGEVIEWS_ENRICHED created and running. Created by query with query ID: CSAS_PAGEVIEWS_ENRICHED_0 
    ----------------------------------------------------------------------------------------------------------
    ```

    !!! tip
		    You can run `DESCRIBE pageviews_enriched;` to describe the stream.

3.  Use `SELECT` to view query results as they come in. To stop viewing
    the query results, press Ctrl-C. This stops printing to the
    console but it does not terminate the actual query. The query
    continues to run in the underlying KSQL application.

    ```sql
    SELECT * FROM pageviews_enriched emit changes;
    ```

    Your output should resemble:

    ```
    +-------------+------------+------------+------------+------------+------------+
    |ROWTIME      |ROWKEY      |USERID      |PAGEID      |REGIONID    |GENDER      |
    +-------------+------------+------------+------------+------------+------------+
    |1581079706741|User_5      |User_5      |Page_53     |Region_3    |FEMALE      |
    |1581079707742|User_2      |User_2      |Page_86     |Region_5    |OTHER       |
    |1581079708745|User_9      |User_9      |Page_75     |Region_1    |OTHER       |
    
    ^CQuery terminated
    ```

    Use CTRL+C to terminate the query.

4.  Create a new persistent query where a condition limits the streams
    content, using `WHERE`. Results from this query are written to a
    Kafka topic called `PAGEVIEWS_FEMALE`.

    ```sql
    CREATE STREAM pageviews_female AS
      SELECT * FROM pageviews_enriched
      WHERE gender = 'FEMALE'
      EMIT CHANGES;
    ```

    Your output should resemble:

    ```
     Message                                                                                               
    -------------------------------------------------------------------------------------------------------
     Stream PAGEVIEWS_FEMALE created and running. Created by query with query ID: CSAS_PAGEVIEWS_FEMALE_11 
    -------------------------------------------------------------------------------------------------------
    ```

    !!! tip
		    You can run `DESCRIBE pageviews_female;` to describe the stream.

5.  Create a new persistent query where another condition is met, using
    `LIKE`. Results from this query are written to the
    `pageviews_enriched_r8_r9` Kafka topic.

    ```sql
    CREATE STREAM pageviews_female_like_89
      WITH (kafka_topic='pageviews_enriched_r8_r9') AS
      SELECT * FROM pageviews_female
      WHERE regionid LIKE '%_8' OR regionid LIKE '%_9'
      EMIT CHANGES;
    ```

    Your output should resemble:

    ```
     Message                                                                                                               
    -----------------------------------------------------------------------------------------------------------------------
     Stream PAGEVIEWS_FEMALE_LIKE_89 created and running. Created by query with query ID: CSAS_PAGEVIEWS_FEMALE_LIKE_89_13 
    -----------------------------------------------------------------------------------------------------------------------
    ```

6.  Create a new persistent query that counts the pageviews for each
    region and gender combination in a [tumbling
    window](https://docs.confluent.io/current/streams/developer-guide/dsl-api.html#tumbling-time-windows)
    of 30 seconds when the count is greater than one. Results from this
    query are written to the `PAGEVIEWS_REGIONS` Kafka topic in the Avro
    format. KSQL will register the Avro schema with the configured
    {{ site.sr }} when it writes the first message to the
    `PAGEVIEWS_REGIONS` topic.

    ```sql
    CREATE TABLE pageviews_regions
      WITH (VALUE_FORMAT='avro') AS
    SELECT gender, regionid , COUNT(*) AS numusers
    FROM pageviews_enriched
      WINDOW TUMBLING (size 30 second)
    GROUP BY gender, regionid
    EMIT CHANGES;
    ```

    Your output should resemble:

    ```
     Message                                                                                                
    --------------------------------------------------------------------------------------------------------
     Table PAGEVIEWS_REGIONS created and running. Created by query with query ID: CTAS_PAGEVIEWS_REGIONS_15 
    --------------------------------------------------------------------------------------------------------
    ```

    !!! tip
		    You can run `DESCRIBE pageviews_regions;` to describe the table.

7.  Optional: View results from the above queries using push query.

    ```sql
    SELECT * FROM pageviews_regions EMIT CHANGES LIMIT 5;
    ```

    Your output should resemble:

    ```
    +---------------+-----------------+---------------+---------------+---------------+---------------+---------------+
    |ROWTIME        |ROWKEY           |WINDOWSTART    |WINDOWEND      |GENDER         |REGIONID       |NUMUSERS       |
    +---------------+-----------------+---------------+---------------+---------------+---------------+---------------+
    |1581080500530  |OTHER|+|Region_9 |1581080490000  |1581080520000  |OTHER          |Region_9       |1              |
    |1581080501530  |OTHER|+|Region_5 |1581080490000  |1581080520000  |OTHER          |Region_5       |2              |
    |1581080510532  |MALE|+|Region_7  |1581080490000  |1581080520000  |MALE           |Region_7       |4              |
    |1581080513532  |FEMALE|+|Region_1|1581080490000  |1581080520000  |FEMALE         |Region_1       |2              |
    |1581080516533  |MALE|+|Region_2  |1581080490000  |1581080520000  |MALE           |Region_2       |3              |
    Limit Reached
    Query terminated
    ```
    !!! note
       Notice the addition of the WINDOWSTART and WINDOWEND columns.
       These are available because `pageviews_regions` is aggregating data
       per 30 second _window_. ksqlDB automatically adds these system columns
       for windowed results.

8.  Optional: View results from the above queries using pull query

    When a CREATE TABLE statement contains a GROUP BY clause, ksqlDB is internally building an
    table containing the results of the aggregation. ksqlDB supports pull queries against
    such aggregation results.

    Unlike the push query used in the previous step, which _pushes_ a stream of results to you,
    pull queries pull a result set and automatically terminate.

    Push queries do not have the `EMIT CHANGES` clause.

    View all the windows and user counts available for a specific gender and region using a pull query:

    ```sql
    SELECT * FROM pageviews_regions WHERE ROWKEY='OTHER|+|Region_9';
    ```

    Your output should resemble:

    ```
    +------------------+------------------+------------------+------------------+------------------+------------------+------------------+
    |ROWKEY            |WINDOWSTART       |WINDOWEND         |ROWTIME           |GENDER            |REGIONID          |NUMUSERS          |
    +------------------+------------------+------------------+------------------+------------------+------------------+------------------+
    |OTHER|+|Region_9  |1581080490000     |1581080520000     |1581080500530     |OTHER             |Region_9          |1                 |
    |OTHER|+|Region_9  |1581080550000     |1581080580000     |1581080576526     |OTHER             |Region_9          |4                 |
    |OTHER|+|Region_9  |1581080580000     |1581080610000     |1581080606525     |OTHER             |Region_9          |4                 |
    |OTHER|+|Region_9  |1581080610000     |1581080640000     |1581080622524     |OTHER             |Region_9          |3                 |
    |OTHER|+|Region_9  |1581080640000     |1581080670000     |1581080667528     |OTHER             |Region_9          |6                 |
    ...
    ```

    Pull queries on windowed tables such as pageviews_regions also supports querying a single window's result:

    ```sql
    SELECT NUMUSERS FROM pageviews_regions WHERE ROWKEY='OTHER|+|Region_9' AND WINDOWSTART=1581080550000;
    ```

    !!! important
       You will need to change value of `WINDOWSTART` in the above SQL to match one of the window boundaries in your data.
       Otherwise no results will be returned.

    Your output should resemble:

    ```
    +----------+
    |NUMUSERS  |
    +----------+
    |4         |
    Query terminated
    ```

    Or querying a range of windows:

    ```sql
    SELECT WINDOWSTART, WINDOWEND, NUMUSERS FROM pageviews_regions WHERE ROWKEY='OTHER|+|Region_9' AND 1581080550000 <= WINDOWSTART AND WINDOWSTART <= 1581080610000;
    ```

    !!! important
       You will need to change value of `WINDOWSTART` in the above SQL to match one of the window boundaries in your data.
       Otherwise no results will be returned.

    Your output should resemble:

    ```
    +----------------------------+----------------------------+----------------------------+
    |WINDOWSTART                 |WINDOWEND                   |NUMUSERS                    |
    +----------------------------+----------------------------+----------------------------+
    |1581080550000               |1581080580000               |4                           |
    |1581080580000               |1581080610000               |4                           |
    |1581080610000               |1581080640000               |3                           |
    Query terminated
    ```

9.  Optional: Show all persistent queries.

    ```sql
    SHOW QUERIES;
    ```

    Your output should resemble:

    ```
     Query ID                         | Status  | Sink Name                | Sink Kafka Topic         | Query String                                                                                                                                                                                                                                                                                                                                                                                                 
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    CTAS_PAGEVIEWS_REGIONS_15        | RUNNING | PAGEVIEWS_REGIONS        | PAGEVIEWS_REGIONS        | CREATE TABLE PAGEVIEWS_REGIONS WITH (KAFKA_TOPIC='PAGEVIEWS_REGIONS', PARTITIONS=1, REPLICAS=1, VALUE_FORMAT='avro') AS SELECT  PAGEVIEWS_ENRICHED.GENDER GENDER,  PAGEVIEWS_ENRICHED.REGIONID REGIONID,  COUNT(*) NUMUSERSFROM PAGEVIEWS_ENRICHED PAGEVIEWS_ENRICHEDWINDOW TUMBLING ( SIZE 30 SECONDS ) GROUP BY PAGEVIEWS_ENRICHED.GENDER, PAGEVIEWS_ENRICHED.REGIONIDEMIT CHANGES;                        
    CSAS_PAGEVIEWS_FEMALE_LIKE_89_13 | RUNNING | PAGEVIEWS_FEMALE_LIKE_89 | pageviews_enriched_r8_r9 | CREATE STREAM PAGEVIEWS_FEMALE_LIKE_89 WITH (KAFKA_TOPIC='pageviews_enriched_r8_r9', PARTITIONS=1, REPLICAS=1) AS SELECT *FROM PAGEVIEWS_FEMALE PAGEVIEWS_FEMALEWHERE ((PAGEVIEWS_FEMALE.REGIONID LIKE '%_8') OR (PAGEVIEWS_FEMALE.REGIONID LIKE '%_9'))EMIT CHANGES;                                                                                                                                        
    CSAS_PAGEVIEWS_ENRICHED_0        | RUNNING | PAGEVIEWS_ENRICHED       | PAGEVIEWS_ENRICHED       | CREATE STREAM PAGEVIEWS_ENRICHED WITH (KAFKA_TOPIC='PAGEVIEWS_ENRICHED', PARTITIONS=1, REPLICAS=1) AS SELECT  USERS_ORIGINAL.USERID USERID,  PAGEVIEWS_ORIGINAL.PAGEID PAGEID,  USERS_ORIGINAL.REGIONID REGIONID,  USERS_ORIGINAL.GENDER GENDERFROM PAGEVIEWS_ORIGINAL PAGEVIEWS_ORIGINALLEFT OUTER JOIN USERS_ORIGINAL USERS_ORIGINAL ON ((PAGEVIEWS_ORIGINAL.USERID = USERS_ORIGINAL.USERID))EMIT CHANGES; 
    CSAS_PAGEVIEWS_FEMALE_11         | RUNNING | PAGEVIEWS_FEMALE         | PAGEVIEWS_FEMALE         | CREATE STREAM PAGEVIEWS_FEMALE WITH (KAFKA_TOPIC='PAGEVIEWS_FEMALE', PARTITIONS=1, REPLICAS=1) AS SELECT *FROM PAGEVIEWS_ENRICHED PAGEVIEWS_ENRICHEDWHERE (PAGEVIEWS_ENRICHED.GENDER = 'FEMALE')EMIT CHANGES;                                                                                              
    
    For detailed information on a Query run: EXPLAIN <Query ID>;
    ```

10.  Optional: Examine query run-time metrics and details. Observe that
    information including the target Kafka topic is available, as well
    as throughput figures for the messages being processed.

    ```sql
    DESCRIBE EXTENDED PAGEVIEWS_REGIONS;
    ```

    Your output should resemble:

    ```
    Name                 : PAGEVIEWS_REGIONS
    Type                 : TABLE
    Key field            : 
    Timestamp field      : Not set - using <ROWTIME>
    Key format           : KAFKA
    Value format         : AVRO
    Kafka topic          : PAGEVIEWS_REGIONS (partitions: 1, replication: 1)
    Statement            : CREATE TABLE PAGEVIEWS_REGIONS WITH (KAFKA_TOPIC='PAGEVIEWS_REGIONS', PARTITIONS=1, REPLICAS=1, VALUE_FORMAT='json') AS SELECT
      PAGEVIEWS_ENRICHED.GENDER GENDER,
      PAGEVIEWS_ENRICHED.REGIONID REGIONID,
      COUNT(*) NUMUSERS
    FROM PAGEVIEWS_ENRICHED PAGEVIEWS_ENRICHED
    WINDOW TUMBLING ( SIZE 30 SECONDS ) 
    GROUP BY PAGEVIEWS_ENRICHED.GENDER, PAGEVIEWS_ENRICHED.REGIONID
    EMIT CHANGES;
    
     Field    | Type                                              
    --------------------------------------------------------------
     ROWTIME  | BIGINT           (system)                         
     ROWKEY   | VARCHAR(STRING)  (system) (Window type: TUMBLING) 
     GENDER   | VARCHAR(STRING)                                   
     REGIONID | VARCHAR(STRING)                                   
     NUMUSERS | BIGINT                                            
    --------------------------------------------------------------
    
    Queries that write from this TABLE
    -----------------------------------
    CTAS_PAGEVIEWS_REGIONS_15 (RUNNING) : CREATE TABLE PAGEVIEWS_REGIONS WITH (KAFKA_TOPIC='PAGEVIEWS_REGIONS', PARTITIONS=1, REPLICAS=1, VALUE_FORMAT='json') AS SELECT  PAGEVIEWS_ENRICHED.GENDER GENDER,  PAGEVIEWS_ENRICHED.REGIONID REGIONID,  COUNT(*) NUMUSERSFROM PAGEVIEWS_ENRICHED PAGEVIEWS_ENRICHEDWINDOW TUMBLING ( SIZE 30 SECONDS ) GROUP BY PAGEVIEWS_ENRICHED.GENDER, PAGEVIEWS_ENRICHED.REGIONIDEMIT CHANGES;
    
    For query topology and execution plan please run: EXPLAIN <QueryId>
    
    Local runtime statistics
    ------------------------
    messages-per-sec:      0.90   total-messages:       498     last-message: 2020-02-07T13:10:32.033Z
    ```

Using Nested Schemas (STRUCT) in KSQL
-------------------------------------

Struct support enables the modeling and access of nested data in Kafka
topics, from both JSON and Avro.

In this section, you use the `ksql-datagen` tool to create some sample data
that includes a nested `address` field. Run this in a new window, and leave
it running.

```bash
<path-to-confluent>/bin/ksql-datagen  \
     quickstart=orders \
     format=json \
     topic=orders \
     msgRate=5
```

From the KSQL command prompt, register the topic in KSQL:

```sql
CREATE STREAM ORDERS 
 (
   ROWKEY INT KEY, 
   ORDERTIME BIGINT, 
   ORDERID INT, 
   ITEMID STRING, 
   ORDERUNITS DOUBLE, 
   ADDRESS STRUCT<CITY STRING, STATE STRING, ZIPCODE BIGINT>
 )
   WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='json', key='orderid');
```

Your output should resemble:

```
 Message
----------------
 Stream created
----------------
```

Use the `DESCRIBE` function to observe the schema, which includes a
`STRUCT`:

```sql
DESCRIBE ORDERS;
```

Your output should resemble:

```
Name                 : ORDERS
 Field      | Type
----------------------------------------------------------------------------------
 ROWTIME    | BIGINT           (system)
 ROWKEY     | INT              (system)
 ORDERTIME  | BIGINT
 ORDERID    | INTEGER
 ITEMID     | VARCHAR(STRING)
 ORDERUNITS | DOUBLE
 ADDRESS    | STRUCT<CITY VARCHAR(STRING), STATE VARCHAR(STRING), ZIPCODE BIGINT>
----------------------------------------------------------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
ksql>
```

Query the data, using `->` notation to access the Struct contents:

```sql
SELECT ORDERID, ADDRESS->CITY FROM ORDERS EMIT CHANGES LIMIT 5;
```

Your output should resemble:

```
+-----------------------------------+-----------------------------------+
|ORDERID                            |ADDRESS__CITY                      |
+-----------------------------------+-----------------------------------+
|1188                               |City_95                            |
|1189                               |City_24                            |
|1190                               |City_57                            |
|1191                               |City_37                            |
|1192                               |City_82                            |
Limit Reached
Query terminated
```

Stream-Stream join
------------------

Using a stream-stream join, it is possible to join two *event streams*
on a common key. An example of this could be a stream of order events and
a stream of shipment events. By joining these on the order key, you can
see shipment information alongside the order.

In the ksqlDB CLI create two new streams, both streams will store their
order id in ROWKEY:

```sql
CREATE STREAM NEW_ORDERS (ROWKEY INT KEY, TOTAL_AMOUNT DOUBLE, CUSTOMER_NAME VARCHAR)
WITH (KAFKA_TOPIC='new_orders', VALUE_FORMAT='JSON', PARTITIONS=2);

CREATE STREAM SHIPMENTS (ROWKEY INT KEY, SHIPMENT_ID INT, WAREHOUSE VARCHAR)
WITH (KAFKA_TOPIC='shipments', VALUE_FORMAT='JSON', PARTITIONS=2);
```

!!! note
  ksqlDB will create the underlying topics in Kafka when these statements
  are executed. You can also specify the `REPLICAS` count.

After both `CREATE STREAM` statements, your output should resemble:

```
 Message
----------------
 Stream created
----------------
```

Populate the streams with some sample data using the INSERT VALUES statement:

```sql
-- Insert values in NEW_ORDERS:
-- insert supplying the list of columns to insert:
INSERT INTO NEW_ORDERS (ROWKEY, CUSTOMER_NAME, TOTAL_AMOUNT) 
  VALUES (1, 'Bob Smith', 10.50);
  
-- short hand version can be used when inserting values for all columns, (except ROWTIME), in column order:
INSERT INTO NEW_ORDERS  VALUES (2, 3.32, 'Sarah Black');
INSERT INTO NEW_ORDERS  VALUES (3, 21.00, 'Emma Turner');

-- Insert values in SHIPMENTS:
INSERT INTO SHIPMENTS VALUES (1, 42, 'Nashville');
INSERT INTO SHIPMENTS VALUES (3, 43, 'Palo Alto');
```

Query the data to confirm that it's present in the topics.

!!! tip
      Run the following to tell KSQL to read from the beginning of each stream:
      ```sql
      SET 'auto.offset.reset' = 'earliest';
      ```
      You can skip this if you have already run it within your current
      KSQL CLI session.

For the `NEW_ORDERS` topic, run:

```sql
SELECT * FROM NEW_ORDERS EMIT CHANGES LIMIT 3;
```

Your output should resemble:

```
+-------------------------+-------------------------+-------------------------+-------------------------+
|ROWTIME                  |ROWKEY                   |TOTAL_AMOUNT             |CUSTOMER_NAME            |
+-------------------------+-------------------------+-------------------------+-------------------------+
|1581083057609            |1                        |10.5                     |Bob Smith                |
|1581083178418            |2                        |3.32                     |Sarah Black              |
|1581083210494            |3                        |21.0                     |Emma Turner              |
Limit Reached
Query terminated
```

For the `SHIPMENTS` topic, run:

```sql
SELECT * FROM SHIPMENTS EMIT CHANGES LIMIT 2;
```

Your output should resemble:

```
+-------------------------+-------------------------+-------------------------+-------------------------+
|ROWTIME                  |ROWKEY                   |SHIPMENT_ID              |WAREHOUSE                |
+-------------------------+-------------------------+-------------------------+-------------------------+
|1581083340711            |1                        |42                       |Nashville                |
|1581083384229            |3                        |43                       |Palo Alto                |
Limit Reached
Query terminated
```

Run the following query, which will show orders with associated
shipments, based on a join window of 1 hour.

```sql
SELECT O.ROWKEY AS ORDER_ID, O.TOTAL_AMOUNT, O.CUSTOMER_NAME,
S.SHIPMENT_ID, S.WAREHOUSE
FROM NEW_ORDERS O
INNER JOIN SHIPMENTS S
  WITHIN 1 HOURS
  ON O.ROWKEY = S.ROWKEY
EMIT CHANGES;
```

Your output should resemble:

```
+--------------------------+--------------------------+--------------------------+--------------------------+--------------------------+
|ORDER_ID                  |TOTAL_AMOUNT              |CUSTOMER_NAME             |SHIPMENT_ID               |WAREHOUSE                 |
+--------------------------+--------------------------+--------------------------+--------------------------+--------------------------+
|1                         |10.5                      |Bob Smith                 |42                        |Nashville                 |
|3                         |21.0                      |Emma Turner               |43                        |Palo Alto                 |
```

Note that message with `ORDER_ID=2` has no corresponding `SHIPMENT_ID`
or `WAREHOUSE` - this is because there is no corresponding row on the
shipments stream within the time window specified.

Start the ksqlDB CLI in a second window by running:

```bash
LOG_DIR=./ksql_logs <path-to-confluent>/bin/ksql
```

Enter the following INSERT VALUES statement to insert the shipment for
order id 2:

```sql
INSERT INTO SHIPMENTS VALUES (2, 49, 'London');
```

Switching back to your primary ksqlDB CLI window, notice that a third
row has now been output:

```
+--------------------------+--------------------------+--------------------------+--------------------------+--------------------------+
|ORDER_ID                  |TOTAL_AMOUNT              |CUSTOMER_NAME             |SHIPMENT_ID               |WAREHOUSE                 |
+--------------------------+--------------------------+--------------------------+--------------------------+--------------------------+
|1                         |10.5                      |Bob Smith                 |42                        |Nashville                 |
|3                         |21.0                      |Emma Turner               |43                        |Palo Alto                 |
|2                         |3.32                      |Sarah Black               |49                        |London                    |
```

Press Ctrl+C to cancel the `SELECT` query and return to the KSQL prompt.

Table-Table join
----------------

Using a table-table join, it is possible to join two *tables* of on a
common key. KSQL tables provide the latest *value* for a given *key*.
They can only be joined on the *key*, and one-to-many (1:N) joins are
not supported in the current semantic model.

In this example we have location data about a warehouse from one system,
being enriched with data about the size of the warehouse from another.

In the KSQL CLI, register both topics as KSQL tables. Note, in this example
the warehouse id is stored both in the key and in the WAREHOUSE_ID field
in the value:

```sql
CREATE TABLE WAREHOUSE_LOCATION 
   (ROWKEY INT KEY, WAREHOUSE_ID INT, CITY VARCHAR, COUNTRY VARCHAR)
   WITH (KAFKA_TOPIC='warehouse_location',
      VALUE_FORMAT='JSON',
      KEY='WAREHOUSE_ID',
      PARTITIONS=2);

CREATE TABLE WAREHOUSE_SIZE 
   (ROWKEY INT KEY, WAREHOUSE_ID INT, SQUARE_FOOTAGE DOUBLE)
   WITH (KAFKA_TOPIC='warehouse_size',
      VALUE_FORMAT='JSON',
      KEY='WAREHOUSE_ID',
      PARTITIONS=2);
```

After both `CREATE TABLE` statements, your output should resemble:

```
 Message
---------------
 Table created
---------------
```

In the KSQL CLI, insert sample data into the tables:

```sql
-- note: ksqlDB will automatically populate ROWKEY with the same value as WAREHOUSE_ID:
INSERT INTO WAREHOUSE_LOCATION (WAREHOUSE_ID, CITY, COUNTRY) VALUES (1, 'Leeds', 'UK');
INSERT INTO WAREHOUSE_LOCATION (WAREHOUSE_ID, CITY, COUNTRY) VALUES (2, 'Sheffield', 'UK');
INSERT INTO WAREHOUSE_LOCATION (WAREHOUSE_ID, CITY, COUNTRY) VALUES (3, 'Berlin', 'Germany');

INSERT INTO WAREHOUSE_SIZE (WAREHOUSE_ID, SQUARE_FOOTAGE) VALUES (1, 16000);
INSERT INTO WAREHOUSE_SIZE (WAREHOUSE_ID, SQUARE_FOOTAGE) VALUES (2, 42000);
INSERT INTO WAREHOUSE_SIZE (WAREHOUSE_ID, SQUARE_FOOTAGE) VALUES (3, 94000);
```

Check both tables that the message key (`ROWKEY`) matches the declared
key (`WAREHOUSE_ID`) - the output should show that they are equal. If
they were not, the join will not succeed or behave as expected.

Inspect the WAREHOUSE_LOCATION table:

```sql
SELECT ROWKEY, WAREHOUSE_ID FROM WAREHOUSE_LOCATION EMIT CHANGES LIMIT 3;
```

Your output should resemble:

```
+---------------------------------------+---------------------------------------+
|ROWKEY                                 |WAREHOUSE_ID                           |
+---------------------------------------+---------------------------------------+
|2                                      |2                                      |
|1                                      |1                                      |
|3                                      |3                                      |
Limit Reached
Query terminated
```

Inspect the WAREHOUSE_SIZE table:

```sql
SELECT ROWKEY, WAREHOUSE_ID FROM WAREHOUSE_SIZE EMIT CHANGES LIMIT 3;
```

Your output should resemble:

```
+---------------------------------------+---------------------------------------+
|ROWKEY                                 |WAREHOUSE_ID                           |
+---------------------------------------+---------------------------------------+
|2                                      |2                                      |
|1                                      |1                                      |
|3                                      |3                                      |
Limit Reached
Query terminated
```

Now join the two tables:

```sql
SELECT WL.WAREHOUSE_ID, WL.CITY, WL.COUNTRY, WS.SQUARE_FOOTAGE
FROM WAREHOUSE_LOCATION WL
  LEFT JOIN WAREHOUSE_SIZE WS
    ON WL.WAREHOUSE_ID=WS.WAREHOUSE_ID
EMIT CHANGES
LIMIT 3;
```

Your output should resemble:

```
+------------------+------------------+------------------+------------------+
|WL_WAREHOUSE_ID   |CITY              |COUNTRY           |SQUARE_FOOTAGE    |
+------------------+------------------+------------------+------------------+
|1                 |Leeds             |UK                |16000.0           |
|1                 |Leeds             |UK                |16000.0           |
|2                 |Sheffield         |UK                |42000.0           |
Limit Reached
Query terminated
```

INSERT INTO
-----------

The `INSERT INTO` syntax can be used to merge the contents of multiple
streams. An example of this could be where the same event type is coming
from different sources.

Run two datagen processes, each writing to a different topic, simulating
order data arriving from a local installation vs from a third-party:
:::

!!! tip
      Each of these commands should be run in a separate window. When the
      exercise is finished, exit them by pressing Ctrl-C.

```bash
<path-to-confluent>/bin/ksql-datagen \
     quickstart=orders \
     format=json \
     topic=orders_local \
     msgRate=2

<path-to-confluent>/bin/ksql-datagen \
     quickstart=orders \
     format=json \
     topic=orders_3rdparty \
     msgRate=2
```

In KSQL, register the source topic for each:

```sql
CREATE STREAM ORDERS_SRC_LOCAL
 (
   ROWKEY INT KEY, 
   ORDERTIME BIGINT, 
   ORDERID INT, 
   ITEMID STRING, 
   ORDERUNITS DOUBLE, 
   ADDRESS STRUCT<CITY STRING, STATE STRING, ZIPCODE BIGINT>
 )
  WITH (KAFKA_TOPIC='orders_local', VALUE_FORMAT='JSON');

CREATE STREAM ORDERS_SRC_3RDPARTY
 (
   ROWKEY INT KEY, 
   ORDERTIME BIGINT, 
   ORDERID INT, 
   ITEMID STRING, 
   ORDERUNITS DOUBLE, 
   ADDRESS STRUCT<CITY STRING, STATE STRING, ZIPCODE BIGINT>
 )
  WITH (KAFKA_TOPIC='orders_3rdparty', VALUE_FORMAT='JSON');
```

After each `CREATE STREAM` statement you should get the message:

```
 Message
----------------
 Stream created
----------------
```

Create the output stream, using the standard `CREATE STREAM … AS`
syntax. Because multiple sources of data are being joined into a common
target, it is useful to add in lineage information. This can be done by
simply including it as part of the `SELECT`:

```sql
CREATE STREAM ALL_ORDERS AS SELECT 'LOCAL' AS SRC, * FROM ORDERS_SRC_LOCAL EMIT CHANGES;
```

Your output should resemble:

```
 Message                                                                                   
-------------------------------------------------------------------------------------------
 Stream ALL_ORDERS created and running. Created by query with query ID: CSAS_ALL_ORDERS_17 
-------------------------------------------------------------------------------------------
```

Use the `DESCRIBE` command to observe the schema of the target stream.

```sql
DESCRIBE ALL_ORDERS;
```

Your output should resemble:

```
Name                 : ALL_ORDERS
 Field      | Type                                                                
----------------------------------------------------------------------------------
 ROWTIME    | BIGINT           (system)                                           
 ROWKEY     | INTEGER          (system)                                           
 SRC        | VARCHAR(STRING)                                                     
 ORDERTIME  | BIGINT                                                              
 ORDERID    | INTEGER                                                             
 ITEMID     | VARCHAR(STRING)                                                     
 ORDERUNITS | DOUBLE                                                              
 ADDRESS    | STRUCT<CITY VARCHAR(STRING), STATE VARCHAR(STRING), ZIPCODE BIGINT> 
----------------------------------------------------------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
```

Add stream of 3rd party orders into the existing output stream:

```sql
INSERT INTO ALL_ORDERS SELECT '3RD PARTY' AS SRC, * FROM ORDERS_SRC_3RDPARTY EMIT CHANGES;
```

Your output should resemble:

```
 Message                                                    
------------------------------------------------------------
 Insert Into query is running with query ID: InsertQuery_43 
------------------------------------------------------------
```

Query the output stream to verify that data from each source is being
written to it:

```sql
SELECT * FROM ALL_ORDERS EMIT CHANGES;
```

Your output should resemble the following. Note that there are messages
from both source topics (denoted by `LOCAL` and `3RD PARTY`
respectively).

```
+--------------+----------+-----------+--------------+----------+-------------+----------------------+---------------------------------------------+
|ROWTIME       |ROWKEY    |SRC        |ORDERTIME     |ORDERID   |ITEMID       |ORDERUNITS            |ADDRESS                                      |
+--------------+----------+-----------+--------------+----------+-------------+----------------------+---------------------------------------------+
|1581085344272 |510       |3RD PARTY  |1503198352036 |510       |Item_643     |1.653210222047296     |{CITY=City_94, STATE=State_72, ZIPCODE=61274}|
|1581085344293 |546       |LOCAL      |1498476865306 |546       |Item_234     |9.284691223615178     |{CITY=City_44, STATE=State_29, ZIPCODE=84678}|
|1581085344776 |511       |3RD PARTY  |1489945722538 |511       |Item_264     |8.213163488516212     |{CITY=City_36, STATE=State_13, ZIPCODE=44821}|
…
```

Press Ctrl+C to cancel the `SELECT` query and return to the KSQL prompt.

You can view the two queries that are running using `SHOW QUERIES`:

```sql
SHOW QUERIES;
```

Your output should resemble:

```
 Query ID                         | Status  | Sink Name                | Sink Kafka Topic         | Query String                                                                                                                                                                                                                                                                                                                                                                                                 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 InsertQuery_43                   | RUNNING | ALL_ORDERS               | ALL_ORDERS               | INSERT INTO ALL_ORDERS SELECT '3RD PARTY' AS SRC, * FROM ORDERS_SRC_3RDPARTY EMIT CHANGES;                                                                                                                                                                                                                                                                                                                   
 CSAS_ALL_ORDERS_17               | RUNNING | ALL_ORDERS               | ALL_ORDERS               | CREATE STREAM ALL_ORDERS WITH (KAFKA_TOPIC='ALL_ORDERS', PARTITIONS=1, REPLICAS=1) AS SELECT  'LOCAL' SRC,  *FROM ORDERS_SRC_LOCAL ORDERS_SRC_LOCALEMIT CHANGES;                                                                                                                                                                                                                                             
...
```

Terminate and Exit
==================

KSQL
----

!!! important
      Persisted queries will continuously run as KSQL
      applications until they are manually terminated. Exiting KSQL CLI does
      not terminate persistent queries.

1.  From the output of `SHOW QUERIES;` identify a query ID you would
    like to terminate. For example, if you wish to terminate query ID
    `CTAS_PAGEVIEWS_REGIONS_15`:

    ```sql
    TERMINATE CTAS_PAGEVIEWS_REGIONS_15;
    ```

    !!! tip
          The actual name of the query running may vary; refer to the output
          of `SHOW QUERIES;`.

2.  Run the `exit` command to leave the KSQL CLI.

    ```
    ksql> exit
    Exiting ksqlDB.
    ```

Confluent CLI
-------------

If you are running {{ site.cp }} using the CLI, you can stop it with
this command.

```bash
<path-to-confluent>/bin/confluent local stop
```

Page last revised on: {{ git_revision_date }}
