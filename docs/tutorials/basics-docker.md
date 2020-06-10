---
layout: page
title: Write streaming queries against Apache Kafka® by using ksqlDB
tagline: Use ksqlDB for event streaming applications
description: Learn to write streaming SQL queries
keywords: ksqldb, docker
---

This tutorial demonstrates a simple workflow using ksqlDB to write
streaming queries against messages in {{ site.aktm }} in a Docker
environment.

To get started, you must start a {{ site.ak }} cluster, including
{{ site.zk }}, and a {{ site.ak }} broker. ksqlDB queries messages
from this {{ site.ak }} cluster.

Prerequisites:

-   Docker:
    -   Docker version 1.11 or later is [installed and
        running](https://docs.docker.com/engine/installation/).
    -   Docker Compose is
        [installed](https://docs.docker.com/compose/install/).
        Docker Compose is installed by default with Docker for Mac.
    -   Docker memory is allocated minimally at 8 GB. When using
        Docker Desktop for Mac, the default Docker memory allocation
        is 2 GB. You can change the default allocation to 8 GB in
        **Docker** > **Preferences** > **Advanced**.
-   [Git](https://git-scm.com/downloads).
-   Internet connectivity.
-   Ensure you are on an
    [Operating System](https://docs.confluent.io/current/installation/versions-interoperability.html#operating-systems) currently
    supported by {{ site.cp }}.
-   [wget](https://www.gnu.org/software/wget/) to get the connector
    configuration file.

Download the Tutorial and Start ksqlDB
--------------------------------------

### 1. Clone the ksqlDB repository.

```bash
git clone https://github.com/confluentinc/ksql.git
cd ksql
```

### 2. Switch to the correct branch

Switch to the correct ksqlDB release branch.

```bash
git checkout {{ site.ksqldbreleasebranch }}
```

### 3. Launch the tutorial in Docker

Navigate to the ksqlDB repository `docs/tutorials/` directory and
launch the tutorial in Docker. Depending on your network speed, this
may take up to 5-10 minutes.

```bash
cd docs/tutorials/
docker-compose up -d
```

### 4. Run the data generator tool

From two separate terminal windows, run the data generator tool to
simulate "user" and "pageview" data:

```bash
docker run --network tutorials_default --rm --name datagen-pageviews \
    confluentinc/ksqldb-examples:{{ site.cprelease }} \
    ksql-datagen \
        bootstrap-server=kafka:39092 \
        quickstart=pageviews \
        format=delimited \
        topic=pageviews \
        msgRate=5 
```

```bash
docker run --network tutorials_default --rm --name datagen-users \
    confluentinc/ksqldb-examples:{{ site.cprelease }} \
    ksql-datagen \
        bootstrap-server=kafka:39092 \
        quickstart=users \
        format=delimited \
        topic=users \
        msgRate=1 
```

### 5. Start the ksqlDB CLI

From the host machine, start ksqlDB CLI:

```bash
docker run --network tutorials_default --rm --interactive --tty \
    confluentinc/ksqldb-cli:{{ site.release }} ksql \
    http://ksql-server:8088
```

Your output should resemble:

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

Copyright 2017-2020 Confluent Inc.

CLI v{{ site.release }}, Server v{{ site.cprelease }} located at http://ksql-server:8088

Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

ksql>
```

Inspect Kafka Topics By Using SHOW and PRINT Statements
-------------------------------------------------------

ksqlDB enables inspecting {{ site.ak }} topics and messages in real time.

- Use the SHOW TOPICS statement to list the available topics in the
  {{ site.ak }} cluster.
- Use the PRINT statement to see a topic's messages as they arrive.

In the ksqlDB CLI, run the following statement:

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
statement to see the full list of topics in the {{ site.ak }} cluster:

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
    Key format: KAFKA_STRING
    Value format: KAFKA_STRING
    rowtime: 6/10/20 9:25:39 PM UTC, key: User_1, value: 1500241674756,User_1,Region_7,MALE
    rowtime: 6/10/20 9:25:40 PM UTC, key: User_1, value: 1491524185606,User_1,Region_9,OTHER
    rowtime: 6/10/20 9:25:41 PM UTC, key: User_3, value: 1519250711583,User_3,Region_4,MALE
    rowtime: 6/10/20 9:25:42 PM UTC, key: User_7, value: 1507812294830,User_7,Region_4,OTHER
    rowtime: 6/10/20 9:25:43 PM UTC, key: User_8, value: 1499893930696,User_8,Region_8,MALE
    rowtime: 6/10/20 9:25:44 PM UTC, key: User_3, value: 1510178846565,User_3,Region_3,OTHER
    ^CTopic printing ceased
```

Press Ctrl+C to stop printing messages.

Inspect the `pageviews` topic by using the PRINT statement:

```sql
PRINT pageviews;
```

Your output should resemble:

```
    Key format: KAFKA_BIGINT or KAFKA_DOUBLE
    Format: KAFKA_STRING
    rowtime: 10/23/18 12:24:03 AM PSD, key: 1540254243183, value: 1540254243183,User_9,Page_20
    rowtime: 10/23/18 12:24:03 AM PSD, key: 1540254243617, value: 1540254243617,User_7,Page_47
    rowtime: 10/23/18 12:24:03 AM PSD, key: 1540254243888, value: 1540254243888,User_4,Page_27
    ^Crowtime: 10/23/18 12:24:05 AM PSD, key: 1540254245161, value: 1540254245161,User_9,Page_62
    Topic printing ceased
```

Press Ctrl+C to stop printing messages.

!!! note
    ksqlDB has determined that the key format is either `KAFKA_BIGINT` or `KAFKA_DOUBLE`.
    ksqlDB has not narrowed it further because it is not possible to rule out
    either format just by inspecting the key's serialized bytes. In this case we know the key is
    a `BIGINT`. For other cases you may know the key type or you may need to speak to the author
    of the data.

For more information, see [ksqlDB Syntax Reference](../developer-guide/syntax-reference.md).

Create a Stream and Table
-------------------------

These examples query messages from Kafka topics called `pageviews` and
`users` using the following schemas:

![image](../img/ksql-quickstart-schemas.jpg)

### 1. Create a ksqlDB stream

Create a stream, named `pageviews_original`, from the `pageviews`
{{ site.ak }} topic, specifying the `value_format` of `DELIMITED`.

```sql
CREATE STREAM pageviews_original (rowkey string key, viewtime bigint, userid varchar, pageid varchar)
  WITH (kafka_topic='pageviews', value_format='DELIMITED');
```

Your output should resemble:

```
 Message
----------------
 Stream created
----------------
```

!!! tip
    You can run `DESCRIBE pageviews_original;` to see the schema for the stream.

### 2. Create a ksqlDB table

Create a table, named `users_original`, from the `users` topic, specifying
the `value_format` of `DELIMITED`.

```sql
CREATE TABLE users_original (registertime BIGINT, userid VARCHAR, gender VARCHAR, regionid VARCHAR)
  WITH (KAFKA_TOPIC = 'users', VALUE_FORMAT='DELIMITED', KEY = 'userid');
```

Your output should resemble:

```
 Message
---------------
 Table created
---------------
```

!!! note
      The data generated has the same value in the {{ site.ak }} record's key
      as the `userId` field in the value. Specifying `key='userId'`
      in the WITH clause above lets ksqlDB know this. ksqlDB uses this information
      to allow joins against the table to use the more
      descriptive `userId` column name, rather than `ROWKEY`. Joining
      on either yields the same results. If your data doesn't
      contain a copy of the key in the value, you can join on `ROWKEY`.

Inspect the schema for the table.

```sql
DESCRIBE users_original;
```

```
Name                 : USERS_ORIGINAL
 Field        | Type
--------------------------------
 ROWTIME      | BIGINT
 ROWKEY       | VARCHAR(STRING)
 REGISTERTIME | BIGINT
 USERID       | VARCHAR(STRING)
 GENDER       | VARCHAR(STRING)
 REGIONID     | VARCHAR(STRING)
--------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
```

Show all streams and tables.

```sql
SHOW STREAMS;
SHOW TABLES;
```

Your output should resemble:

```
ksql> SHOW STREAMS;

 Stream Name        | Kafka Topic | Format
----------------------------------------------
 PAGEVIEWS_ORIGINAL | pageviews   | DELIMITED
----------------------------------------------
ksql> SHOW TABLES;

 Table Name     | Kafka Topic | Format    | Windowed
-----------------------------------------------------
 USERS_ORIGINAL | users       | DELIMITED | false
-----------------------------------------------------
```

Viewing your data
-----------------

### 1. Create a query that returns data from a table

Use `SELECT` to create a query that returns data from a table. This
query includes the `LIMIT` keyword to limit the number of rows
returned in the query result, and the `EMIT CHANGES` keywords to
indicate you that want to stream results back. This is known as a
[push query](../concepts/queries/push.md). See the
[queries](../concepts/queries/index.md) section for an explanation of the
different query types. Note that exact data output may vary because
of the randomness of the data generation.

```sql
SELECT * from users_original emit changes limit 5;
```

Your output should resemble:

```
+----------------+----------------+----------------+----------------+----------------+----------------+
|ROWTIME         |ROWKEY          |REGISTERTIME    |USERID          |GENDER          |REGIONID        |
+----------------+----------------+----------------+----------------+----------------+----------------+
|1591825087142   |User_6          |1497791942007   |User_6          |Region_1        |FEMALE          |
|1591825090142   |User_1          |1500394777538   |User_1          |Region_2        |FEMALE          |
|1591825092142   |User_8          |1512408593783   |User_8          |Region_2        |FEMALE          |
|1591825093141   |User_4          |1515780973022   |User_4          |Region_4        |OTHER           |
|1591825094142   |User_2          |1487969622095   |User_2          |Region_7        |OTHER           |
Limit Reached
Query terminated
```

!!! note
    Push queries on tables output the full history of the table that is stored
    in the {{ site.ak }} changelog topic, which means that it outputs historic
    data, followed by the stream of updates to the table. So it's likely that
    rows with matching `ROWKEY` are output as existing rows in the table are
    updated.

### 2. Create a query that returns data from a stream

View the data in your `pageviews_original` stream by issuing the following
push query:

```sql
SELECT viewtime, userid, pageid FROM pageviews_original emit changes LIMIT 3;
```

Your output should resemble:

```
+-----------------------------------+-----------------------------------+-----------------------------------+
|VIEWTIME                           |USERID                             |PAGEID                             |
+-----------------------------------+-----------------------------------+-----------------------------------+
|1591825136802                      |User_6                             |Page_57                            |
|1591825137003                      |User_7                             |Page_12                            |
|1591825137202                      |User_8                             |Page_72                            |
Limit Reached
Query terminated
```

!!! note
    By default, push queries on streams only output changes that occur
    after the query is started, which means that historic data isn't included.
    Run `set 'auto.offset.reset'='earliest';` to update your session
    properties if you want to see the historic data.

Write Queries
-------------

These examples write queries using ksqlDB.

### 1. Create a query that returns data from a ksqlDB stream

Create a query that enriches the pageviews data with the user's `gender`
and `regionid` from the `users` table. The following query enriches the
`pageviews_original` STREAM by doing a `LEFT JOIN` with the
`users_original` TABLE on the `userid` column.

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
+--------------------------+--------------------------+--------------------------+--------------------------+
|USERID                    |PAGEID                    |REGIONID                  |GENDER                    |
+--------------------------+--------------------------+--------------------------+--------------------------+
|User_7                    |Page_81                   |FEMALE                    |Region_2                  |
|User_9                    |Page_70                   |OTHER                     |Region_3                  |
|User_6                    |Page_87                   |FEMALE                    |Region_6                  |
|User_9                    |Page_82                   |OTHER                     |Region_3                  |
|User_4                    |Page_60                   |OTHER                     |Region_3                  |
Limit Reached
Query terminated
```

!!! note
  The join to the `users` table is on the `userid` column, which was identified as
  an alias for the table's primary key, `ROWKEY`, in the CREATE TABLE statement.
  `userId` and `ROWKEY` can be used interchangeably as the join criteria for
  the table. However, the data in `userid` on the stream side does not match
  the stream's key. Hence, ksqlDB internally repartitions the stream
  by the `userId` column before performing the join.

### 2. Create a persistent query

Create a persistent query by using the `CREATE STREAM` keywords to
precede the `SELECT` statement and removing the `LIMIT` clause.
The results from this query are written to the `PAGEVIEWS_ENRICHED` {{ site.ak }} topic.

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
-------------------------------------------------
 Created query with ID CSAS_PAGEVIEWS_ENRICHED_0
-------------------------------------------------
```

!!! tip
    You can run `DESCRIBE pageviews_enriched;` to describe the stream.

### 3. View query results

Use `SELECT` to view query results as they come in. To stop viewing
the query results, press Ctrl-C. This stops printing to the
console but it does not terminate the actual query. The query
continues to run in the underlying ksqlDB application.

```sql
SELECT * FROM pageviews_enriched emit changes LIMIT 5;
```

Your output should resemble:

```
+----------------+----------------+----------------+----------------+----------------+----------------+
|ROWTIME         |ROWKEY          |USERID          |PAGEID          |REGIONID        |GENDER          |
+----------------+----------------+----------------+----------------+----------------+----------------+
|1591825269603   |User_6          |User_6          |Page_83         |MALE            |Region_5        |
|1591825269802   |User_6          |User_6          |Page_88         |MALE            |Region_5        |
|1591825270002   |User_9          |User_9          |Page_14         |MALE            |Region_1        |
|1591825270202   |User_1          |User_1          |Page_71         |OTHER           |Region_1        |
|1591825270403   |User_7          |User_7          |Page_77         |MALE            |Region_8        |
Limit Reached
Query terminated
```

### 4. Create a filter query

Create a new persistent query where a condition limits the streams
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
-----------------------------------------------
 Created query with ID CSAS_PAGEVIEWS_FEMALE_5
-----------------------------------------------
```

!!! tip
    You can run `DESCRIBE pageviews_female;` to describe the stream.

### 5. Create a LIKE query

Create a new persistent query where another condition is met, using
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
-------------------------------------------------------
 Created query with ID CSAS_PAGEVIEWS_FEMALE_LIKE_89_7
-------------------------------------------------------
```

### 6. Count and group pageview events

Create a new persistent query that counts the pageviews for each
region and gender combination in a
[tumbling window](https://docs.confluent.io/current/streams/developer-guide/dsl-api.html#tumbling-time-windows)
of 30 seconds when the count is greater than one. Results from this
query are written to the `PAGEVIEWS_REGIONS` Kafka topic in the Avro
format. ksqlDB registers the Avro schema with the configured 
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
------------------------------------------------
 Created query with ID CTAS_PAGEVIEWS_REGIONS_9
------------------------------------------------
```

!!! tip
    You can run `DESCRIBE pageviews_regions;` to describe the table.

### 7. View query results using a push query

View results from the previous queries by using the `SELECT` statement.

```sql
SELECT * FROM pageviews_regions EMIT CHANGES LIMIT 5;
```

Your output should resemble:

```
+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|ROWTIME       |ROWKEY        |WINDOWSTART   |WINDOWEND     |GENDER        |REGIONID      |NUMUSERS      |
+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|1591825365002 |Region_6|+|FEM|1591825350000 |1591825380000 |Region_6      |FEMALE        |3             |
|              |ALE           |              |              |              |              |              |
|1591825366203 |Region_1|+|OTH|1591825350000 |1591825380000 |Region_1      |OTHER         |1             |
|              |ER            |              |              |              |              |              |
|1591825368003 |Region_6|+|MAL|1591825350000 |1591825380000 |Region_6      |MALE          |5             |
|              |E             |              |              |              |              |              |
|1591825368802 |Region_7|+|OTH|1591825350000 |1591825380000 |Region_7      |OTHER         |1             |
|              |ER            |              |              |              |              |              |
|1591825374003 |Region_1|+|MAL|1591825350000 |1591825380000 |Region_1      |MALE          |8             |
|              |E             |              |              |              |              |              |
Limit Reached
Query terminated
```

!!! note
   Notice the addition of the WINDOWSTART and WINDOWEND columns.
   These are available because `pageviews_regions` is aggregating data
   per 30 second _window_. ksqlDB automatically adds these system columns
   for windowed results.

!!! note
   The grouping columns `gender` and `regionid` are currently stored combined
   in a single key column. This will be rectified once multiple key columns
   are supported. Until then, these columns are combined and the resulting column
   is given a system-generated column name.

### 8. View query results using a pull query

When a CREATE TABLE statement contains a GROUP BY clause, ksqlDB builds an internal
table containing the results of the aggregation. ksqlDB supports pull queries against
such aggregation results.

Unlike the push query used in the previous step, which _pushes_ a stream of results to you,
pull queries pull a result set and automatically terminate.

Pull queries do not have the `EMIT CHANGES` clause.

View all of the windows and user counts that are available for a specific gender and region by using a pull query:

```sql
SELECT * FROM pageviews_regions WHERE ROWKEY='Region_1|+|OTHER';
```

Your output should resemble:

```
+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|ROWKEY        |WINDOWSTART   |WINDOWEND     |ROWTIME       |GENDER        |REGIONID      |NUMUSERS      |
+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
|Region_1|+|OTH|1591825350000 |1591825380000 |1591825366203 |Region_1      |OTHER         |1             |
|ER            |              |              |              |              |              |              |
|Region_1|+|OTH|1591825380000 |1591825410000 |1591825409602 |Region_1      |OTHER         |20            |
|ER            |              |              |              |              |              |              |
|Region_1|+|OTH|1591825410000 |1591825440000 |1591825413002 |Region_1      |OTHER         |3             |
|ER            |              |              |              |              |              |              |
|Region_1|+|OTH|1591825440000 |1591825470000 |1591825462002 |Region_1      |OTHER         |2             |
|ER            |              |              |              |              |              |              |
Query terminated
```

Pull queries on windowed tables such as `pageviews_regions` also support querying a single window's result:

```sql
SELECT NUMUSERS FROM pageviews_regions WHERE ROWKEY='Region_1|+|OTHER' AND WINDOWSTART=1591825350000;
```

!!! important
   You must change the value of `WINDOWSTART` in the above SQL to match one of the window boundaries in your data.
   Otherwise no results will be returned.

Your output should resemble:

```
+--------------------------------------------------------------------------------------------------------------+
|NUMUSERS                                                                                                      |
+--------------------------------------------------------------------------------------------------------------+
|1                                                                                                             |
Query terminated
```

Or querying a range of windows:

```sql
SELECT WINDOWSTART, WINDOWEND, NUMUSERS FROM pageviews_regions WHERE ROWKEY='Region_1|+|OTHER' AND 1591825350000 <= WINDOWSTART AND WINDOWSTART <= 1591825440000;
```

!!! important
   You must change the value of `WINDOWSTART` in the above SQL to match one of the window boundaries in your data.
   Otherwise no results will be returned.

Your output should resemble:

```
+-----------------------------------+-----------------------------------+-----------------------------------+
|WINDOWSTART                        |WINDOWEND                          |NUMUSERS                           |
+-----------------------------------+-----------------------------------+-----------------------------------+
|1591825350000                      |1591825380000                      |1                                  |
|1591825380000                      |1591825410000                      |20                                 |
|1591825410000                      |1591825440000                      |3                                  |
|1591825440000                      |1591825470000                      |2                                  |
Query terminated
```

### 9. View persistent queries

Show the running persistent queries:

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

### 10. Examine query run-time metrics and details

Observe that information including the target {{ site.ak }} topic is
available, as well as throughput figures for the messages being processed.

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
 ROWKEY   | VARCHAR(STRING)  (key) (Window type: TUMBLING)
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

Use Nested Schemas (STRUCT) in ksqlDB
-------------------------------------

Struct support enables the modeling and access of nested data in {{ site.ak }}
topics, from JSON, Avro, and Protobuf.

### 1. Generate data

In this section, you use the `ksql-datagen` tool to create some sample data
that includes a nested `address` field. Run this in a new window, and leave
it running.

```bash
docker run --network tutorials_default --rm  \
  confluentinc/ksqldb-examples:{{ site.cprelease }} \
  ksql-datagen \
      quickstart=orders \
      format=json \
      topic=orders \
      msgRate=5 \
      bootstrap-server=kafka:39092
```

### 2. Register a stream

From the ksqlDB command prompt, register the a stream on the `orders` topic:

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

### 3. Observe the stream's schema

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
 ROWTIME    | BIGINT
 ROWKEY     | INTEGER
 ORDERTIME  | BIGINT
 ORDERID    | INTEGER
 ITEMID     | VARCHAR(STRING)
 ORDERUNITS | DOUBLE
 ADDRESS    | STRUCT<CITY VARCHAR(STRING), STATE VARCHAR(STRING), ZIPCODE BIGINT>
----------------------------------------------------------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
```

### 4. Access the struct data

Query the data by using the `->` notation to access the struct contents:

```sql
SELECT ORDERID, ADDRESS->CITY FROM ORDERS EMIT CHANGES LIMIT 5;
```

Your output should resemble:

```
+------------------------------------------------------+------------------------------------------------------+
|ORDERID                                               |ADDRESS__CITY                                         |
+------------------------------------------------------+------------------------------------------------------+
|350                                                   |City_11                                               |
|351                                                   |City_29                                               |
|352                                                   |City_26                                               |
|353                                                   |City_25                                               |
|354                                                   |City_22                                               |
^CQuery terminated
```

Stream-Stream join
------------------

Using a stream-stream join, you can join two event streams on a
common key. An example of this could be a stream of order events and
a stream of shipment events. By joining these on the order key, you can
see shipment information alongside the order.

### 1. Create two streams

In the ksqlDB CLI create two new streams. Both streams will store their
order ID in ROWKEY:

```sql
CREATE STREAM NEW_ORDERS (ROWKEY INT KEY, TOTAL_AMOUNT DOUBLE, CUSTOMER_NAME VARCHAR)
WITH (KAFKA_TOPIC='new_orders', VALUE_FORMAT='JSON', PARTITIONS=2);

CREATE STREAM SHIPMENTS (ROWKEY INT KEY, SHIPMENT_ID INT, WAREHOUSE VARCHAR)
WITH (KAFKA_TOPIC='shipments', VALUE_FORMAT='JSON', PARTITIONS=2);
```

!!! note
  ksqlDB creates the underlying topics in {{ site.ak }} when it executes these statements.
  You can also specify the `REPLICAS` count.

After both `CREATE STREAM` statements, your output should resemble:

```
 Message
----------------
 Stream created
----------------
```

### 2. Populate two source topics

Populate the streams with some sample data by using the INSERT VALUES statement:

```sql
-- Insert values in NEW_ORDERS:
-- insert supplying the list of columns to insert:
INSERT INTO NEW_ORDERS (ROWKEY, CUSTOMER_NAME, TOTAL_AMOUNT) 
  VALUES (1, 'Bob Smith', 10.50);
  
-- shorthand version can be used when inserting values for all columns, (except ROWTIME), in column order:
INSERT INTO NEW_ORDERS  VALUES (2, 3.32, 'Sarah Black');
INSERT INTO NEW_ORDERS  VALUES (3, 21.00, 'Emma Turner');

-- Insert values in SHIPMENTS:
INSERT INTO SHIPMENTS VALUES (1, 42, 'Nashville');
INSERT INTO SHIPMENTS VALUES (3, 43, 'Palo Alto');
```

### 3. Set the auto.offset.reset property

Run the following statement to tell ksqlDB to read from the beginning of all
streams:

```sql
SET 'auto.offset.reset' = 'earliest';
```

!!! tip
    You can skip this step if you've already run it within your current
    ksqlDB CLI session.

### 4. Examine streams for events 

Query the streams to confirm that events are present in the topics.

For the `NEW_ORDERS` stream, run:

```sql
SELECT * FROM NEW_ORDERS EMIT CHANGES LIMIT 3;
```

Your output should resemble:

```
+--------------------------+--------------------------+--------------------------+--------------------------+
|ROWTIME                   |ROWKEY                    |TOTAL_AMOUNT              |CUSTOMER_NAME             |
+--------------------------+--------------------------+--------------------------+--------------------------+
|1591825956028             |2                         |3.32                      |Sarah Black               |
|1591825956188             |3                         |21.0                      |Emma Turner               |
|1591825955781             |1                         |10.5                      |Bob Smith                 |
Limit Reached
Query terminated
```

For the `SHIPMENTS` stream, run:

```sql
SELECT * FROM SHIPMENTS EMIT CHANGES LIMIT 2;
```

Your output should resemble:

```
+--------------------------+--------------------------+--------------------------+--------------------------+
|ROWTIME                   |ROWKEY                    |SHIPMENT_ID               |WAREHOUSE                 |
+--------------------------+--------------------------+--------------------------+--------------------------+
|1591825956368             |1                         |42                        |Nashville                 |
|1591825958498             |3                         |43                        |Palo Alto                 |
Limit Reached
Query terminated
```

### 5. Join the streams

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
+--------------------+--------------------+--------------------+--------------------+--------------------+
|ORDER_ID            |TOTAL_AMOUNT        |CUSTOMER_NAME       |SHIPMENT_ID         |WAREHOUSE           |
+--------------------+--------------------+--------------------+--------------------+--------------------+
|3                   |21.0                |Emma Turner         |43                  |Palo Alto           |
|1                   |10.5                |Bob Smith           |42                  |Nashville           |
```

Messages with `ORDER_ID=2` have no corresponding `SHIPMENT_ID` or
`WAREHOUSE`. This is because there's no corresponding row on the
`SHIPMENTS` stream within the time window specified.

Start the ksqlDB CLI in a second window by running:

```bash
docker run --network tutorials_default --rm --interactive --tty \
    confluentinc/ksqldb-cli:{{ site.release }} ksql \
    http://ksql-server:8088
```

Enter the following INSERT VALUES statement to insert the shipment for
order id 2:

```sql
INSERT INTO SHIPMENTS VALUES (2, 49, 'London');
```

Switching back to your primary ksqlDB CLI window, notice that a third
row has now been output:

```
+--------------------+--------------------+--------------------+--------------------+--------------------+
|ORDER_ID            |TOTAL_AMOUNT        |CUSTOMER_NAME       |SHIPMENT_ID         |WAREHOUSE           |
+--------------------+--------------------+--------------------+--------------------+--------------------+
|1                   |10.5                |Bob Smith           |42                  |Nashville           |
|3                   |21.0                |Emma Turner         |43                  |Palo Alto           |
|2                   |3.32                |Sarah Black         |49                  |London              |
```

Press Ctrl+C to cancel the `SELECT` query and return to the prompt.

Table-Table join
----------------

Using a table-table join, it's possible to join two tables of on a
common key. ksqlDB tables provide the latest *value* for a given *key*.
They can only be joined on the *key*, and one-to-many (1:N) joins are
not supported in the current semantic model.

In this example, location data about a warehouse from one system is
enriched with data about the size of the warehouse from another.

### 1. Register two tables

In the KSQL CLI, register both topics as KSQL tables. Note, in this example
the warehouse id is stored both in the key and in the WAREHOUSE_ID field
in the value:

```sql
CREATE TABLE WAREHOUSE_LOCATION 
   (ROWKEY INTEGER KEY, WAREHOUSE_ID INT, CITY VARCHAR, COUNTRY VARCHAR)
   WITH (KAFKA_TOPIC='warehouse_location',
      VALUE_FORMAT='JSON',
      KEY='WAREHOUSE_ID',
      PARTITIONS=2);

CREATE TABLE WAREHOUSE_SIZE 
   (ROWKEY INTEGER KEY, WAREHOUSE_ID INT, SQUARE_FOOTAGE DOUBLE)
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

### 2. Populate two source topics

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

### 3. Examine tables for keys

Check both tables that the message key (`ROWKEY`) matches the declared
key (`WAREHOUSE_ID`). The output should show that they are equal. If
they were not, the join won't succeed or behave as expected.

Inspect the WAREHOUSE_LOCATION table:

```sql
SELECT ROWKEY, WAREHOUSE_ID FROM WAREHOUSE_LOCATION EMIT CHANGES LIMIT 3;
```

Your output should resemble:

```
+------------------------------------------------------+------------------------------------------------------+
|ROWKEY                                                |WAREHOUSE_ID                                          |
+------------------------------------------------------+------------------------------------------------------+
|2                                                     |2                                                     |
|3                                                     |3                                                     |
|1                                                     |1                                                     |
Limit Reached
Query terminated
```

Inspect the WAREHOUSE_SIZE table:

```sql
SELECT ROWKEY, WAREHOUSE_ID FROM WAREHOUSE_SIZE EMIT CHANGES LIMIT 3;
```

Your output should resemble:

```
+------------------------------------------------------+------------------------------------------------------+
|ROWKEY                                                |WAREHOUSE_ID                                          |
+------------------------------------------------------+------------------------------------------------------+
|1                                                     |1                                                     |
|2                                                     |2                                                     |
|3                                                     |3                                                     |
Limit Reached
Query terminated
```

### 4. Join the tables

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
+--------------------------+--------------------------+--------------------------+--------------------------+
|WL_WAREHOUSE_ID           |CITY                      |COUNTRY                   |SQUARE_FOOTAGE            |
+--------------------------+--------------------------+--------------------------+--------------------------+
|2                         |Sheffield                 |UK                        |42000.0                   |
|3                         |Berlin                    |Germany                   |94000.0                   |
|2                         |Sheffield                 |UK                        |42000.0                   |
Limit Reached
Query terminated
```

INSERT INTO
-----------

The `INSERT INTO` syntax can be used to merge the contents of multiple
streams. An example of this could be where the same event type is coming
from different sources.

### 1. Generate data

Run two datagen processes, each writing to a different topic, simulating
order data arriving from a local installation vs from a third-party:

```bash
docker run --network tutorials_default --rm  --name datagen-orders-local \
  confluentinc/ksqldb-examples:{{ site.cprelease }} \
  ksql-datagen \
      quickstart=orders \
      format=json \
      topic=orders_local \
      msgRate=2 \
      bootstrap-server=kafka:39092
```

```bash
docker run --network tutorials_default --rm --name datagen-orders_3rdparty \
  confluentinc/ksqldb-examples:{{ site.cprelease }} \
  ksql-datagen \
      quickstart=orders \
      format=json \
      topic=orders_3rdparty \
      msgRate=2 \
      bootstrap-server=kafka:39092
```

### 2. Register streams

In ksqlDB, register the source topic for each:

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

### 3. Create a persistent query

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
------------------------------------------
 Created query with ID CSAS_ALL_ORDERS_11
------------------------------------------
```

### 4. Examine the stream's schema

Use the `DESCRIBE` command to observe the schema of the target stream.

```sql
DESCRIBE ALL_ORDERS;
```

Your output should resemble:

```
Name                 : ALL_ORDERS
 Field      | Type                                                                
----------------------------------------------------------------------------------
 ROWKEY     | INTEGER          (key)
 SRC        | VARCHAR(STRING)                                                     
 ORDERTIME  | BIGINT                                                              
 ORDERID    | INTEGER                                                             
 ITEMID     | VARCHAR(STRING)                                                     
 ORDERUNITS | DOUBLE                                                              
 ADDRESS    | STRUCT<CITY VARCHAR(STRING), STATE VARCHAR(STRING), ZIPCODE BIGINT> 
----------------------------------------------------------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
```

### 5. Insert another stream 

Add a stream of 3rd-party orders into the existing output stream:

```sql
INSERT INTO ALL_ORDERS SELECT '3RD PARTY' AS SRC, * FROM ORDERS_SRC_3RDPARTY EMIT CHANGES;
```

Your output should resemble:

```
 Message
--------------------------------------
 Created query with ID InsertQuery_27
--------------------------------------
```

### 6. Query the output stream

Query the output stream to verify that data from each source is being
written to it:

```sql
SELECT * FROM ALL_ORDERS EMIT CHANGES LIMIT 5;
```

Your output should resemble:

```
+------------+------------+------------+------------+------------+------------+------------+------------+
|ROWTIME     |ROWKEY      |SRC         |ORDERTIME   |ORDERID     |ITEMID      |ORDERUNITS  |ADDRESS     |
+------------+------------+------------+------------+------------+------------+------------+------------+
|159182661634|0           |LOCAL       |151718678870|0           |Item_605    |6.2818393891|{CITY=City_9|
|6           |            |            |5           |            |            |66553       |9, STATE=Sta|
|            |            |            |            |            |            |            |te_61, ZIPCO|
|            |            |            |            |            |            |            |DE=95427}   |
|159182661637|1           |LOCAL       |150403178993|1           |Item_291    |1.9701245349|{CITY=City_1|
|1           |            |            |2           |            |            |00344       |3, STATE=Sta|
|            |            |            |            |            |            |            |te_42, ZIPCO|
|            |            |            |            |            |            |            |DE=88155}   |
|159182661659|2           |LOCAL       |150087908278|2           |Item_398    |4.3000220453|{CITY=City_7|
|4           |            |            |4           |            |            |45575       |7, STATE=Sta|
|            |            |            |            |            |            |            |te_56, ZIPCO|
|            |            |            |            |            |            |            |DE=89098}   |
|159182661709|3           |LOCAL       |150801615765|3           |Item_768    |8.5027061574|{CITY=City_3|
|4           |            |            |1           |            |            |8345        |7, STATE=Sta|
|            |            |            |            |            |            |            |te_87, ZIPCO|
|            |            |            |            |            |            |            |DE=30173}   |
|159182661759|4           |LOCAL       |149567554961|4           |Item_814    |2.1522597994|{CITY=City_5|
|3           |            |            |7           |            |            |61952       |1, STATE=Sta|
|            |            |            |            |            |            |            |te_25, ZIPCO|
|            |            |            |            |            |            |            |DE=63275}   |
Limit Reached
Query terminated
```

Events from both source topics are present, denoted by `LOCAL` and `3RD PARTY` respectively.

### 7. View the queries

You can view the two queries that are running using `SHOW QUERIES`:

```sql
SHOW QUERIES;
```

Your output should resemble:

```
 Query ID                         | Status  | Sink Name                | Sink Kafka Topic         | Query String                                                                                                                                                                                                                                                                                                                                                                                                 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 INSERTQUERY_43                   | RUNNING | ALL_ORDERS               | ALL_ORDERS               | INSERT INTO ALL_ORDERS SELECT '3RD PARTY' AS SRC, * FROM ORDERS_SRC_3RDPARTY EMIT CHANGES;
 CSAS_ALL_ORDERS_17               | RUNNING | ALL_ORDERS               | ALL_ORDERS               | CREATE STREAM ALL_ORDERS WITH (KAFKA_TOPIC='ALL_ORDERS', PARTITIONS=1, REPLICAS=1) AS SELECT  'LOCAL' SRC,  *FROM ORDERS_SRC_LOCAL ORDERS_SRC_LOCALEMIT CHANGES;                                                                                                                                                                                                                                             
...
```

Terminate and Exit
------------------

### ksqlDB

!!! important
	Persisted queries run continuously as ksqlDB applications until they're
    terminated manually. Exiting the ksqlDB CLI doesn't terminate persistent
    queries.

From the output of `SHOW QUERIES;` identify a query ID you would
like to terminate. For example, if you wish to terminate query ID
`CTAS_PAGEVIEWS_REGIONS_15`:

```sql
TERMINATE CTAS_PAGEVIEWS_REGIONS_15;
```

!!! tip
    The actual name of the query running may vary; refer to the output
    of `SHOW QUERIES;`.

Run the `exit` command to leave the ksqlDB CLI.

```
ksql> exit
Exiting ksqlDB.
```

### Docker

To stop all data generator containers, run the following:

```bash
docker ps|grep ksql-datagen|awk '{print $1}'|xargs -Ifoo docker stop foo
```

To stop all Docker containers, run the following:

```bash
docker container stop $(docker container list -q)
```

If you're running {{ site.ak }} using Docker Compose, you can stop it
and remove the containers and their data with this command.

```bash
docker-compose down
```

Produce extra topic data and verify your environment
----------------------------------------------------

The following instructions aren't required to run the quick start. They're
optional steps to produce extra topic data and verify the environment.

### Produce more topic data

The Compose file automatically runs a data generator that continuously
produces data to two {{ site.ak }} topics, `pageviews` and `users`. No further
action is required if you want to use just the data available. You can
return to the [main ksqlDB quick start](#create-a-stream-and-table) to start
querying the data in these two topics.

However, if you want to produce additional data, you can use the
following methods.

Produce {{ site.ak }} data with the {{ site.ak }} command line
`kafka-console-producer`. The following example generates data with
a value in DELIMITED format.

```bash
docker-compose exec kafka kafka-console-producer \
                            --topic t1 \
                            --broker-list kafka:39092  \
                            --property parse.key=true \
                            --property key.separator=:
```

Your data input should resemble:

```
key1:v1,v2,v3
key2:v4,v5,v6
key3:v7,v8,v9
key1:v10,v11,v12
```

Produce {{ site.ak }} data with the {{ site.ak }} command line
`kafka-console-producer`. The following example generates data with
a value in JSON format.

```bash
docker-compose exec kafka kafka-console-producer \
                            --topic t2 \
                            --broker-list kafka:39092  \
                            --property parse.key=true \
                            --property key.separator=:
```

Your data input should resemble:

```
key1:{"id":"key1","col1":"v1","col2":"v2","col3":"v3"}
key2:{"id":"key2","col1":"v4","col2":"v5","col3":"v6"}
key3:{"id":"key3","col1":"v7","col2":"v8","col3":"v9"}
key1:{"id":"key1","col1":"v10","col2":"v11","col3":"v12"}
```

You can also use the `kafkacat` command line tool:

```bash
docker run --interactive --rm --network tutorials_default \
  confluentinc/cp-kafkacat \
  kafkacat -b kafka:39092 \
          -t warehouse_size \
          -K: \
          -P <<EOF
1:{"warehouse_id":1,"square_footage":16000}
2:{"warehouse_id":2,"square_footage":42000}
3:{"warehouse_id":3,"square_footage":94000}
EOF
```

You can also use `INSERT VALUES` SQL statements, as demonstrated in the previous
examples.

### Verify your environment

The following steps are optional verification steps to ensure your
environment is properly setup.

Verify that six Docker containers were created.

```bash
docker-compose ps
```

Your output should resemble:

```
        Name                        Command            State                 Ports
----------------------------------------------------------------------------------------------------
tutorials_kafka_1             /etc/confluent/docker/run   Up      0.0.0.0:39092->39092/tcp, 9092/tcp
tutorials_ksql-server_1       /etc/confluent/docker/run   Up      8088/tcp
tutorials_schema-registry_1   /etc/confluent/docker/run   Up      8081/tcp
tutorials_zookeeper_1         /etc/confluent/docker/run   Up      2181/tcp, 2888/tcp, 3888/tcp
```

Take note of the `Up` state.

Earlier steps in this quickstart started two data generators that
pre-populate two topics, named `pageviews` and `users`, with mock
data. Verify that the data generator created these topics, including
`pageviews` and `users`.

```bash
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Your output should resemble this.

```
_confluent-metrics
_schemas
pageviews
users
```

Use the `kafka-console-consumer` to view a few messages from each
topic. The topic `pageviews` has a key that is a mock time stamp and
a value that is in `DELIMITED` format. The topic `users` has a key
that is the user ID and a value that is in `Json` format.

```bash
docker-compose exec kafka kafka-console-consumer \
                            --topic pageviews \
                            --bootstrap-server kafka:39092 \
                            --from-beginning \
                            --max-messages 3 \
                            --property print.key=true
```

Your output should resemble:

```
1491040409254    1491040409254,User_5,Page_70
1488611895904    1488611895904,User_8,Page_76
1504052725192    1504052725192,User_8,Page_92
```

```bash
docker-compose exec kafka kafka-console-consumer \
                            --topic users \
                            --bootstrap-server kafka:39092 \
                            --from-beginning \
                            --max-messages 3 \
                            --property print.key=true
```

Your output should resemble:

```
User_2   {"registertime":1509789307038,"gender":"FEMALE","regionid":"Region_1","userid":"User_2"}
User_6   {"registertime":1498248577697,"gender":"OTHER","regionid":"Region_8","userid":"User_6"}
User_8   {"registertime":1494834474504,"gender":"MALE","regionid":"Region_5","userid":"User_8"}
```

