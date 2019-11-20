---
layout: page
title: Write Streaming Queries Against Apache Kafka® Using KSQL and Confluent Control Center (Local)
tagline: Use KSQL for event streaming applications
description: Learn how to use KSQL to create event streaming applications on Kafka topics
keywords: ksql
---

Write Streaming Queries Against {{ site.aktm }} Using KSQL (Local)
===================================================================

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

CLI v{{ site.release }}, Server v{{ site.release }} located at http://ksqldb-server:8088

Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

ksql>
```

Inspect Kafka Topics By Using SHOW and PRINT Statements
=======================================================

KSQL enables inspecting Kafka topics and messages in real time.

-   Use the SHOW TOPICS statement to list the available topics in the
    Kafka cluster.
-   Use the PRINT statement to see a topic\'s messages as they arrive.

In the KSQL CLI, run the following statement:

```sql
SHOW TOPICS;
```

Your output should resemble:

```
 Kafka Topic        | Partitions | Partition Replicas
------------------------------------------------------
 _confluent-metrics | 12         | 1
 _schemas           | 1          | 1
 pageviews          | 1          | 1
 users              | 1          | 1
------------------------------------------------------
```

Inspect the `users` topic by using the PRINT statement:

```sql
PRINT 'users';
```

Your output should resemble:

```json
Format:JSON
{"ROWTIME":1540254230041,"ROWKEY":"User_1","registertime":1516754966866,"userid":"User_1","regionid":"Region_9","gender":"MALE"}
{"ROWTIME":1540254230081,"ROWKEY":"User_3","registertime":1491558386780,"userid":"User_3","regionid":"Region_2","gender":"MALE"}
{"ROWTIME":1540254230091,"ROWKEY":"User_7","registertime":1514374073235,"userid":"User_7","regionid":"Region_2","gender":"OTHER"}
^C{"ROWTIME":1540254232442,"ROWKEY":"User_4","registertime":1510034151376,"userid":"User_4","regionid":"Region_8","gender":"FEMALE"}
Topic printing ceased
```

Press Ctrl+C to stop printing messages.

Inspect the `pageviews` topic by using the PRINT statement:

```sql
PRINT 'pageviews';
```

Your output should resemble:

```
Format:STRING
10/23/18 12:24:03 AM UTC , 9461 , 1540254243183,User_9,Page_20
10/23/18 12:24:03 AM UTC , 9471 , 1540254243617,User_7,Page_47
10/23/18 12:24:03 AM UTC , 9481 , 1540254243888,User_4,Page_27
^C10/23/18 12:24:05 AM UTC , 9521 , 1540254245161,User_9,Page_62
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
    CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH
    (kafka_topic='pageviews', value_format='DELIMITED');
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
          stream. Notice that KSQL created two additional columns, named
          `ROWTIME`, which corresponds with the Kafka message timestamp, and
          `ROWKEY`, which corresponds with the Kafka message key.

2.  Create a table, named `users_original`, from the `users` Kafka
    topic, specifying the `value_format` of `JSON`.

    ```sql
    CREATE TABLE users_original (registertime BIGINT, gender VARCHAR, regionid VARCHAR, userid VARCHAR) WITH
    (kafka_topic='users', value_format='JSON', key = 'userid');
    ```

    Your output should resemble:

    ```
     Message
    ---------------
     Table created
    ---------------
    ```

    !!! tip
          You can run `DESCRIBE users_original;` to see the schema for the
          Table.

3.  Optional: Show all streams and tables.
    ```
    ksql> SHOW STREAMS;

    Stream Name              | Kafka Topic              | Format
    -----------------------------------------------------------------
    PAGEVIEWS_ORIGINAL       | pageviews                | DELIMITED

    ksql> SHOW TABLES;

    Table Name        | Kafka Topic       | Format    | Windowed
    --------------------------------------------------------------
    USERS_ORIGINAL    | users             | JSON      | false
    ```

Write Queries
=============

These examples write queries using KSQL.

!!! note
      By default, KSQL reads the topics for streams and tables from
      the latest offset.

1.  Use `SELECT` to create a query that returns data from a STREAM. This
    query includes the `LIMIT` keyword to limit the number of rows
    returned in the query result. Note that exact data output may vary
    because of the randomness of the data generation.

    ```sql
    SELECT pageid FROM pageviews_original LIMIT 3;
    ```

    Your output should resemble:

    ```
    Page_24
    Page_73
    Page_78
    LIMIT reached
    Query terminated
    ```

2.  Create a persistent query by using the `CREATE STREAM` keywords to
    precede the `SELECT` statement. The results from this query are
    written to the `PAGEVIEWS_ENRICHED` Kafka topic. The following query
    enriches the `pageviews_original` STREAM by doing a `LEFT JOIN` with
    the `users_original` TABLE on the user ID.

    ```sql
    CREATE STREAM pageviews_enriched AS
      SELECT users_original.userid AS userid, pageid, regionid, gender
      FROM pageviews_original
      LEFT JOIN users_original
        ON pageviews_original.userid = users_original.userid;
    ```

    Your output should resemble:

    ```
     Message
    ----------------------------
     Stream created and running
    ----------------------------
    ```

    !!! tip
		    You can run `DESCRIBE pageviews_enriched;` to describe the stream.

3.  Use `SELECT` to view query results as they come in. To stop viewing
    the query results, press Ctrl-C. This stops printing to the
    console but it does not terminate the actual query. The query
    continues to run in the underlying KSQL application.

    ```sql
    SELECT * FROM pageviews_enriched;
    ```

    Your output should resemble:

    ```
    1519746861328 | User_4 | User_4 | Page_58 | Region_5 | OTHER
    1519746861794 | User_9 | User_9 | Page_94 | Region_9 | MALE
    1519746862164 | User_1 | User_1 | Page_90 | Region_7 | FEMALE
    ^CQuery terminated
    ```

4.  Create a new persistent query where a condition limits the streams
    content, using `WHERE`. Results from this query are written to a
    Kafka topic called `PAGEVIEWS_FEMALE`.

    ```sql
    CREATE STREAM pageviews_female AS
      SELECT * FROM pageviews_enriched
      WHERE gender = 'FEMALE';
    ```

    Your output should resemble:

    ```
     Message
    ----------------------------
     Stream created and running
    ----------------------------
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
    WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';
    ```

    Your output should resemble:

    ```
     Message
    ----------------------------
     Stream created and running
    ----------------------------
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
    HAVING COUNT(*) > 1;
    ```

    Your output should resemble:

    ```
     Message
    ---------------------------
     Table created and running
    ---------------------------
    ```

    !!! tip
		    You can run `DESCRIBE pageviews_regions;` to describe the table.

7.  Optional: View results from the above queries using `SELECT`.

    ```sql
    SELECT gender, regionid, numusers FROM pageviews_regions LIMIT 5;
    ```

    Your output should resemble:

    ```
    FEMALE | Region_6 | 3
    FEMALE | Region_1 | 4
    FEMALE | Region_9 | 6
    MALE | Region_8 | 2
    OTHER | Region_5 | 4
    LIMIT reached
    Query terminated
    ksql>
    ```

8.  Optional: Show all persistent queries.

    ```sql
    SHOW QUERIES;
    ```

    Your output should resemble:

    ```
    Query ID                        | Kafka Topic              | Query String
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    CSAS_PAGEVIEWS_FEMALE_1         | PAGEVIEWS_FEMALE         | CREATE STREAM pageviews_female AS       SELECT * FROM pageviews_enriched       WHERE gender = 'FEMALE';
    CTAS_PAGEVIEWS_REGIONS_3        | PAGEVIEWS_REGIONS        | CREATE TABLE pageviews_regions         WITH (VALUE_FORMAT='avro') AS       SELECT gender, regionid , COUNT(*) AS numusers       FROM pageviews_enriched         WINDOW TUMBLING (size 30 second)       GROUP BY gender, regionid       HAVING COUNT(*) > 1;
    CSAS_PAGEVIEWS_FEMALE_LIKE_89_2 | PAGEVIEWS_FEMALE_LIKE_89 | CREATE STREAM pageviews_female_like_89         WITH (kafka_topic='pageviews_enriched_r8_r9') AS       SELECT * FROM pageviews_female       WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';
    CSAS_PAGEVIEWS_ENRICHED_0       | PAGEVIEWS_ENRICHED       | CREATE STREAM pageviews_enriched AS       SELECT users_original.userid AS userid, pageid, regionid, gender       FROM pageviews_original       LEFT JOIN users_original         ON pageviews_original.userid = users_original.userid;
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    For detailed information on a Query run: EXPLAIN <Query ID>;
    ```

9.  Optional: Examine query run-time metrics and details. Observe that
    information including the target Kafka topic is available, as well
    as throughput figures for the messages being processed.

    ```sql
    DESCRIBE EXTENDED PAGEVIEWS_REGIONS;
    ```

    Your output should resemble:

    ```
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
     format=avro \
     topic=orders 
```

From the KSQL command prompt, register the topic in KSQL:

```sql
CREATE STREAM ORDERS WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='AVRO');
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
 ROWKEY     | VARCHAR(STRING)  (system)
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
SELECT ORDERID, ADDRESS->CITY FROM ORDERS;
```

Your output should resemble:

```
0 | City_35
1 | City_21
2 | City_47
3 | City_57
4 | City_17
```

Press Ctrl+C to cancel the `SELECT` query.

Stream-Stream join
------------------

Using a stream-stream join, it is possible to join two *event streams*
on a common key. An example of this could be a stream of order events and
a stream of shipment events. By joining these on the order key, you can
see shipment information alongside the order.

In a separate console window, populate the `orders` and `shipments`
topics by using the `kafkacat` utility:

```bash
<path-to-confluent>/bin/kafka-console-producer \
  --broker-list localhost:9092 \
  --topic new_orders \
  --property "parse.key=true" \
  --property "key.separator=:"<<EOF
1:{"order_id":1,"total_amount":10.50,"customer_name":"Bob Smith"}
2:{"order_id":2,"total_amount":3.32,"customer_name":"Sarah Black"}
3:{"order_id":3,"total_amount":21.00,"customer_name":"Emma Turner"}
EOF

<path-to-confluent>/bin/kafka-console-producer \
  --broker-list localhost:9092 \
  --topic shipments \
  --property "parse.key=true" \
  --property "key.separator=:"<<EOF
1:{"order_id":1,"shipment_id":42,"warehouse":"Nashville"}
3:{"order_id":3,"shipment_id":43,"warehouse":"Palo Alto"}
EOF
```

!!! note
      You may see the following warning message when running the
      above statements, but it can be safely ignored:
      ```
      Error while fetching metadata with correlation id 1 : {new_orders=LEADER_NOT_AVAILABLE} (org.apache.kafka.clients.NetworkClient)
      Error while fetching metadata with correlation id 1 : {shipments=LEADER_NOT_AVAILABLE} (org.apache.kafka.clients.NetworkClient)
      ```

In the KSQL CLI, register both topics as KSQL streams:

```sql
CREATE STREAM NEW_ORDERS (ORDER_ID INT, TOTAL_AMOUNT DOUBLE, CUSTOMER_NAME VARCHAR)
WITH (KAFKA_TOPIC='new_orders', VALUE_FORMAT='JSON');

CREATE STREAM SHIPMENTS (ORDER_ID INT, SHIPMENT_ID INT, WAREHOUSE VARCHAR)
WITH (KAFKA_TOPIC='shipments', VALUE_FORMAT='JSON');
```

After both `CREATE STREAM` statements, your output should resemble:

```
 Message
----------------
 Stream created
----------------
```

Query the data to confirm that it's present in the topics.

!!! tip
      Run the following to tell KSQL to read from the beginning of the topic: 
      ```sql
      SET 'auto.offset.reset' = 'earliest';
      ```
      You can skip this if you have already run it within your current
      KSQL CLI session.

For the `NEW_ORDERS` topic, run:

```sql
SELECT ORDER_ID, TOTAL_AMOUNT, CUSTOMER_NAME FROM NEW_ORDERS LIMIT 3;
```

Your output should resemble:

```
1 | 10.5 | Bob Smith
2 | 3.32 | Sarah Black
3 | 21.0 | Emma Turner
```

For the `SHIPMENTS` topic, run:

```sql
SELECT ORDER_ID, SHIPMENT_ID, WAREHOUSE FROM SHIPMENTS LIMIT 2;
```

Your output should resemble:

```
1 | 42 | Nashville
3 | 43 | Palo Alto
```

Run the following query, which will show orders with associated
shipments, based on a join window of 1 hour.

```sql
SELECT O.ORDER_ID, O.TOTAL_AMOUNT, O.CUSTOMER_NAME,
S.SHIPMENT_ID, S.WAREHOUSE
FROM NEW_ORDERS O
INNER JOIN SHIPMENTS S
  WITHIN 1 HOURS
  ON O.ORDER_ID = S.ORDER_ID;
```

Your output should resemble:

```
1 | 10.5 | Bob Smith | 42 | Nashville
3 | 21.0 | Emma Turner | 43 | Palo Alto
```

Note that message with `ORDER_ID=2` has no corresponding `SHIPMENT_ID`
or `WAREHOUSE` - this is because there is no corresponding row on the
shipments stream within the time window specified.

Press Ctrl+C to cancel the `SELECT` query and return to the KSQL prompt.

Table-Table join
----------------

Using a table-table join, it is possible to join two *tables* of on a
common key. KSQL tables provide the latest *value* for a given *key*.
They can only be joined on the *key*, and one-to-many (1:N) joins are
not supported in the current semantic model.

In this example we have location data about a warehouse from one system,
being enriched with data about the size of the warehouse from another.

In a separate console window, populate the two topics by using the
`kafkacat` utility:

```bash
<path-to-confluent>/bin/kafka-console-producer \
  --broker-list localhost:9092 \
  --topic warehouse_location \
  --property "parse.key=true" \
  --property "key.separator=:"<<EOF
```

Your output should resemble:

```json
1:{"warehouse_id":1,"city":"Leeds","country":"UK"}
2:{"warehouse_id":2,"city":"Sheffield","country":"UK"}
3:{"warehouse_id":3,"city":"Berlin","country":"Germany"}
EOF
```

```bash
<path-to-confluent>/bin/kafka-console-producer \
  --broker-list localhost:9092 \
  --topic warehouse_size \
  --property "parse.key=true" \
  --property "key.separator=:"<<EOF
```

Your output should resemble:

```json
1:{"warehouse_id":1,"square_footage":16000}
2:{"warehouse_id":2,"square_footage":42000}
3:{"warehouse_id":3,"square_footage":94000}
EOF
```

In the KSQL CLI, register both topics as KSQL tables:

```sql
CREATE TABLE WAREHOUSE_LOCATION (WAREHOUSE_ID INT, CITY VARCHAR, COUNTRY VARCHAR)
WITH (KAFKA_TOPIC='warehouse_location',
      VALUE_FORMAT='JSON',
      KEY='WAREHOUSE_ID');

CREATE TABLE WAREHOUSE_SIZE (WAREHOUSE_ID INT, SQUARE_FOOTAGE DOUBLE)
WITH (KAFKA_TOPIC='warehouse_size',
      VALUE_FORMAT='JSON',
      KEY='WAREHOUSE_ID');
```

After both `CREATE TABLE` statements, your output should resemble:

```
 Message
---------------
 Table created
---------------
```

Check both tables that the message key (`ROWKEY`) matches the declared
key (`WAREHOUSE_ID`) - the output should show that they are equal. If
they are not, the join will not succeed or behave as expected.

!!! tip
      Run the following to tell KSQL to read from the beginning of the topic: 
      ```sql
      SET 'auto.offset.reset' = 'earliest';
      ```
      You can skip this if you have already run it within your current
      KSQL CLI session.

Inspect the WAREHOUSE_LOCATION table:

```sql
SELECT ROWKEY, WAREHOUSE_ID FROM WAREHOUSE_LOCATION LIMIT 3;
```

Your output should resemble:

```
1 | 1
2 | 2
3 | 3
Limit Reached
Query terminated
```

Inspect the WAREHOUSE_SIZE table:

```sql
SELECT ROWKEY, WAREHOUSE_ID FROM WAREHOUSE_SIZE LIMIT 3;
```

Your output should resemble:

```
1 | 1
2 | 2
3 | 3
Limit Reached
Query terminated
```

Now join the two tables:

```sql
SELECT WL.WAREHOUSE_ID, WL.CITY, WL.COUNTRY, WS.SQUARE_FOOTAGE
FROM WAREHOUSE_LOCATION WL
  LEFT JOIN WAREHOUSE_SIZE WS
    ON WL.WAREHOUSE_ID=WS.WAREHOUSE_ID
LIMIT 3;
```

Your output should resemble:

```
1 | Leeds | UK | 16000.0
2 | Sheffield | UK | 42000.0
3 | Berlin | Germany | 94000.0
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
     format=avro \
     topic=orders_local 

<path-to-confluent>/bin/ksql-datagen \ 
     quickstart=orders \
     format=avro \
     topic=orders_3rdparty 
```

In KSQL, register the source topic for each:

```sql
CREATE STREAM ORDERS_SRC_LOCAL
  WITH (KAFKA_TOPIC='orders_local', VALUE_FORMAT='AVRO');

CREATE STREAM ORDERS_SRC_3RDPARTY
  WITH (KAFKA_TOPIC='orders_3rdparty', VALUE_FORMAT='AVRO');
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
CREATE STREAM ALL_ORDERS AS SELECT 'LOCAL' AS SRC, * FROM ORDERS_SRC_LOCAL;
```

Your output should resemble:

```
 Message
----------------------------
 Stream created and running
----------------------------
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
 ROWKEY     | VARCHAR(STRING)  (system)
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
INSERT INTO ALL_ORDERS SELECT '3RD PARTY' AS SRC, * FROM ORDERS_SRC_3RDPARTY;
```

Your output should resemble:

```
 Message
-------------------------------
 Insert Into query is running.
-------------------------------
```

Query the output stream to verify that data from each source is being
written to it:

```sql
SELECT * FROM ALL_ORDERS;
```

Your output should resemble the following. Note that there are messages
from both source topics (denoted by `LOCAL` and `3RD PARTY`
respectively).

```
1531736084879 | 1802 | 3RD PARTY | 1508543844870 | 1802 | Item_427 | 5.003326679575532 | {CITY=City_27, STATE=State_63, ZIPCODE=12589}
1531736085016 | 1836 | LOCAL | 1489112050820 | 1836 | Item_224 | 9.561788841477156 | {CITY=City_67, STATE=State_99, ZIPCODE=28638}
1531736085118 | 1803 | 3RD PARTY | 1516295084125 | 1803 | Item_208 | 7.984495994658404 | {CITY=City_13, STATE=State_56, ZIPCODE=23417}
1531736085222 | 1804 | 3RD PARTY | 1503734687976 | 1804 | Item_498 | 4.8212828530483876 | {CITY=City_42, STATE=State_45, ZIPCODE=87842}
1531736085444 | 1837 | LOCAL | 1511189492298 | 1837 | Item_183 | 1.3867306505950954 | {CITY=City_28, STATE=State_86, ZIPCODE=14742}
1531736085531 | 1838 | LOCAL | 1497601536360 | 1838 | Item_945 | 4.825111590185673 | {CITY=City_78, STATE=State_13, ZIPCODE=59763}
…
```

Press Ctrl+C to cancel the `SELECT` query and return to the KSQL prompt.

You can view the two queries that are running using `SHOW QUERIES`:

```sql
SHOW QUERIES;
```

Your output should resemble:

```
Query ID          | Kafka Topic | Query String
-------------------------------------------------------------------------------------------------------------------
CSAS_ALL_ORDERS_0 | ALL_ORDERS  | CREATE STREAM ALL_ORDERS AS SELECT 'LOCAL' AS SRC, * FROM ORDERS_SRC_LOCAL;
InsertQuery_1     | ALL_ORDERS  | INSERT INTO ALL_ORDERS SELECT '3RD PARTY' AS SRC, * FROM ORDERS_SRC_3RDPARTY;
-------------------------------------------------------------------------------------------------------------------
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
    `CTAS_PAGEVIEWS_REGIONS`:

    ```sql
    TERMINATE CTAS_PAGEVIEWS_REGIONS;
    ```

    !!! tip
          The actual name of the query running may vary; refer to the output
          of `SHOW QUERIES;`.

2.  Run the `exit` command to leave the KSQL CLI.

    ```
    ksql> exit
    Exiting KSQL.
    ```

Confluent CLI
-------------

If you are running {{ site.cp }} using the CLI, you can stop it with
this command.

```bash
<path-to-confluent>/bin/confluent local stop
```

Page last revised on: {{ git_revision_date }}
