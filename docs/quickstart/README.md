# Quick Start Guide

| [Overview](/docs/) | [Installation](/docs/installation.md) | Quick Start Guide | [Syntax Reference](/docs/syntax-reference.md) | [Examples](/docs/examples.md) | [FAQ](/docs/faq.md)  |
|----------|--------------|-------------|------------------|------------------|------------------|

> *Important: This release is a *developer preview* and is free and open-source from Confluent under the Apache 2.0 license.*

**Table of Contents**

- [Setup](#setup)
- [Create a STREAM and TABLE](#create-a-stream-and-table)
- [Write Queries](#write-queries)
- [Terminate and Exit](#terminate-and-exit)

Welcome to the quick start guide for KSQL!

The goal of this quick start guide is to demonstrate a simple workflow using KSQL to write streaming queries against data in Kafka.


## Setup

Because KSQL queries data in a Kafka cluster, you will need to bring up a Kafka cluster, including ZooKeeper and a Kafka broker. Do not run KSQL against a production Kafka cluster while KSQL is in developer preview.

1.  Bring up a Kafka cluster and start KSQL.

	* [Follow these instructions if you are using Docker](/docs/quickstart/quickstart-docker.rst)  (we recommend Docker for simplicity)
	* [Follow these instructions if you are not using Docker](quickstart-non-docker.rst)

2.  After you have successfully started the Kafka cluster and started KSQL, you will see the KSQL prompt:

    ``bash
    ksql>
    ``

3.  KSQL provides a structured query language to query Kafka data, so you need some data to query. For this quick start, you will produce mock streams to the Kafka cluster.

	* If you are using our Docker Compose files, a Docker container is already running with a data generator that is continuously producing Kafka messages to the Kafka cluster. No further action is required
	* If you are not using our Docker environment, then follow these [instructions](quickstart-non-docker.rst#produce-topic-data) to generate data to the Kafka cluster

## Create a STREAM and TABLE

This KSQL quick start shows examples querying data from Kafka topics called `pageviews` and `users` using the following schemas:

![image](/docs/quickstart/ksql-quickstart-schemas.jpg)
    
Before proceeding, please check:

* In the terminal window where you started KSQL, you see the `ksql>` prompt
* If you are not using Docker, you must manually have run the data generator to produce topics called `pageviews` and `users`. If you haven't done this, please follow these [instructions](/docs/quickstart/quickstart-non-docker.md#produce-topic-data) to generate data. (Docker compose file automatically runs the data generator)


1. Create a STREAM `pageviews_original` from the Kafka topic `pageviews`, specifying the `value_format` of `DELIMITED`. Describe the new STREAM.  Notice that KSQL created additional columns called `ROWTIME`, which corresponds to the Kafka message logstamp time, and `ROWKEY`, which corresponds to the Kafka message key.

   ```bash
   ksql> CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH (kafka_topic='pageviews', value_format='DELIMITED');

   ksql> DESCRIBE pageviews_original;

    Field    | Type            
   ----------------------------
    ROWTIME  | BIGINT          
    ROWKEY   | VARCHAR(STRING) 
    VIEWTIME | BIGINT          
    USERID   | VARCHAR(STRING) 
    PAGEID   | VARCHAR(STRING) 
    ```

2. Create a TABLE `users_original` from the Kafka topic `users`, specifying the `value_format` of `JSON`. Describe the new TABLE.

   ```bash
   ksql> CREATE TABLE users_original (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (kafka_topic='users', value_format='JSON');

   ksql> DESCRIBE users_original;

    Field        | Type            
   --------------------------------
    ROWTIME      | BIGINT          
    ROWKEY       | VARCHAR(STRING) 
    REGISTERTIME | BIGINT          
    GENDER       | VARCHAR(STRING) 
    REGIONID     | VARCHAR(STRING) 
    USERID       | VARCHAR(STRING)
    ``` 

3. Show all STREAMS and TABLES.

   ```bash
   ksql> SHOW STREAMS;
   
    Stream Name              | Kafka Topic              | Format    
   -----------------------------------------------------------------
    PAGEVIEWS_ORIGINAL       | pageviews                | DELIMITED 

   ksql> SHOW TABLES;
   
    Table Name        | Kafka Topic       | Format    | Windowed 
   --------------------------------------------------------------
    USERS_ORIGINAL    | users             | JSON      | false   
   ```


## Write Queries

1. Use `SELECT` to create a query that returns data from a STREAM. To stop viewing the data, press `<ctrl-c>`. You may optionally include the `LIMIT` keyword to limit the number of rows returned in the query result. Note that exact data output may vary because of the randomness of the data generation.

   ```bash
   ksql> SELECT pageid FROM pageviews_original LIMIT 3;
   Page_24
   Page_73
   Page_78
   LIMIT reached for the partition.
   Query terminated
   ksql> 
   ```

2. Create a persistent query by using the `CREATE STREAM` keywords to precede the `SELECT` statement. Unlike the non-persistent query above, results from this query are written to a Kafka topic `PAGEVIEWS_FEMALE`. The query below enriches the `pageviews` STREAM by doing a `LEFT JOIN` with the `users_original` TABLE on the user ID, where a condition is met.

   ```bash
   ksql> CREATE STREAM pageviews_female AS SELECT users_original.userid AS userid, pageid, regionid, gender FROM pageviews_original LEFT JOIN users_original ON pageviews_original.userid = users_original.userid WHERE gender = 'FEMALE';

   ksql> DESCRIBE pageviews_female;
    Field    | Type            
   ----------------------------
    ROWTIME  | BIGINT          
    ROWKEY   | VARCHAR(STRING) 
    USERID   | VARCHAR(STRING) 
    PAGEID   | VARCHAR(STRING) 
    REGIONID | VARCHAR(STRING) 
    GENDER   | VARCHAR(STRING) 
    ```

3. Use `SELECT` to view query results as they come in. To stop viewing the query results, press `<ctrl-c>`. This stops printing to the console but it does not terminate the actual query. The query continues to run in the underyling Kafka Streams application.

   ```bash
   ksql> SELECT * FROM pageviews_female;
   1502477856762 | User_2 | User_2 | Page_55 | Region_9 | FEMALE
   1502477857946 | User_5 | User_5 | Page_14 | Region_2 | FEMALE
   1502477858436 | User_3 | User_3 | Page_60 | Region_3 | FEMALE
   ^CQuery terminated
   ksql> 
   ```

4. Create a new persistent query where another condition is met, using `LIKE`. Results from this query are written to a Kafka topic called `pageviews_enriched_r8_r9`.

   ```bash
   ksql> CREATE STREAM pageviews_female_like_89 WITH (kafka_topic='pageviews_enriched_r8_r9', value_format='DELIMITED') AS SELECT * FROM pageviews_female WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';
   ```

5. Create a new persistent query that counts the pageviews for each region and gender combination in a `tumbling window <http://docs.confluent.io/current/streams/developer-guide.html#tumbling-time-windows>`__ of 30 seconds when the count is greater than 1. Results from this query are written to a Kafka topic called `PAGEVIEWS_REGIONS`.

   ```bash
   ksql> CREATE TABLE pageviews_regions AS SELECT gender, regionid , COUNT(*) AS numusers FROM pageviews_female WINDOW TUMBLING (size 30 second) GROUP BY gender, regionid HAVING COUNT(*) > 1;

   ksql> DESCRIBE pageviews_regions;

    Field    | Type            
   ----------------------------
    ROWTIME  | BIGINT          
    ROWKEY   | VARCHAR(STRING) 
    GENDER   | VARCHAR(STRING) 
    REGIONID | VARCHAR(STRING) 
    NUMUSERS | BIGINT 
    ```

6. Use `SELECT` to view results from the above query.

   ```bash
   ksql> SELECT regionid, numusers FROM pageviews_regions LIMIT 5;
   Region_3 | 4
   Region_3 | 5
   Region_6 | 5
   Region_6 | 6
   Region_3 | 8
   LIMIT reached for the partition.
   Query terminated
   ksql> 
   ```

7. Show all queries.

   ```bash
   ksql> SHOW QUERIES;

    Query ID | Kafka Topic              | Query String                                                                                                                                                                                                                      
   -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    1        | PAGEVIEWS_FEMALE         | CREATE STREAM pageviews_female AS SELECT users_original.userid AS userid, pageid, regionid, gender FROM pageviews_original LEFT JOIN users_original ON pageviews_original.userid = users_original.userid WHERE gender = 'FEMALE'; 
    2        | pageviews_enriched_r8_r9 | CREATE STREAM pageviews_female_like_89 WITH (kafka_topic='pageviews_enriched_r8_r9', value_format='DELIMITED') AS SELECT * FROM pageviews_female WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';                                
    3        | PAGEVIEWS_REGIONS        | CREATE TABLE pageviews_regions AS SELECT gender, regionid , COUNT(*) AS numusers FROM pageviews_female WINDOW TUMBLING (size 30 second) GROUP BY gender, regionid HAVING COUNT(*) > 1;   
   ```


## Terminate and Exit

1. Until you terminate a query, it will run continuously as a Kafka Streams application. From the output of `SHOW QUERIES;` identify a query ID you would like to terminate. For example, if you wish to terminate query ID `2`:

   ```bash
   ksql> terminate 2;
   ```

2. To exit from KSQL application, from the KSQL prompt `ksql>`, type 'exit'.

   ```bash
   ksql> exit
   ```
