# Quick Start

| [Overview](/docs#ksql-documentation) | Quick Start | [Concepts](/docs/concepts.md#concepts) | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) |[Demo](/ksql-clickstream-demo#clickstream-analysis) | [Examples](/docs/examples.md#examples) | [FAQ](/docs/faq.md#frequently-asked-questions)  |
|---|----|-----|----|----|----|----|

Welcome to the quick start for KSQL!

The goal of this quick start is to demonstrate a simple workflow using KSQL to write streaming queries against data in Kafka.

**Table of Contents**

- [Setup](#setup)
- [Create a STREAM and TABLE](#create-a-stream-and-table)
- [Write Queries](#write-queries)
- [Terminate and Exit](#terminate-and-exit)

## Setup

Because KSQL queries data in a Kafka cluster, you will need to bring up a Kafka cluster, including ZooKeeper and a Kafka broker. Do not run KSQL against a production Kafka cluster while KSQL is in developer preview.

1.  Bring up a Kafka cluster and start KSQL.

  * [Follow these instructions if you are using Docker](/docs/quickstart/quickstart-docker.md#docker-setup-for-ksql)  (we recommend Docker for simplicity)
  * [Follow these instructions if you are not using Docker](quickstart-non-docker.md#non-docker-setup-for-ksql)

2.  After you have successfully started the Kafka cluster and started KSQL, you will see the KSQL prompt:

    ```bash
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

    Copyright 2017 Confluent Inc.

    CLI v0.5, Server v0.5 located at http://localhost:8090

    Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

    ksql>
    ```

3.  KSQL provides a structured query language to query Kafka data, so you need some data to query. For this quick start, you will produce mock streams to the Kafka cluster.

  * If you are using our Docker Compose files, a Docker container is already running with a data generator that is continuously producing Kafka messages to the Kafka cluster. No further action is required
  * If you are not using our Docker environment, then follow these [instructions](quickstart-non-docker.md#produce-topic-data) to generate data to the Kafka cluster

## Create a Stream and Table

This KSQL quick start shows examples querying data from Kafka topics called `pageviews` and `users` using the following schemas:

![image](/docs/quickstart/ksql-quickstart-schemas.jpg)
    
Before proceeding, please check:

* In the terminal window where you started KSQL, you see the `ksql>` prompt
* If you are not using Docker, you must manually have run the data generator to produce topics called `pageviews` and `users`. If you haven't done this, please follow these [instructions](/docs/quickstart/quickstart-non-docker.md#produce-topic-data) to generate data. (Docker compose file automatically runs the data generator)


1. Create a STREAM `pageviews_original` from the Kafka topic `pageviews`, specifying the `value_format` of `DELIMITED`. Describe the new STREAM.  Notice that KSQL created additional columns called `ROWTIME`, which corresponds to the Kafka message timestamp, and `ROWKEY`, which corresponds to the Kafka message key.

   ```bash
   ksql> CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH (kafka_topic='pageviews', value_format='DELIMITED');

   ksql> DESCRIBE pageviews_original;

    Field    | Type
   --------------------------------------
    ROWTIME  | BIGINT           (system)
    ROWKEY   | VARCHAR(STRING)  (system)
    VIEWTIME | BIGINT
    USERID   | VARCHAR(STRING)
    PAGEID   | VARCHAR(STRING)
   --------------------------------------
    For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
   ```

2. Create a TABLE `users_original` from the Kafka topic `users`, specifying the `value_format` of `JSON`. Describe the new TABLE.

   ```bash
   ksql> CREATE TABLE users_original (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (kafka_topic='users', value_format='JSON', key = 'userid');

   ksql> DESCRIBE users_original;

    Field        | Type
   ------------------------------------------
    ROWTIME      | BIGINT           (system)
    ROWKEY       | VARCHAR(STRING)  (system)
    REGISTERTIME | BIGINT
    GENDER       | VARCHAR(STRING)
    REGIONID     | VARCHAR(STRING)
    USERID       | VARCHAR(STRING)
   ------------------------------------------
   For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
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

**Note:** By default KSQL reads the topics for streams and tables from the latest offset.

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
   --------------------------------------
	ROWTIME  | BIGINT           (system)
	ROWKEY   | VARCHAR(STRING)  (system)
	USERID   | VARCHAR(STRING)  (key)
	PAGEID   | VARCHAR(STRING)
	REGIONID | VARCHAR(STRING)
	GENDER   | VARCHAR(STRING)
   --------------------------------------
   For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
    ```

3. Use `SELECT` to view query results as they come in. To stop viewing the query results, press `<ctrl-c>`. This stops printing to the console but it does not terminate the actual query. The query continues to run in the underlying KSQL application.

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

5. Create a new persistent query that counts the pageviews for each region and gender combination in a [tumbling window](http://docs.confluent.io/current/streams/developer-guide.html#tumbling-time-windows) of 30 seconds when the count is greater than 1. Results from this query are written to a Kafka topic called `PAGEVIEWS_REGIONS` in the Avro format. KSQL will register the avro schema with the configured schema registry when it writes the first message to the `PAGEVIEWS_REGIONS` topic. 

   ```bash
   ksql> CREATE TABLE pageviews_regions WITH (value_format='avro') AS SELECT gender, regionid , COUNT(*) AS numusers FROM pageviews_female WINDOW TUMBLING (size 30 second) GROUP BY gender, regionid HAVING COUNT(*) > 1;

   ksql> DESCRIBE pageviews_regions;

	Field   | Type
   --------------------------------------
	ROWTIME  | BIGINT           (system)
	ROWKEY   | VARCHAR(STRING)  (system)
	GENDER   | VARCHAR(STRING)  (key)
	REGIONID | VARCHAR(STRING)  (key)
	NUMUSERS | BIGINT
   --------------------------------------
   For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
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

7. Show all persistent queries.

   ```bash
   ksql> SHOW QUERIES;

	Query ID                      | Kafka Topic              | Query String
   ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	CTAS_PAGEVIEWS_REGIONS        | PAGEVIEWS_REGIONS        | CREATE TABLE pageviews_regions WITH (value_format='avro') AS SELECT gender, regionid , COUNT(*) AS numusers FROM pageviews_female WINDOW TUMBLING (size 30 second) GROUP BY gender, regionid HAVING COUNT(*) > 1;
	CSAS_PAGEVIEWS_FEMALE         | PAGEVIEWS_FEMALE         | CREATE STREAM pageviews_female AS SELECT users_original.userid AS userid, pageid, regionid, gender FROM pageviews_original LEFT JOIN users_original ON pageviews_original.userid = users_original.userid WHERE gender = 'FEMALE';
	CSAS_PAGEVIEWS_FEMALE_LIKE_89 | pageviews_enriched_r8_r9 | CREATE STREAM pageviews_female_like_89 WITH (kafka_topic='pageviews_enriched_r8_r9', value_format='DELIMITED') AS SELECT * FROM pageviews_female WHERE regionid LIKE '%_8' OR regionid LIKE '%_9';
   ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   ```


## Terminate and Exit

### KSQL

**Important:** Queries will continuously run as KSQL applications until they are manually terminated. Exiting KSQL does not terminate persistent queries.

1. From the output of `SHOW QUERIES;` identify a query ID you would like to terminate. For example, if you wish to terminate query ID `CTAS_PAGEVIEWS_REGIONS`:

   ```bash
   ksql> TERMINATE CTAS_PAGEVIEWS_REGIONS;
   ```

2. To exit from KSQL, type 'exit'.

   ```bash
   ksql> exit
   ```

### Docker
If you are running Docker Compose, you must explicitly shut down Docker Compose. For more information, see the [docker-compose down](https://docs.docker.com/compose/reference/down/) documentation.

**Important:** This command will delete all KSQL queries and topic data.

```
$ docker-compose down
```

### Confluent Platform
If you are running the Confluent Platform, you can stop it with this command. 

```
$ confluent stop
```

## Next steps
Try the end-to-end [Clickstream Analysis demo](/ksql-clickstream-demo#clickstream-analysis), which shows how to build an application that performs real-time user analytics.
