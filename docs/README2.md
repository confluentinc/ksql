# KSQL Documentation

## Overview

KSQL is an open source streaming SQL engine that implements continuous, interactive queries against Apache Kafka™. It allows you to query, read, write, and process data in Apache Kafka in real-time, at scale using SQL an SQL-like commands. KSQL interacts directly with the [Kafka Streams API](http://docs.confluent.io/current/streams/concepts.html), removing the requirement of building a Java app. Common KSQL use cases are:

- Fraud detection - identify and act on out of the ordinary data to provide real-time awareness. 
- Personalization - create real-time experiences and insight for end users driven by data. 
- Notifications - build custom alerts and messages based on real-time data. 
- Real-time Analytics - power real-time dashboards to understand what’s happening as it does. 
- Sensor data and IoT - understand and deliver sensor data how and where it needs to be. 
- Customer 360 - provide a clear, real-time understanding of your customers across every interaction.

KSQL lowers the barriers for using real-time data in your applications. It is powered by a scalable streaming platform without the learning curve or additional management complexity of other stream processing solutions.

> *Important: This release is a *developer preview* and is free and open-source from Confluent under the Apache 2.0 license.*

## Getting Started
<!-- What do you users need to get started -->

* Beginners: Try the [interactive quickstart](/quickstart/).
* Advanced users: Try the [end-to-end KSQL demo](https://github.com/confluentinc/ksql).

# Concepts

KSQL provides a structured query language to do stream processing on Kafka topics.  KSQL is built on top of Kafka and the Kafka [Streams API](https://kafka.apache.org/documentation/streams). 

## Stream
A stream is an unbounded sequence of structured values that are stored in a Kafka topic. The structure of the values is specified in a schema. In Kafka streams vocabulary, a KSQL stream is a [KStream](http://docs.confluent.io/current/streams/concepts.html?highlight=kstream#kstream) plus a schema. 

## Table
A table in KSQL is finite, where the bounds are defined by the size of the key space. The key space is an evolving collection of structured values, where the structure of the values is specified in a schema. These values are stored in a changelog topic in Kafka. In Kafka Streams vocabulary, a KSQL table is a [KTable](http://docs.confluent.io/current/streams/concepts.html?highlight=ktable#ktable) plus a schema.

## Topic
A topic is a category or feed name where records are published. For more information, see the [Apache Kafka documentation](https://kafka.apache.org/documentation/#intro_topics).

# Installation

You can quickly install KSQL in your environment. 

You can use KSQL in stand-alone mode or in client-server mode.

In stand-alone mode, both the KSQL client and server components are co-located on the same machine, in the same JVM, and are started together which makes it convenient for local development and testing.

![alt text](https://user-images.githubusercontent.com/2977624/29090610-f4b11096-7c34-11e7-8a63-85c9ead22bc3.png)

In client-server mode, you can run a pool of KSQL servers on remote machines, VMs, or containers and the CLI connects to them over HTTP.

![alt text](https://user-images.githubusercontent.com/2977624/29090617-fab5e930-7c34-11e7-9eee-0554192854d5.png)

**Prerequisites:**

- [Maven](https://maven.apache.org/install.html)
- [Git](https://git-scm.com/downloads) and [Confluent KSQL repository](https://github.com/confluentinc/ksql)
- A Kafka cluster
- Java: Minimum version 1.7. 

1.  Navigate to the KSQL root directory and compile the KSQL code:

	```bash
	mvn clean install
	```

	When this command completes, the output should resemble:

	```bash
	...
	[INFO] BUILD SUCCESS
	[INFO] ------------------------------------------------------------------------
	[INFO] Total time: 02:56 min
	[INFO] Finished at: 2017-08-10T15:25:02-07:00
	[INFO] Final Memory: 168M/1755M
	[INFO] ------------------------------------------------------------------------
	```

1.  Start KSQL by running the compiled JAR file. Use the local argument for the developer preview. This starts the KSQL engine locally.

	```bash
	java -jar ksql-cli/target/ksql-cli-0.1-SNAPSHOT-standalone.jar local
	```

	When this command completes, you should see the KSQL prompt:

	```bash
	                       ======================================
	                       =      _  __ _____  ____  _          =
	                       =     | |/ // ____|/ __ \| |         =
	                       =     | ' /| (___ | |  | | |         =
	                       =     |  <  \___ \| |  | | |         =
	                       =     | . \ ____) | |__| | |____     =
	                       =     |_|\_\_____/ \___\_\______|    =
	                       =                                    =
	                       = Streaming Query Language for Kafka =
	Copyright 2017 Confluent Inc.                         

	CLI v0.0.1, Server v0.0.1 located at http://localhost:9098

	Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

	ksql> 
	```

# Limitations
Content TBD.

# Syntax Reference
The KSQL CLI provides a terminal-based interactive shell for running queries. The CLI is a self-executing JAR file, which means it acts like a normal UNIX executable.


# CLI-specific commands
These commands are non-KSQL statements such as setting a property or adding a resource. Run the CLI with the --help option to see the available options.

```bash
Description:
  The KSQL CLI provides a terminal-based interactive shell for running queries. Each command must be on a separate line. For KSQL command syntax, see the documentation at https://github.com/confluentinc/ksql/docs/.

help:
  Show this message.

clear:
  Clear the current terminal.

output:
  View the current output format.

output <format>:
  Set the output format to <format> (valid formats: 'JSON', 'TABULAR')
  For example: "output JSON"

history:
  Show previous lines entered during the current CLI session. You can use up and down arrow keys to navigate to the previous lines too.

version:
  Get the current KSQL version.

exit:
  Exit the CLI.


Default behavior:

    Lines are read one at a time and are sent to the server as KSQL unless one of the following is true:

    1. The line is empty or entirely whitespace. In this case, no request is made to the server.

    2. The line ends with backslash ('\'). In this case, lines are continuously read and stripped of their trailing newline and '\' until one is encountered that does not end with '\'; then, the concatenation of all lines read during this time is sent to the server as KSQL.
```

# KSQL commands
KSQL statements should be terminated with a semicolon (`;`). If desired, use a back-slash ('\\') to indicate continuation on the next line. 

## DDL statements

The supported DDL statements are described here.

### DESCRIBE stream-or-table
List the columns in a stream or table along with their data type and other attributes.

### SHOW | LIST TOPICS
List the topics in schema or in the current schema.

### SHOW | LIST STREAMS
List the streams in schema or in the current schema.

### SHOW | LIST TABLES
List the tables in schema or in the current schema.

### SHOW QUERIES
List the queries in schema or in the current schema.

### TERMINATE `query-id`
End a query. Queries will run continuously as Kafka Streams applications until they are explicitly terminated. 

### CREATE STREAM stream_name (  { column_name data_type} [, ...] ) WITH ( property_name = expression [, ...] );
Create a new empty Kafka stream with the specified columns and properties.

The supported column data types are BOOELAN(BOOL), INTEGER(INT), BIGINT(LONG), DOUBLE, VARCHAR (STRING), ARRAY<ArrayType> (JSON only) and MAP<VARCHAR, ValueType> (JSON only).

In addition to the defined columns in the statement, KSQL adds two implicit columns to every stream, ROWKEY and ROWTIME, which represent the corresponding Kafka message key and message timestamp.

The possible properties to set in the WITH clause:
* KAFKA_TOPIC: The name of the Kafka topic that this streams is built upon. The topic should already exist in Kafka. This is a required property.
* VALUE_FORMAT: Specifies the format in which the value in the topic that data is serialized in. Currently, KSQL supports JSON, delimited. This is a required property.
* KEY: The name of the column that is the key.
* TIMESTAMP: The name of the column that will be used as the timestamp. This can be used to define the event time.

Example

```
ksql> CREATE STREAM pageview (viewtime bigint, userid varchar, pageid varchar) WITH (value_format = 'json', kafka_topic='pageview_topic_json');
```

### CREATE TABLE table_name (  { column_name data_type} [, ...] ) WITH ( property_name = expression [, ...] );
Create a new KSQL table with the specified columns and properties. The supported column data types are BOOELAN(BOOL), INTEGER(INT), BIGINT(LONG), DOUBLE, VARCHAR (STRING), ARRAY<ArrayType> (JSON only) and MAP<VARCHAR, ValueType> (JSON only).

In addition to the defined columns in the statement, KSQL adds two implicit columns to every table, ROWKEY and ROWTIME, which represent the corresponding Kafka message key and message timestamp.

The possible properties to set in the WITH clause:
* KAFKA_TOPIC: The name of the Kafka topic that this streams is built upon. The topic should already exist in Kafka. This is a required property.
* VALUE_FORMAT: Specifies the format in which the value in the topic that data is serialized in. Currently, KSQL supports JSON, delimited. This is a required property.
* TIMESTAMP: The name of the column that will be used as the timestamp.

Example

```
ksql> CREATE TABLE users (usertimestamp bigint, userid varchar, gender varchar, regionid varchar) WITH (value_format = 'json', kafka_topic='user_topic_json'); 
```

## DML statements

### SELECT
Selects rows from a KSQL stream or table. The result of this statement will be printed out in the console. To stop the continuous query in the CLI press Ctrl+C.

```
SELECT `select_expr` [, ...] 
FROM `from_item` [, ...]
[ WINDOW `window_expression` ]
[ WHERE `condition` ]
[ GROUP BY `grouping expression` ]
[ HAVING `having_expression` ]
```

where `from_item` is one of the following:

- `table_name [ [ AS ] alias]`

- `from_item LEFT JOIN from_item ON join_condition`

The WINDOW clause is used to define a window for aggregate queries. KSQL supports the following WINDOW types:

* TUMBLING 
  The TUMBLING window requires a size parameter.

  Example

  ```
  ksql> SELECT * FROM orders WHERE orderunits > 5 ;
  ```

* HOPPING
  The HOPPING window is a fixed sized, (possibly) overlapping window. You must provide two values for a HOPPING window, size and advance interval. The following is an example query with hopping window.

  Example

  ```
  ksql> SELECT ITEMID, SUM(arraycol[0]) FROM ORDERS window HOPPING ( size 20 second, advance by 5 second) GROUP BY ITEMID;
  ```

* SESSION
  SESSION windows are used to aggregate key-based events into so-called sessions. The SESSION window requires the session inactivity gap size. 

  Example

  ```
  ksql> SELECT ITEMID, SUM(arraycol[0]) FROM ORDERS window SESSION (20 second) GROUP BY ITEMID;
  ```

### CREATE STREAM AS SELECT
Create a new KSQL stream along with the corresponding Kafka topic and stream the result of the SELECT query into the topic.  

```
CREATE STREAM `stream_name`
[WITH ( `property_name = expression` [, ...] )] 
AS SELECT  `select_expr` [, ...] 
FROM `from_item` [, ...] 
[ WHERE `condition` ] 
[PARTITION BY `column_name`]
```  
 
You can use the WITH section to set the properties for the result KSQL topic. The properties that can be set are:

* KAFKA_TOPIC: The name of KSQL topic and the corresponding Kafka topic associated with the new KSQL stream. If not specified, the name of the stream will be used as default.

* FORMAT: Specifies the format in which the result topic data is serialized in. KSQL supports JSON, Avro and CSV. If not set the same format of the input stream will be used.

* AVROSCHEMAFILE: The path to write the Avro schema file for the result. If the output format is Avro, avroschemafile will be set. If not set the generated schema file will be written to "/tmp/" folder with the name of the stream as the file name.

* PARTITIONS: The number of partitions in the sink stream.

* REPLICATIONS: The replication factor for the sink stream.

* TIMESTAMP: The name of the column that will be used as the timestamp. This can be used to define the event time.

### CREATE TABLE AS SELECT
Create a new KSQL table along with the corresponding KSQL topic and Kafka topic and stream the result of the SELECT query into the topic.  

```
CREATE TABLE `stream_name` 
[WITH ( `property_name = expression` [, ...] )] 
AS SELECT  `select_expr` [, ...] 
FROM `from_item` [, ...] 
[ WHERE `condition` ]
[ GROUP BY `grouping expression` ] 
[ HAVING `having_expression` ]
```

The WITH section can be used to set the properties for the result KSQL topic. The properties that can be set are as the following:

* KAFKA_TOPIC: The name of KSQL topic and the corresponding Kafka topic associated with the new KSQL stream. If not set, the name of the stream will be used as default.

* FORMAT: Specifies the format in which the result topic data is serialized in. KSQL supports JSON, Avro and CSV. If not set, the same format of the input stream will be used.

* AVROSCHEMAFILE: The path to write the Avro schema file for the result. If the output format is Avro, avroschemafile will be set. If not set the generated schema file will be written to `/tmp/` folder with the name of the stream as the file name.

* PARTITIONS: The number of partitions in the sink stream.

* REPLICATIONS: The replication factor for the sink stream.


## Scalar Functions
KSQL provides a set of internal functions that can use used in query expressions. Here are the available functions:

| Function | Example | Description |
|----------|---------|-------------|
| LCASE      | `LCASE(col1)`            | Convert a string to lowercase  |
| UCASE      | `UCASE(col1)`              | Convert a string to uppercase   |
| SUBSTRING  | `SUBSTRING(col1, 2, 5)`    | Return the substring with the start and end indices  |
| CONCAT     | `CONCAT(col1, '_hello')`   | Concatenate two strings  |
| TRIM       | `TRIM(col1)`       | Trim the spaces from the beginning and end of a string |
| LEN        | `LEN(col1)`                | The length of a string |
| ABS        | `ABS(col1)`                | The absolute value of a value |
| CEIL       | `CEIL(col1)`              | The ceiling of a value |
| FLOOR      | `FLOOR(col1)`             | The floor of a value |
| ROUND      | `ROUND(col1)`             | Round a value to the nearest integral value |
| RANDOM     | `RANDOM()`                | Return a random value between 0 and 1.0 |

## Aggregate Functions
KSQL provides a set of internal aggregate functions that can use used in query expressions. Here are the available aggregate functions:

| Function | Example | Description |
|----------|---------|-------------|
| COUNT      | `COUNT(col1)`             | Count the number of rows | 
| SUM        | `SUM(col1)`              | Sums the column values | 
| MIN        | `MIN(col1)`               | Return the min value for a given column and window | 
| MAX        | `MAX(col1)`               | Return the max value for a given column and window | 


