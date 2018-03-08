# Syntax Reference

| [Overview](/docs#ksql-documentation) | [Quick Start](/docs/quickstart#quick-start) | [Concepts](/docs/concepts.md#concepts) | Syntax Reference | [Demo](/ksql-clickstream-demo#clickstream-analysis) | [Examples](/docs/examples.md#examples) | [FAQ](/docs/faq.md#frequently-asked-questions)  |
|---|----|-----|----|----|----|----|


The KSQL CLI provides a terminal-based interactive shell for running queries.

**Table of Contents**

- [CLI-specific commands](#cli-specific-commands)
- [KSQL statements](#ksql-statements)
- [Scalar functions](#scalar-functions)
- [Aggregate functions](#aggregate-functions)
- [Configuring KSQL](#configuring-ksql)


# CLI-specific commands

Unlike KSQL statements such as `SELECT`, these commands are for setting a KSQL configuration, exiting the CLI, etc.
Run the CLI with `--help` to see the available options.

**Tip:** You can search and browse your command history in the KSQL CLI with `Ctrl-R`.  After pressing `Ctrl-R`, start
typing the command or any part of the command to show an auto-complete of past commands.

```
Description:
  The KSQL CLI provides a terminal-based interactive shell for running queries.  Each command must be on a separate
  line. For KSQL command syntax, see the documentation at https://github.com/confluentinc/ksql/docs/.

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
  Show previous lines entered during the current CLI session. You can use up and down arrow keys to navigate to the
  previous lines too.

version:
  Get the current KSQL version.

exit:
  Exit the CLI.


Default behavior:

    Lines are read one at a time and are sent to the server as KSQL unless one of the following is true:

    1. The line is empty or entirely whitespace. In this case, no request is made to the server.

    2. The line ends with backslash (`\`). In this case, lines are continuously read and stripped of their trailing
    newline and `\` until one is encountered that does not end with `\`; then, the concatenation of all lines read
    during this time is sent to the server as KSQL.
```


# KSQL statements

**Important:**

* KSQL statements must be terminated with a semicolon (`;`).
* Multi-line statements:
    * In the CLI you must use a back-slash (`\`) to indicate continuation of a statement on the next line.
    * Do not use `\` for multi-line statements in `.sql` files.


### CREATE STREAM

**Synopsis**

```sql
CREATE STREAM stream_name ( { column_name data_type } [, ...] )
  WITH ( property_name = expression [, ...] );
```

**Description**

Create a new stream with the specified columns and properties.

The supported column data types are:

* `BOOLEAN`
* `INTEGER`
* `BIGINT`
* `DOUBLE`
* `VARCHAR` (or `STRING`)
* `ARRAY<ArrayType>` (JSON and AVRO only)
* `MAP<VARCHAR, ValueType>` (JSON and AVRO only)

KSQL adds the implicit columns `ROWTIME` and `ROWKEY` to every stream and table, which represent the
corresponding Kafka message timestamp and message key, respectively.

The WITH clause supports the following properties:

| Property                | Description                                                                                |
|-------------------------|--------------------------------------------------------------------------------------------|
| KAFKA_TOPIC (required)  | The name of the Kafka topic that backs this stream. The topic must already exist in Kafka. |
| VALUE_FORMAT (required) | Specifies the serialization format of the message value in the topic.  Supported formats: `JSON`, `DELIMITED`, and `AVRO`|
| KEY                     | Associates the message key in the Kafka topic with a column in the KSQL stream. |
| TIMESTAMP               | Associates the message timestamp in the Kafka topic with a column in the KSQL stream. Time-based operations such as windowing will process a record according to this timestamp. |

Using Avro requires Confluent Schema Registry and setting `ksql.schema.registry.url` in the KSQL configuration file.

Example:

```sql
CREATE STREAM pageviews (viewtime BIGINT, user_id VARCHAR, page_id VARCHAR)
  WITH (VALUE_FORMAT = 'JSON',
        KAFKA_TOPIC = 'my-pageviews-topic');
```


### CREATE TABLE

**Synopsis**

```sql
CREATE TABLE table_name ( { column_name data_type } [, ...] )
  WITH ( property_name = expression [, ...] );
```

**Description**

Create a new table with the specified columns and properties.

The supported column data types are:

* `BOOLEAN`
* `INTEGER`
* `BIGINT`
* `DOUBLE`
* `VARCHAR` (or `STRING`)
* `ARRAY<ArrayType>` (JSON and AVRO only)
* `MAP<VARCHAR, ValueType>` (JSON and AVRO only)

KSQL adds the implicit columns `ROWTIME` and `ROWKEY` to every stream and table, which represent the
corresponding Kafka message timestamp and message key, respectively.

The WITH clause supports the following properties:

| Property                | Description                                                                                |
|-------------------------|--------------------------------------------------------------------------------------------|
| KAFKA_TOPIC (required)  | The name of the Kafka topic that backs this table. The topic must already exist in Kafka.  |
| VALUE_FORMAT (required) | Specifies the serialization format of the message value in the topic.  Supported formats: `JSON`, `DELIMITED`, and `AVRO`. |
| KEY          (required) | Associates the message key in the Kafka topic with a column in the KSQL table. |
| TIMESTAMP               | Associates the message timestamp in the Kafka topic with a column in the KSQL table. Time-based operations such as windowing will process a record according to this timestamp. |

Using Avro requires Confluent Schema Registry and setting `ksql.schema.registry.url` in the KSQL configuration file.

Example:

```sql
CREATE TABLE users (usertimestamp BIGINT, user_id VARCHAR, gender VARCHAR, region_id VARCHAR)
  WITH (VALUE_FORMAT = 'JSON',
        KAFKA_TOPIC = 'my-users-topic',
        KEY = 'user_id');
```


### CREATE STREAM AS SELECT

**Synopsis**

```sql
CREATE STREAM stream_name
  [WITH ( property_name = expression [, ...] )]
  AS SELECT  select_expr [, ...]
  FROM from_item [, ...]
  [ WHERE condition ]
  [PARTITION BY column_name];
```

**Description**

Create a new stream along with the corresponding Kafka topic, and continuously write the result of the SELECT query into
the stream and its corresponding topic.

If the PARTITION BY clause is present, then the resulting stream will have the specified column as its key.

The WITH clause supports the following properties:

| Property                | Description                                                                                |
|-------------------------|--------------------------------------------------------------------------------------------|
| KAFKA_TOPIC             | The name of the Kafka topic that backs this stream.  If this property is not set, then the name of the stream will be used as default. |
| VALUE_FORMAT            | Specifies the serialization format of the message value in the topic.  Supported formats: `JSON`, `DELIMITED`, and `AVRO`.  If this property is not set, then the format of the input stream/table will be used. |
| PARTITIONS              | The number of partitions in the topic.  If this property is not set, then the number of partitions of the input stream/table will be used. |
| REPLICAS                | The replication factor for the topic.  If this property is not set, then the number of replicas of the input stream/table will be used. |
| TIMESTAMP               | Associates the message timestamp in the Kafka topic with a column in the KSQL stream. Time-based operations such as windowing will process a record according to this timestamp. |

Using Avro requires Confluent Schema Registry and setting `ksql.schema.registry.url` in the KSQL configuration file.

> Note: The `KEY` property is not supported by CREATE STREAM AS -- use PARTITION BY instead.


### CREATE TABLE AS SELECT

**Synopsis**

```sql
CREATE TABLE stream_name
  [WITH ( property_name = expression [, ...] )]
  AS SELECT  select_expr [, ...]
  FROM from_item [, ...]
  [ WINDOW window_expression ]
  [ WHERE condition ]
  [ GROUP BY grouping_expression ]
  [ HAVING having_expression ];
```

**Description**

Create a new KSQL table along with the corresponding Kafka topic and stream the result of the
SELECT query as a changelog into the topic.

The WITH clause supports the following properties:

| Property                | Description                                                                                |
|-------------------------|--------------------------------------------------------------------------------------------|
| KAFKA_TOPIC             | The name of the Kafka topic that backs this table.  If this property is not set, then the name of the table will be used as default. |
| VALUE_FORMAT            | Specifies the serialization format of the message value in the topic.  Supported formats: `JSON`, `DELIMITED`, and `AVRO`.  If this property is not set, then the format of the input stream/table will be used. |
| PARTITIONS              | The number of partitions in the topic.  If this property is not set, then the number of partitions of the input stream/table will be used. |
| REPLICAS                | The replication factor for the topic.  If this property is not set, then the number of replicas of the input stream/table will be used. |
| TIMESTAMP               | Associates the message timestamp in the Kafka topic with a column in the KSQL table. Time-based operations such as windowing will process a record according to this timestamp. |

Using Avro requires Confluent Schema Registry and setting `ksql.schema.registry.url` in the KSQL configuration file.
Also since KSQL column names are case insensitive, avro field names will be considered case insensitive in KSQL.


### DESCRIBE

**Synopsis**

```sql
DESCRIBE [EXTENDED] (stream_name|table_name);
```


**Description**

* DESCRIBE: List the columns in a stream or table along with their data type and other attributes.
* DESCRIBE EXTENDED: Display DESCRIBE information with additional runtime statistics, Kafka topic details, and the
  set of queries that populate the table or stream

Example of describing a table:

```sqlite-psql
ksql> DESCRIBE ip_sum;

 Field   | Type
-------------------------------------
 ROWTIME | BIGINT           (system)
 ROWKEY  | VARCHAR(STRING)  (system)
 IP      | VARCHAR(STRING)  (key)
 KBYTES  | BIGINT
-------------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>

```

Example of describing a table with extended information:

```sqlite-psql
ksql> DESCRIBE EXTENDED ip_sum;
Type                 : TABLE
Key field            : CLICKSTREAM.IP
Timestamp field      : Not set - using <ROWTIME>
Key format           : STRING
Value format         : JSON
Kafka output topic   : IP_SUM (partitions: 4, replication: 1)

 Field   | Type
-------------------------------------
 ROWTIME | BIGINT           (system)
 ROWKEY  | VARCHAR(STRING)  (system)
 IP      | VARCHAR(STRING)  (key)
 KBYTES  | BIGINT
-------------------------------------

Queries that write into this TABLE
-----------------------------------
id:CTAS_IP_SUM - CREATE TABLE IP_SUM as SELECT ip,  sum(bytes)/1024 as kbytes FROM CLICKSTREAM window SESSION (300 second) GROUP BY ip;

For query topology and execution plan please run: EXPLAIN <QueryId>

Local runtime statistics
------------------------
messages-per-sec:      4.41   total-messages:       486     last-message: 12/14/17 4:32:23 PM GMT
 failed-messages:         0      last-failed:       n/a
(Statistics of the local KSQL Server interaction with the Kafka topic IP_SUM)
```


### EXPLAIN

**Synopsis**

```sqlite-psql
EXPLAIN (sql_expression|query_id);
```


**Description**

Show the execution plan for a SQL expression or, given the id of a running query, show the execution plan plus
additional runtime information and metrics.  Statements such as DESCRIBE EXTENDED, for example, show the ids of
queries related to a stream or table.

Example of explaining a running query:

```sqlite-psql
ksql> EXPLAIN ctas_ip_sum;

Type                 : QUERY
SQL                  : CREATE TABLE IP_SUM as SELECT ip,  sum(bytes)/1024 as kbytes FROM CLICKSTREAM window SESSION (300 second) GROUP BY ip;


Local runtime statistics
------------------------
messages-per-sec:     104.38   total-messages:       14238     last-message: 12/14/17 4:30:42 PM GMT
 failed-messages:          0      last-failed:         n/a
(Statistics of the local KSQL Server interaction with the Kafka topic IP_SUM)

Execution plan
--------------
 > [ PROJECT ] Schema: [IP : STRING , KBYTES : INT64].
         > [ AGGREGATE ] Schema: [CLICKSTREAM.IP : STRING , CLICKSTREAM.BYTES : INT64 , KSQL_AGG_VARIABLE_0 : INT64].
                 > [ PROJECT ] Schema: [CLICKSTREAM.IP : STRING , CLICKSTREAM.BYTES : INT64].
                         > [ REKEY ] Schema: [CLICKSTREAM.ROWTIME : INT64 , CLICKSTREAM.ROWKEY : STRING , CLICKSTREAM._TIME : INT64 , CLICKSTREAM.TIME : STRING , CLICKSTREAM.IP : STRING , CLICKSTREAM.REQUEST : STRING , CLICKSTREAM.STATUS : INT32 , CLICKSTREAM.USERID : INT32 , CLICKSTREAM.BYTES : INT64 , CLICKSTREAM.AGENT : STRING].
                                 > [ SOURCE ] Schema: [CLICKSTREAM.ROWTIME : INT64 , CLICKSTREAM.ROWKEY : STRING , CLICKSTREAM._TIME : INT64 , CLICKSTREAM.TIME : STRING , CLICKSTREAM.IP : STRING , CLICKSTREAM.REQUEST : STRING , CLICKSTREAM.STATUS : INT32 , CLICKSTREAM.USERID : INT32 , CLICKSTREAM.BYTES : INT64 , CLICKSTREAM.AGENT : STRING].


Processing topology
-------------------
Sub-topologies:
  Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [clickstream])
      --> KSTREAM-MAP-0000000001
    Processor: KSTREAM-MAP-0000000001 (stores: [])
      --> KSTREAM-TRANSFORMVALUES-0000000002
      <-- KSTREAM-SOURCE-0000000000
```


###  DROP STREAM

**Synopsis**

```sql
DROP STREAM stream_name;
```

**Description**

Drops an existing stream.


### DROP TABLE

**Synopsis**

```sql
DROP TABLE table_name;
```

**Description**

Drops an existing table.

### PRINT

```sql
PRINT qualifiedName (FROM BEGINNING)? ((INTERVAL | SAMPLE) number)?
```

**Description**

Print Kafka-topic contents to the KSQL CLI. Note, SQL grammar defaults to uppercase formatting, to print topics containing lower-case characters, use quotations as shown in the example.

For example:

```sql
ksql> PRINT 'ksql__commands' FROM BEGINNING;
Format:JSON
{"ROWTIME":1516010696273,"ROWKEY":"\"stream/CLICKSTREAM/create\"","statement":"CREATE STREAM clickstream (_time bigint,time varchar, ip varchar, request varchar, status int, userid int, bytes bigint, agent varchar) with (kafka_topic = 'clickstream', value_format = 'json');","streamsProperties":{}}
{"ROWTIME":1516010709492,"ROWKEY":"\"table/EVENTS_PER_MIN/create\"","statement":"create table events_per_min as select userid, count(*) as events from clickstream window  TUMBLING (size 10 second) group by userid;","streamsProperties":{}}
^CTopic printing ceased
```

### SELECT

**Synopsis**

```sql
SELECT select_expr [, ...]
  FROM from_item [, ...]
  [ WINDOW window_expression ]
  [ WHERE condition ]
  [ GROUP BY grouping_expression ]
  [ HAVING having_expression ];
```

**Description**

Selects rows from a KSQL stream or table. The result of this statement will not be persisted in a
 Kafka topic and will only be printed out in the console. To stop the continuous query in the CLI
  press `Ctrl-C`.

In the above statements from_item is one of the following:

- `stream_name [ [ AS ] alias]`
- `table_name [ [ AS ] alias]`
- `from_item LEFT JOIN from_item ON join_condition`

WHERE clause can refer to any column defined for a stream or table, including the two implicit columns `ROWTIME`
and `ROWKEY`.

Example:

```sql
SELECT * FROM pageviews
  WHERE ROWTIME >= 1510923225000
    AND ROWTIME <= 1510923228000;
```

> Tip: If you want to select older data, you can configure KSQL to query the stream from the beginning.  You must
> do this configuration before running the query:
>
> ```sql
> SET 'auto.offset.reset' = 'earliest';
> ```

The WINDOW clause lets you control how to *group input records that have the same key* into so-called *windows* for
operations such as aggregations or joins.  Windows are tracked per record key.  KSQL supports the following WINDOW
types:

* **TUMBLING**:
  Tumbling windows group input records into fixed-sized, non-overlapping windows based on the records' timestamps.
  You must specify the *window size* for tumbling windows. Note: Tumbling windows are a special case of hopping windows
  where the window size is equal to the advance interval.

  Example:

  ```sql
  SELECT item_id, SUM(quantity)
    FROM orders
    WINDOW TUMBLING (SIZE 20 SECONDS)
    GROUP BY item_id;
  ```

* **HOPPING**:
  Hopping windows group input records into fixed-sized, (possibly) overlapping windows based on the records' timestamps.
  You must specify the *window size* and the *advance interval* for hopping windows.

  Example:

  ```sql
  SELECT item_id, SUM(quantity)
    FROM orders
    WINDOW HOPPING (SIZE 20 SECONDS, ADVANCE BY 5 SECONDS)
    GROUP BY item_id;
  ```

* **SESSION**:
  Session windows group input records into so-called sessions.
  You must specify the *session inactivity gap* parameter for session windows.
  For example, imagine you set the inactivity gap to 5 minutes.  If, for a given record key such as "alice", no new
  input data arrives for more than 5 minutes, then the current session for "alice" is closed, and any newly arriving
  data for "alice" in the future will mark the beginning of a new session.

  Example:

  ```sql
  SELECT item_id, SUM(quantity)
    FROM orders
    WINDOW SESSION (20 SECONDS)
    GROUP BY item_id;
  ```

#### CAST

**Synopsis**

```sql
CAST (expression AS data_type);
```

You can cast an expression's type to a new type using CAST. Here is an example of converting a BIGINT into a VARCHAR
type:

```sql
-- This query converts the numerical count into a suffixed string; e.g., 5 becomes '5_HELLO'
SELECT page_id, CONCAT(CAST(COUNT(*) AS VARCHAR), '_HELLO')
  FROM pageviews_enriched
  WINDOW TUMBLING (SIZE 20 SECONDS)
  GROUP BY page_id;
```

#### LIKE

**Synopsis**

```sql
column_name LIKE pattern;
```

The LIKE operator is used for prefix or suffix matching. Currently KSQL supports `%`, which represents zero or more
characters.

Example:

```sql
SELECT user_id
  FROM users
  WHERE user_id LIKE 'santa%';
```


### SHOW TOPICS

**Synopsis**

```sql
SHOW | LIST TOPICS;
```

**Description**

List the available topics in the Kafka cluster that KSQL is configured to connect to (default setting for
`bootstrap.servers`: `localhost:9092`).


### SHOW STREAMS

**Synopsis**

```sql
SHOW | LIST STREAMS;
```

**Description**

List the defined streams.


### SHOW TABLES

**Synopsis**

```sql
SHOW | LIST TABLES;
```

**Description**

List the defined tables.


### SHOW QUERIES

**Synopsis**

```sql
SHOW QUERIES;
```

**Description**

List the running persistent queries.


### SHOW PROPERTIES

**Synopsis**

```sql
SHOW PROPERTIES;
```

**Description**

List the [configuration settings](#configuring-ksql) that are currently in effect.


### TERMINATE

**Synopsis**

```sql
TERMINATE query_id;
```

**Description**

Terminate a persistent query. Persistent queries run continuously until they are explicitly terminated.

* In standalone mode, exiting the CLI will stop (think: "pause") any persistent queries because exiting the CLI will
  also stop the KSQL server.  When the CLI is restarted, the server will be restarted, too, and any previously defined
  persistent queries will resume processing.
* In client-server mode, exiting the CLI will not stop persistent queries because the KSQL server(s) will continue to
  process the queries.

(To terminate a non-persistent query use `Ctrl-C` in the CLI.)


# Scalar functions

| Function            | Example                                                 | Description                          |
|---------------------|---------------------------------------------------------|--------------------------------------|
| ABS                 | `ABS(col1)`                                             | The absolute value of a value        |
| CEIL                | `CEIL(col1)`                                            | The ceiling of a value               |
| CONCAT              | `CONCAT(col1, '_hello')`                                | Concatenate two strings              |
| EXTRACTJSONFIELD    | `EXTRACTJSONFIELD(message, '$.log.cloud')`              |  Given a string column in JSON format, extract the field that matches |the given pattern.
| ARRAYCONTAINS       | `ARRAYCONTAINS('[1, 2, 3]', 3)`                         |  Given JSON or AVRO array checks if a search value contains in it. |
| FLOOR               | `FLOOR(col1)`                                           | The floor of a value                 |
| LCASE               | `LCASE(col1)`                                           | Convert a string to lowercase        |
| LEN                 | `LEN(col1)`                                             | The length of a string               |
| RANDOM              | `RANDOM()`                                              | Return a random DOUBLE value between 0 and 1.0 |
| ROUND               | `ROUND(col1)`                                           | Round a value to the nearest BIGINT value |
| STRINGTOTIMESTAMP   | `STRINGTOTIMESTAMP(col1, 'yyyy-MM-dd HH:mm:ss.SSS')`    |  Converts a string value in the given format into the BIGINT value representing the timestamp.  |
| SUBSTRING           | `SUBSTRING(col1, 2, 5)`    | Return the substring with the start and end indices |
| TIMESTAMPTOSTRING   | `TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS')` |  Converts a BIGINT timestamp value into the string representation of the timestamp in the given format.  |
| TRIM                | `TRIM(col1)`                                            | Trim the spaces from the beginning and end of a string |
| UCASE               | `UCASE(col1)`                                           | Convert a string to uppercase        |


# Aggregate functions

| Function    | Example                   | Description                                                     |
|-------------|---------------------------|-----------------------------------------------------------------|
| COUNT       | `COUNT(col1)`             | Count the number of rows                                        |
| MAX         | `MAX(col1)`               | Return the maximum value for a given column and window          |
| MIN         | `MIN(col1)`               | Return the minimum value for a given column and window          |
| SUM         | `SUM(col1)`               | Sums the column values                                          |
| TOPK        | `TOPK(col1, k)`           | Return the TopK values for the given column and window          |
| TOPKDISTINCT| `TOPKDISTINCT(col1, k)`   | Return the distinct TopK values for the given column and window |


# Configuring KSQL

You can set configuration properties for KSQL and your queries with the SET statement.  This includes
[settings for Kafka's Streams API](https://kafka.apache.org/documentation/#streamsconfigs)
(e.g., `cache.max.bytes.buffering`) as well as
settings for Kafka's [producer client](https://kafka.apache.org/documentation/#producerconfigs) and
[consumer client](https://kafka.apache.org/documentation/#newconsumerconfigs) (e.g., `auto.offset.reset`).

```sql
SET '<property-name>'='<property-value>';
```
 
Examples:

```
ksql> SET 'auto.offset.reset'='earliest';
ksql> SET 'commit.interval.ms'='5000';
```

Both property name and property value should be enclosed in single quotes.

> Tip: `SHOW PROPERTIES` shows the current settings.

Once a property has been set, it will remain in effect for the remainder of the KSQL CLI session until you issue another
SET statement to change it.

You can also use a *properties file* instead of the SET statement.  The syntax of properties files follow Java
conventions, which are slightly different to the syntax of the SET statement above.

```bash
# Show the example contents of a properties file
$ cat ksql.properties
auto.offset.reset=earliest

# Start KSQL in standalone mode with the custom properties above
$ ksql-cli local --properties-file ./ksql.properties

# Start a KSQL server node (for client-server mode) with the custom properties above
$ ksql-server-start ./ksql.properties
```

Note: Be careful when you are using KSQL in Docker because the properties file must be available inside the Docker
container.  If you don't want to customize your Docker setup so that it contains an appropriate properties file, you
should not use a properties file but the SET statement.
