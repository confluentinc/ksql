---
layout: page
title: Create a ksqlDB Table
tagline: Create a Table from a Kafka topic
description: Learn how to use the CREATE TABLE statement on a Kafka topic
---

In ksqlDB, you create tables from existing {{ site.aktm }} topics, create
tables that will create new {{ site.ak }} topics, or create tables of
query results from other tables or streams.

-   Use the CREATE TABLE statement to create a table from an existing Kafka topic,
    or a new Kafka topic.
-   Use the CREATE TABLE AS SELECT statement to create a table with
    query results from an existing table or stream.

!!! note
      Creating streams is similar to creating tables. For more information,
      see [Create a ksqlDB Stream](create-a-stream.md).
      
ksqlDB can't infer the topic value's data format, so you must provide the
format of the values that are stored in the topic. In this example, the
data format is `JSON`. For all supported formats, see
[Serialization Formats](serialization.md#serialization-formats).

ksqlDB requires keys to be serialized using {{ site.ak }}'s own serializers or
compatible serializers. For supported data types, see the [`KAFKA` format](./serialization.md#kafka). 
If the data in your {{ site.ak }} topics doesn't have a suitable key format, 
see [Key Requirements](syntax-reference.md#key-requirements).

Create a Table from an existing Kafka Topic
-------------------------------------------

Use the [CREATE TABLE](./create-table) statement to create a table from an existing
underlying {{ site.ak }} topic.

The following examples show how to create tables from a {{ site.ak }} topic
named `users`.

### Create a Table with Selected Columns

The following example creates a table that has four columns from the
`users` topic: `registertime`, `userid`, `gender`, and `regionid`. 
The `userid` column is the primary key of the table. This means that it's loaded from the {{ site.ak }} message
key. Primary key columns can't be `NULL`.

In the ksqlDB CLI, paste the following CREATE TABLE statement:

```sql
CREATE TABLE users (
    userid VARCHAR PRIMARY KEY,
    registertime BIGINT,
    gender VARCHAR,
    regionid VARCHAR
  ) WITH (
    KAFKA_TOPIC = 'users',
    VALUE_FORMAT='JSON'
);
```

Your output should resemble:

```
 Message
---------------
 Table created
---------------
```

Inspect the table by using the SHOW TABLES and DESCRIBE statements:

```sql
SHOW TABLES;
```

Your output should resemble:

```
 Table Name | Kafka Topic | Format | Windowed
----------------------------------------------
 USERS      | users       | JSON   | false
----------------------------------------------
```

Get the schema for the table:

```sql
DESCRIBE users;
```

Your output should resemble:

```
Name                 : USERS
 Field        | Type
------------------------------------------
 USERID       | VARCHAR(STRING)  (key)
 REGISTERTIME | BIGINT
 GENDER       | VARCHAR(STRING)
 REGIONID     | VARCHAR(STRING)
------------------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
```

Create a continuous streaming query on the `users` table by using the
SELECT statement:

```sql
SELECT * FROM users EMIT CHANGES;
```

Assuming the table has content, your output should resemble:

```
+--------+---------------+--------+----------+
| USERID | REGISTERTIME  | GENDER | REGIONID |
+--------+---------------+--------+----------+
| User_2 | 1498028899054 | MALE   | Region_1 |
| User_6 | 1505677113995 | FEMALE | Region_7 |
| User_5 | 1491338621627 | OTHER  | Region_2 |
| User_9 | 1492621173463 | FEMALE | Region_3 |
^CQuery terminated
```

Press Ctrl+C to stop printing the query results.

The table values update continuously with the most recent records,
because the underlying `users` topic receives new messages continuously.

### Creating a Table using Schema Inference

For supported [serialization formats](../developer-guide/serialization.md),
ksqlDB can integrate with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).
ksqlDB can use [Schema Inference](../concepts/schemas.md#schema-inference) to
spare you from defining columns manually in your `CREATE TABLE` statements.

The following example creates a table over an existing topic, loading the
value column definitions from {{ site.sr }}.

```sql
CREATE TABLE users (
    userid VARCHAR PRIMARY KEY
  ) WITH (
    KAFKA_TOPIC = 'users',
    VALUE_FORMAT='AVRO'
);
```

For more information, see [Schema Inference](../concepts/schemas.md#schema-inference).

Create a Table backed by a new Kafka Topic
------------------------------------------

Use the CREATE TABLE statement to create a table without a preexisting
topic by providing the PARTITIONS count, and optionally the REPLICA count,
in the WITH clause.

Taking the example of the users table from above, but where the underlying
Kafka topic does not already exist, you can create the table by pasting
the following CREATE TABLE statement into the CLI:

```sql
CREATE TABLE users (
    userid VARCHAR PRIMARY KEY,
    registertime BIGINT,
    gender VARCHAR,
    regionid VARCHAR
  ) WITH (
    KAFKA_TOPIC = 'users',
    VALUE_FORMAT='JSON',
    PARTITIONS=4,
    REPLICAS=3
  );
```

This will create the users topics for you with the supplied partition and replica count.

Create a ksqlDB Table with Streaming Query Results
--------------------------------------------------

Use the CREATE TABLE AS SELECT statement to create a ksqlDB table view that
contains the results of a SELECT query from another table or stream.

CREATE TABLE AS SELECT creates a new ksqlDB table with a corresponding
Kafka topic and streams the result of the SELECT query as a changelog
into the topic. ksqlDB creates a persistent query that runs continuously
until you terminate it explicitly.

The following SQL statement creates a `users_female` table that
contains results from a persistent query for users that have `gender`
set to `FEMALE`:

```sql
CREATE TABLE users_female AS
  SELECT userid, gender, regionid 
  FROM users
  WHERE gender='FEMALE'
  EMIT CHANGES;
```

Your output should resemble:

```
 Message
---------------------------
 Table created and running
---------------------------
```

Inspect the table by using the SHOW TABLES and PRINT statements:

```sql
SHOW TABLES;
```

Your output should resemble:

```
 Table Name   | Kafka Topic  | Format | Windowed
-------------------------------------------------
 USERS        | users        | JSON   | false
 USERS_FEMALE | USERS_FEMALE | JSON   | false
-------------------------------------------------
```

Print some rows in the table:

```sql
PRINT users_female;
```

Your output should resemble:

```
Key format: KAFKA_STRING
Value format: JSON
rowTime: 12/21/18 23:58:42 PM PSD, key: User_5, value: {"GENDER":"FEMALE","REGIONID":"Region_4"}
rowTime: 12/21/18 23:58:42 PM PSD, key: User_2, value: {"GENDER":"FEMALE","REGIONID":"Region_7"}
rowTime: 12/21/18 23:58:42 PM PSD, key: User_9, value: {"GENDER":"FEMALE","REGIONID":"Region_4"}
^CTopic printing ceased
```

Press Ctrl+C to stop printing the table.

!!! note
		The query continues to run after you stop printing the table.

Use the SHOW QUERIES statement to view the query that ksqlDB created for
the `users_female` table:

```sql
SHOW QUERIES;
```

Your output should resemble:

```
 Query ID            | Kafka Topic  | Query String

 CTAS_USERS_FEMALE_0 | USERS_FEMALE | CREATE TABLE users_female AS   SELECT userid, gender, regionid FROM users   WHERE gender='FEMALE' EMIT CHANGES;

For detailed information on a Query run: EXPLAIN <Query ID>;
```

A persistent query that's created by the CREATE TABLE AS SELECT
statement has the string `CTAS` in its ID, for example,
`CTAS_USERS_FEMALE_0`.

Create a ksqlDB Table from a ksqlDB Stream
------------------------------------------

Use the CREATE TABLE AS SELECT statement to create a table from a
stream. Creating a table from a stream requires aggregation, so you need
to include a function like COUNT(*) in the SELECT clause.

```sql
CREATE TABLE pageviews_table AS
  SELECT userid, pageid, COUNT(*) AS TOTAL
  FROM pageviews_original WINDOW TUMBLING (SIZE 1 MINUTES)
  GROUP BY userid, pageid
  EMIT CHANGES;
```

Your output should resemble:

```
 Message
---------------------------
 Table created and running
---------------------------
```

Observe the changes happening to the table by using a streaming SELECT
statement.

```sql
SELECT ROWTIME, * FROM pageviews_table EMIT CHANGES;
```

Your output should resemble:

```
+---------------+---------------+---------------+------------------+------+
| ROWTIME       | WINDOWSTART   | WINDOWEND     | KSQL_COL_0       | TOTAL|
+---------------+---------------+---------------+------------------+------+
| 1557183919786 | 1557183900000 | 1557183960000 | User_5|+|Page_12 | 1    |
| 1557183929488 | 1557183900000 | 1557183960000 | User_9|+|Page_39 | 1    |
| 1557183930211 | 1557183900000 | 1557183960000 | User_1|+|Page_79 | 1    |
| 1557183930687 | 1557183900000 | 1557183960000 | User_9|+|Page_34 | 1    |
| 1557183929786 | 1557183900000 | 1557183960000 | User_5|+|Page_12 | 2    |
| 1557183931095 | 1557183900000 | 1557183960000 | User_3|+|Page_43 | 1    |
| 1557183930184 | 1557183900000 | 1557183960000 | User_1|+|Page_29 | 1    |
| 1557183930727 | 1557183900000 | 1557183960000 | User_6|+|Page_93 | 3    |
^CQuery terminated
```

!!! note
        It is possible for the same key to be output multiple time when emitting changes
        to the table. This is because each time the row in the table changes it will be emitted.

Look up the value for a specific key within the table by using a SELECT
statement.

```sql
SELECT * FROM pageviews_table WHERE KSQL_COL_0='User_9|+|Page_39';
```

Your output should resemble:

```
+------------------+---------------+---------------+--------+
| KSQL_COL_0       | WINDOWSTART   | WINDOWEND     |  TOTAL |
+------------------+---------------+---------------+--------+
| User_9|+|Page_39 | 1557183900000 | 1557183960000 |  1     |
Query terminated
```


Delete a ksqlDB Table
---------------------

Use the DROP TABLE statement to delete a table. If you created the table
by using CREATE TABLE AS SELECT, you must first terminate the
corresponding persistent query.

Use the TERMINATE statement to stop the `CTAS_USERS_FEMALE_0` query:

```sql
TERMINATE CTAS_USERS_FEMALE_0;
```

Your output should resemble:

```
 Message
-------------------
 Query terminated.
-------------------
```

Use the DROP TABLE statement to delete the `users_female` table:

```sql
DROP TABLE users_female;
```

Your output should resemble:

```
 Message
-----------------------------------
 Source USERS_FEMALE was dropped.
-----------------------------------
```

Next Steps
----------

-   [Join Event Streams with ksqlDB](joins/join-streams-and-tables.md)
