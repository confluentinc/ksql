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

Create a Table from an existing Kafka Topic
-------------------------------------------

Use the CREATE TABLE statement to create a table from an existing
underlying Kafka topic. The Kafka topic must exist already in your Kafka cluster.

The following examples show how to create tables from a Kafka topic,
named `users`. To see these examples in action, create the `users` topic
by following the procedure in
[Write Streaming Queries Against {{ site.aktm }} Using ksqlDB](../tutorials/basics-docker.md).

### Create a Table with Selected Columns

The following example creates a table that has four columns from the
`users` topic: `registertime`, `userid`, `gender`, and `regionid`. Also,
the `userid` field is assigned as the table's KEY property.

!!! note
      The KEY field is optional. For more information, see
      [Key Requirements](syntax-reference.md#key-requirements).

ksqlDB can't infer the topic value's data format, so you must provide the
format of the values that are stored in the topic. In this example, the
data format is `JSON`. Other options are `Avro`, `DELIMITED`, `JSON_SR`, `PROTOBUF`, and `KAFKA`. For
more information, see
[Serialization Formats](serialization.md#serialization-formats).

ksqlDB requires keys to have been serialized using {{ site.ak }}'s own serializers or compatible
serializers. ksqlDB supports `INT`, `BIGINT`, `DOUBLE`, and `STRING` key types.

In the ksqlDB CLI, paste the following CREATE TABLE statement:

```sql
CREATE TABLE users
  (registertime BIGINT,
   userid VARCHAR,
   gender VARCHAR,
   regionid VARCHAR)
  WITH (KAFKA_TOPIC = 'users',
        VALUE_FORMAT='JSON',
        KEY = 'userid');
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
 ROWTIME      | BIGINT           (system)
 ROWKEY       | VARCHAR(STRING)  (system)
 REGISTERTIME | BIGINT
 USERID       | VARCHAR(STRING)
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
+---------------+--------+---------------+--------+--------+----------+
| ROWTIME       | ROWKEY | REGISTERTIME  | USERID | GENDER | REGIONID |
+---------------+--------+---------------+--------+--------+----------+
| 1541439611069 | User_2 | 1498028899054 | User_2 | MALE   | Region_1 |
| 1541439611320 | User_6 | 1505677113995 | User_6 | FEMALE | Region_7 |
| 1541439611396 | User_5 | 1491338621627 | User_5 | OTHER  | Region_2 |
| 1541439611536 | User_9 | 1492621173463 | User_9 | FEMALE | Region_3 |
^CQuery terminated
```

Press Ctrl+C to stop printing the query results.

The table values update continuously with the most recent records,
because the underlying `users` topic receives new messages continuously.

Create a Table backed by a new Kafka Topic
------------------------------------------

Use the CREATE TABLE statement to create a table without a preexisting
topic by providing the PARTITIONS count, and optionally the REPLICA count,
in the WITH clause.

Taking the example of the users table from above, but where the underlying
Kafka topic does not already exist, you can create the table by pasting
the following CREATE TABLE statement into the CLI:

```sql
CREATE TABLE users
  (registertime BIGINT,
   userid VARCHAR,
   gender VARCHAR,
   regionid VARCHAR)
  WITH (KAFKA_TOPIC = 'users',
        VALUE_FORMAT='JSON',
        PARTITIONS=4,
        REPLICAS=3
        KEY = 'userid');
```

This will create the users topics for you with the supplied partition and replica count.


Create a ksqlDB Table with Streaming Query Results
--------------------------------------------------

Use the CREATE TABLE AS SELECT statement to create a ksqlDB table that
contains the results of a SELECT query from another table or stream.

CREATE TABLE AS SELECT creates a new ksqlDB table with a corresponding
Kafka topic and streams the result of the SELECT query as a changelog
into the topic. ksqlDB creates a persistent query that runs continuously
until you terminate it explicitly.

To stream the result of a SELECT query into an *existing* table and its
underlying topic, use the INSERT INTO statement.

The following SQL statement creates a `users_female` table that
contains results from a persistent query for users that have `gender`
set to `FEMALE`:

```sql
CREATE TABLE users_female AS
  SELECT userid, gender, regionid FROM users
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
Key format: KAFKA (STRING)
Value format: JSON
rowTime: 12/21/18 23:58:42 PM PSD, key: User_5, value: {"USERID":"User_5","GENDER":"FEMALE","REGIONID":"Region_4"}
rowTime: 12/21/18 23:58:42 PM PSD, key: User_2, value: {"USERID":"User_2","GENDER":"FEMALE","REGIONID":"Region_7"}
rowTime: 12/21/18 23:58:42 PM PSD, key: User_9, value: {"USERID":"User_9","GENDER":"FEMALE","REGIONID":"Region_4"}
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
--------------------------------------

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
SELECT * FROM pageviews_table EMIT CHANGES;
```

Your output should resemble:

```
+---------------+---------------+---------------+------------------+--------+---------+------+
| ROWTIME       | WINDOWSTART   | WINDOWEND     | ROWKEY           | USERID | PAGEID  | TOTAL|
+---------------+---------------+---------------+------------------+--------+---------+------+
| 1557183919786 | 1557183900000 | 1557183960000 | User_5|+|Page_12 | User_5 | Page_12 | 1    |
| 1557183929488 | 1557183900000 | 1557183960000 | User_9|+|Page_39 | User_9 | Page_39 | 1    |
| 1557183930211 | 1557183900000 | 1557183960000 | User_1|+|Page_79 | User_1 | Page_79 | 1    |
| 1557183930687 | 1557183900000 | 1557183960000 | User_9|+|Page_34 | User_9 | Page_34 | 1    |
| 1557183929786 | 1557183900000 | 1557183960000 | User_5|+|Page_12 | User_5 | Page_12 | 2    |
| 1557183931095 | 1557183900000 | 1557183960000 | User_3|+|Page_43 | User_3 | Page_43 | 1    |
| 1557183930184 | 1557183900000 | 1557183960000 | User_1|+|Page_29 | User_1 | Page_29 | 1    |
| 1557183930727 | 1557183900000 | 1557183960000 | User_6|+|Page_93 | User_6 | Page_93 | 3    |
^CQuery terminated
```

!!! note
        It is possible for the same key to be output multiple time when emitting changes
        to the table. This is because each time the row in the table changes it will be emitted.

Look up the value for a specific key within the table by using a SELECT
statement.

```sql
SELECT * FROM pageviews_table WHERE ROWKEY='User_9|+|Page_39';
```

Your output should resemble:

```
+------------------+---------------+---------------+---------------+--------+---------+-------+
| ROWKEY           | WINDOWSTART   | WINDOWEND     | ROWTIME       | USERID | PAGEID  | TOTAL |
+------------------+---------------+---------------+---------------+--------+---------+-------+
| User_9|+|Page_39 | 1557183900000 | 1557183960000 | 1557183929488 | User_9 | Page_39 | 1     |
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
-   [Clickstream Data Analysis Pipeline Using ksqlDB (Docker)](../tutorials/clickstream-docker.md)

Page last revised on: {{ git_revision_date }}
