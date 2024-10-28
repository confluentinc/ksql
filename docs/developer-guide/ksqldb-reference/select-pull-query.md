---
layout: page
title: SELECT (Pull Query)
tagline:  ksqlDB SELECT statement for pull queries
description: Syntax for the SELECT statement in ksqlDB for pull queries
keywords: ksqlDB, select, pull query
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/select-pull-query.html';
</script>

## Synopsis

```sql
SELECT select_expr [, ...]
  FROM from_item
  [ WHERE where_condition ]
  [ AND window_bounds ]
  [ LIMIT count ];
```

## Description

Pulls the current value from the `from_item` and terminates. The `from_item` can be a 
materialized view, a table, or a stream. The result
of this statement is not persisted in a {{ site.ak }} topic and is printed out
only in the console. Pull queries run with predictably low latency because 
materialized views are incrementally updated as new events arrive.
They are a great match for request/response flows. For asynchronous application flows, see 
[Push Queries](select-push-query.md).

!!! Tip "See pull queries in action"
    - [Confluent Platform quickstart](https://ksqldb.io/quickstart-platform.html#quickstart-content)
    - [Confluent Cloud quickstart](https://ksqldb.io/quickstart-cloud.html#quickstart-content)
    - Recipe: [Detect Unusual Credit Card Activity](https://developer.confluent.io/tutorials/credit-card-activity/confluent.html#execute-ksqldb-code)

You can execute a pull query by sending an HTTP request to the ksqlDB REST API, and
the API responds with a single response.  

-   Pull queries are expressed using a strict subset of ANSI SQL.
-   You can issue a pull query against any table that was created by a 
    [CREATE TABLE AS SELECT](create-table-as-select.md) statement.
-   You can issue a pull query against any stream.    
-   Currently, we do not support pull queries against tables created by using a [CREATE TABLE](create-table.md) statement.
-   Pull queries do not support `JOIN`, `PARTITION BY`, `GROUP BY` and `WINDOW` clauses (but can query materialized tables that contain those clauses).
-   `LIMIT` clause supports non-negative integers.

!!! important
    ksqlDB may experience a deadlock if you run multiple pull queries concurrently.

    If you experience hanging pull queries, the following mitigations may help.

    - Rewrite your persistent queries or add a new persistent query to enable
      efficient pull queries, ideally key lookups, but at least short-range scans,
      or decrease state store size.
    - Rewrite your table scans as individual key lookups.
    - Continue issuing table scans, but issue fewer of them at once.
    - Adjust your pull query load to avoid sharp spikes in the pull query
      request rate all at once.
    - Implement timeouts from your client so that if pull queries take too
      long to run, your client terminates the connection to free up resources
      and unblock other queries.

## `WHERE` Clause Guidelines

By default, only key lookups are enabled. They have the following requirements:

-   Key column(s) must use an equality comparison to a literal (e.g. `KEY = 'abc'`).
-   On windowed tables, `WINDOWSTART` and `WINDOWEND` can be optionally compared to literals. 
    For more information on windowed tables, see [Time and Windows in ksqlDB](/concepts/time-and-windows-in-ksqldb-queries).

You can loosen the restrictions on the `WHERE` clause, or eliminate the `WHERE` clause altogether, 
by enabling table scans in your current CLI session with the command `SET 'ksql.query.pull.table.scan.enabled'='true';`. 
Table scans can also be enabled by default by setting a server configuration property with 
`ksql.query.pull.table.scan.enabled=true`. Once table scans are enabled, the following additional expressions are allowed:

-   Key column(s) using range comparisons to literals.
-   Non-key columns to be used alone, without key references.
-   Columns to be compared to other columns.
-   References to subsets of columns from a multi-column key.
-   Complex expressions without direct column references using UDFs and function calls (e.g. `instr(NAME_COL, 'hello') > 0`).

!!! note
	Table scan based queries are just the next incremental step for ksqlDB pull queries. 
	In future releases, we will continue pushing the envelope of new query capabilities and 
	greater performance and efficiency.

## Examples

### Pull queries

The following examples show pull queries against a table named `TOP_TEN_RANKS`
created by using a [CREATE TABLE AS SELECT](create-table-as-select.md)
statement.

First, create a table named `GRADES` by using a [CREATE TABLE](create-table.md) 
statement:

```sql
CREATE TABLE GRADES (ID INT PRIMARY KEY, GRADE STRING, RANK INT) 
  WITH (kafka_topic = 'test_topic', value_format = 'JSON', partitions = 4);
```

Then, create a derived table named `TOP_TEN_RANKS` by using a 
[CREATE TABLE AS SELECT](create-table-as-select.md) statement:

```sql
CREATE TABLE TOP_TEN_RANKS 
  AS SELECT ID, RANK 
  FROM GRADES 
  WHERE RANK <= 10;
```

If you want to look up only the student with `ID = 5` in the `TOP_TEN_RANKS` table using a pull query:

```sql
SELECT * FROM TOP_TEN_RANKS
  WHERE ID = 5;
```

After enabling table scans, you can fetch the current state of your `TOP_TEN_RANKS` table using a pull query:

```sql
SELECT * FROM TOP_TEN_RANKS;
```

If you want to look up the students whose ranks lie in the range `(4, 8)`:

```sql
SELECT * FROM TOP_TEN_RANKS
  WHERE RANK > 4 AND RANK < 8;
```

#### STREAM
Pull queries against a stream.

First, create a stream named `STUDENTS` by using a [CREATE STREAM](create-stream.md) statement:

```sql
CREATE STREAM STUDENTS (ID STRING KEY, SCORE INT) 
  WITH (kafka_topic='students_topic', value_format='JSON', partitions=4);
```

If you want to look up students with ranks greater than `5` you can issue the query:

```sql
SELECT * FROM STUDENTS
  WHERE rank > 5;
```

#### INNER JOIN

Pull queries against a table `INNER_JOIN` that is created by joining multiple tables:

```sql
CREATE TABLE LEFT_TABLE (ID BIGINT PRIMARY KEY, NAME varchar, VALUE bigint) 
  WITH (kafka_topic='left_topic', value_format='JSON', partitions=4);
```
```sql
CREATE TABLE RIGHT_TABLE (ID BIGINT PRIMARY KEY, F1 varchar, F2 bigint) 
  WITH (kafka_topic='right_topic', value_format='JSON', partitions=4);
```

```sql
CREATE TABLE INNER_JOIN AS SELECT L.ID, NAME, VALUE, F1, F2 FROM LEFT_TABLE L JOIN RIGHT_TABLE R ON L.ID = R.ID;
```

You can fetch the current state of your table `INNER_JOIN` by using a pull query:

```sql
SELECT * FROM INNER_JOIN [ WHERE where_condition ];
```

!!! Tip "See INNER_JOIN in action"
    - [Analyze datacenter power usage](https://developer.confluent.io/tutorials/datacenter/confluent.html#execute-ksqldb-code)
    - [Build a dynamic pricing strategy](https://developer.confluent.io/tutorials/dynamic-pricing/confluent.html#execute-ksqldb-code)
    - [Notify passengers of flight updates](https://developer.confluent.io/tutorials/aviation/confluent.html#execute-ksqldb-code)
    - [Streaming ETL pipeline](/tutorials/etl/#join-the-streams-together)

#### WINDOW

Pull queries against a windowed table `NUMBER_OF_TESTS` created by aggregating the stream `STUDENTS` 
created above:

```sql
CREATE TABLE NUMBER_OF_TESTS AS 
  SELECT ID, COUNT(1) AS COUNT 
  FROM STUDENTS 
  WINDOW TUMBLING(SIZE 1 SECOND) 
  GROUP BY ID;
```

Look up the number of tests taken by a student with `ID='10'`:

```sql
SELECT * 
  FROM NUMBER_OF_TESTS 
  WHERE ID='10';
```

Look up the number of tests taken by a student with `ID='10'` 
in the window range `100 <= WindowStart AND WindowEnd <= 16000`:

```sql
SELECT *
  FROM NUMBER_OF_TESTS 
  WHERE ID='10' AND 100 <= WindowStart AND WindowEnd <= 16000;
```

### STRUCT output

You can output a [struct](/reference/sql/data-types#struct) from a query
by using a SELECT statement. The following example creates a struct from a
stream named `s1`.

```sql
SELECT STRUCT(f1 := v1, f2 := v2) FROM s1;
```

### Pull queries with pseudo columns

You can use the `ROWTIME` pseudo column within pull queries. Below
is an example of issuing a pull query with `ROWTIME` in both the
`SELECT` and `WHERE` clauses.

```sql
SELECT name, ROWTIME FROM users WHERE ROWTIME > 50000;
```

However, this is disallowed for `ROWPARTITION` and `ROWOFFSET`.