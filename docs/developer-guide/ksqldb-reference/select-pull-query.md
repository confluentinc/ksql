---
layout: page
title: SELECT (Pull Query)
tagline:  ksqlDB SELECT statement for pull queries
description: Syntax for the SELECT statement in ksqlDB for pull queries
keywords: ksqlDB, select, pull query
---

SELECT (Pull Query)
===================

Synopsis
--------

```sql
SELECT select_expr [, ...]
  FROM table
  WHERE key_column=key [AND ...]
  [AND window_bounds];
```

Description
-----------

Pulls the current value from the materialized view and terminates. The result
of this statement isn't persisted in a {{ site.ak }} topic and is printed out
only in the console.

Pull queries enable you to fetch the current state of a materialized view.
Because materialized views are incrementally updated as new events arrive,
pull queries run with predictably low latency. They're a great match for
request/response flows. For asynchronous application flows, see
[Push Queries](select-push-query.md).

Execute a pull query by sending an HTTP request to the ksqlDB REST API, and
the API responds with a single response.  

The WHERE clause must contain a value for each primary-key column to retrieve and may
optionally include bounds on `WINDOWSTART` and `WINDOWEND` if the materialized view is windowed.
For more information, see 
[Time and Windows in ksqlDB](../../concepts/time-and-windows-in-ksqldb-queries.md).

Example
-------
You can issue a pull query against a derived table that was created by using 
the [CREATE TABLE AS SELECT](../../ksqldb-reference/create-table-as-select) 
statement. First, create a table named `GRADES` by using a [CREATE TABLE](../../ksqldb-reference/create-table) 
statement:
```sql
CREATE TABLE GRADES (ID INT PRIMARY KEY, GRADE STRING, RANK INT) 
  WITH (kafka_topic = 'test_topic', value_format = 'JSON', partitions = 1);
```
Then, create a derived table named `TOP_TEN_RANKS` by using a 
[CREATE TABLE AS SELECT](../../ksqldb-reference/create-table-as-select) statement:
```sql
CREATE TABLE TOP_TEN_RANKS 
  AS SELECT ID, RANK 
  FROM GRADES 
  WHERE RANK <= 10;
```
You can fetch the current state of your materialized view `TOP_TEN_RANKS` by using a pull query:
```sql
SELECT * FROM TOP_TEN_RANKS;
```
If you want to look up only the student with `ID = 5` in the materialized view:
```sql
SELECT * FROM TOP_TEN_RANKS
  WHERE ID = 5;
```
If the materialized view `pageviews_by_region` is windowed:
```sql
SELECT * FROM pageviews_by_region
  WHERE regionId = 'Region_1'
    AND 1570051876000 <= WINDOWSTART AND WINDOWEND <= 1570138276000;
```

If the `pageviews_by_region` table was created as an aggregation of multiple columns,
then each key column must be present in the WHERE clause. The following example shows how to 
query the table if `countryId` and `regionId` where both key columns:

```sql
SELECT * FROM pageviews_by_region
  WHERE countryId = 'USA' AND regionId = 'Region_1'
    AND 1570051876000 <= WINDOWSTART AND WINDOWEND <= 1570138276000;
```

When writing logical expressions using `WINDOWSTART` or `WINDOWEND`, you can use ISO-8601
formatted datestrings to represent date times. For example, the previous
query is equivalent to the following:

```sql
SELECT * FROM pageviews_by_region
  WHERE regionId = 'Region_1'
    AND '2019-10-02T21:31:16' <= WINDOWSTART AND WINDOWEND <= '2019-10-03T21:31:16';
```

You can specify time zones within the datestring. For example,
`2017-11-17T04:53:45-0330` is in the Newfoundland time zone. If no time zone is
specified within the datestring, then timestamps are interpreted in the UTC
time zone.

If no bounds are placed on `WINDOWSTART` or `WINDOWEND`, rows are returned for all windows
in the windowed table.

You can also issue pull queries against materialized views that are created using 
joining multiple tables:
```sql
CREATE TABLE LEFT_TABLE (ID BIGINT PRIMARY KEY, NAME varchar, VALUE bigint) 
  WITH (kafka_topic='left_topic', value_format='json', partitions=4);
```
```sql
CREATE TABLE RIGHT_TABLE (ID BIGINT PRIMARY KEY, F1 varchar, F2 bigint) 
  WITH (kafka_topic='right_topic', value_format='json', partitions=4);
```
```sql
CREATE TABLE INNER_JOIN AS SELECT L.ID, NAME, VALUE, F1, F2 FROM LEFT_TABLE L JOIN RIGHT_TABLE R ON L.ID = R.ID;
```
You can fetch the current state of your materialized view `INNER_JOIN` by using a pull query:
```sql
SELECT * FROM INNER_JOIN [ WHERE where_condition ];
```
