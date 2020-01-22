---
layout: page
title: SELECT (Push Query)
tagline:  ksqlDB SELECT statement for push queries
description: Syntax for the SELECT statement in ksqlDB for push queries
keywords: ksqlDB, select, push query
---

SELECT (Push Query)
===================

Synopsis
--------

```sql
SELECT select_expr [, ...]
  FROM from_item
  [ LEFT JOIN join_table ON join_criteria ]
  [ WINDOW window_expression ]
  [ WHERE condition ]
  [ GROUP BY grouping_expression ]
  [ HAVING having_expression ]
  EMIT CHANGES
  [ LIMIT count ];
```

Description
-----------

Push a continuous stream of updates to the ksqlDB stream or table. The result of
this statement isn't persisted in a Kafka topic and is printed out only in
the console. To stop the continuous query in the CLI press Ctrl+C.
Note that the WINDOW clause can only be used if the `from_item` is a
stream.

Push queries enable you to query a materialized view with a subscription to
the results. Push queries emit refinements to materialized views, which enable
reacting to new information in real-time. They’re a good fit for asynchronous
application flows. For request/response flows, see [Pull Queries](select-pull-query.md).

Execute a push query by sending an HTTP request to the ksqlDB REST API, and
the API sends back a chunked response of indefinite length.

In the previous statements, `from_item` is one of the following:

-   `stream_name [ alias ]`
-   `table_name [ alias ]`
-   `from_item LEFT JOIN from_item ON join_condition`

The WHERE clause can refer to any column defined for a stream or table,
including the two implicit columns `ROWTIME` and `ROWKEY`.

Example
-------

The following statement shows how to select all records from a `pageviews`
stream that have timestamps between two values.

```sql
SELECT * FROM pageviews
  WHERE ROWTIME >= 1510923225000
    AND ROWTIME <= 1510923228000
  EMIT CHANGES;
```

When writing logical expressions using `ROWTIME`, you can use ISO-8601
formatted date strings to represent date times. For example, the previous
query is equivalent to the following:

```sql
SELECT * FROM pageviews
  WHERE ROWTIME >= '2017-11-17T04:53:45'
    AND ROWTIME <= '2017-11-17T04:53:48'
  EMIT CHANGES;
```

If the datestring is inexact, the rest of the timestamp is assumed to be
padded with 0s. For example, `ROWTIME = '2019-07-30T11:00'` is
equivalent to `ROWTIME = '2019-07-30T11:00:00.0000'`.

You can specify time zones within the datestring. For example,
`2017-11-17T04:53:45-0330` is in the Newfoundland time zone. If no
timezone is specified within the datestring, then timestamps are
interpreted in the UTC time zone.

You use the `LIMIT` clause to limit the number of rows returned. Once the
limit is reached, the query terminates.

The following statement shows how to select five records from a `pageviews`
stream.  

```sql
SELECT * FROM pageviews EMIT CHANGES LIMIT 5;
```

If no limit is supplied the query runs until terminated, streaming
back all results to the console.

!!! tip
      If you want to select older data, you can configure ksqlDB to
      query the stream from the beginning. You must run this configuration
      before running the query:

```sql
SET 'auto.offset.reset' = 'earliest';
```

#### WINDOW

The WINDOW clause lets you control how to group input records *that have
the same key* into so-called *windows* for operations like aggregations
or joins. Windows are tracked per record key. ksqlDB supports the following
WINDOW types.

**TUMBLING**: Tumbling windows group input records into fixed-sized,
non-overlapping windows based on the records' timestamps. You must
specify the *window size* for tumbling windows. Tumbling windows are a
special case of hopping windows, where the window size is equal to the
advance interval.

The following statement shows how to create a push query that has a tumbling
window.

```sql
SELECT item_id, SUM(quantity)
  FROM orders
  WINDOW TUMBLING (SIZE 20 SECONDS)
  GROUP BY item_id
  EMIT CHANGES;
```

**HOPPING**: Hopping windows group input records into fixed-sized,
(possibly) overlapping windows based on the records' timestamps. You
must specify the *window size* and the *advance interval* for
hopping windows.

The following statement shows how to create a push query that has a hopping
window.

```sql
SELECT item_id, SUM(quantity)
  FROM orders
  WINDOW HOPPING (SIZE 20 SECONDS, ADVANCE BY 5 SECONDS)
  GROUP BY item_id
  EMIT CHANGES;
```

**SESSION**: Session windows group input records into so-called
sessions. You must specify the *session inactivity gap* parameter
for session windows. For example, imagine you set the inactivity gap
to 5 minutes. If, for a given record key such as "alice", no new
input data arrives for more than 5 minutes, then the current session
for "alice" is closed, and any newly arriving data for "alice" in
the future will mark the beginning of a new session.

The following statement shows how to create a push query that has a session
window.

```sql
SELECT item_id, SUM(quantity)
  FROM orders
  WINDOW SESSION (20 SECONDS)
  GROUP BY item_id
  EMIT CHANGES;
```

Every output column of an expression in the SELECT list has an output
name. To specify the output name of a column, use `AS OUTPUT_NAME` after
the expression definition. If it is omitted, ksqlDB will assign a system
generated name `KSQL_COL_i` where `i` is the ordinal number of the
expression in the SELECT list. If the expression references a column of
a `from_item`, then the output name is the name of that column.


ksqlDB throws an error for duplicate output names. For example:

```sql
SELECT 1, KSQL_COL_0
  FROM orders
  EMIT CHANGES;
```

is not allowed, as the output name for the literal `1` is `KSQL_COL_0`.

#### CAST

**Synopsis**

```sql
CAST (expression AS data_type);
```

You can cast an expression's type to a new type using CAST.

The following query converts a numerical count, which is a BIGINT, into a
suffixed string, which is a VARCHAR. For example, the integer `5` becomes
`5_HELLO`.

```sql
SELECT page_id, CONCAT(CAST(COUNT(*) AS VARCHAR), '_HELLO')
  FROM pageviews_enriched
  WINDOW TUMBLING (SIZE 20 SECONDS)
  GROUP BY page_id;
```

#### CASE

**Synopsis**

```sql
CASE
   WHEN condition THEN result
   [ WHEN ... THEN ... ]
   …
   [ WHEN … THEN … ]
   [ ELSE result ]
END
```

ksqlDB supports a `searched` form of CASE expression. In this form, CASE
evaluates each boolean `condition` in WHEN clauses, from left to right.
If a condition is true, CASE returns the corresponding result. If none of
the conditions is true, CASE returns the result from the ELSE clause. If
none of the conditions is true and there is no ELSE clause, CASE returns null.

The schema for all results must be the same, otherwise ksqlDB rejects the
statement.

The following push query uses a a CASE expression.

```sql
SELECT
 CASE
   WHEN orderunits < 2.0 THEN 'small'
   WHEN orderunits < 4.0 THEN 'medium'
   ELSE 'large'
 END AS case_result
FROM orders
EMIT CHANGES;
```

#### LIKE

**Synopsis**

```sql
column_name LIKE pattern;
```

The LIKE operator is used for prefix or suffix matching. ksqlDB supports
the `%` wildcard, which represents zero or more characters.

The following push query uses the `%` wildcard to match any `user_id` that
starts with "santa".

```sql
SELECT user_id
  FROM users
  WHERE user_id LIKE 'santa%'
  EMIT CHANGES;
```

#### BETWEEN

**Synopsis**

```sql
WHERE expression [NOT] BETWEEN start_expression AND end_expression;
```

The BETWEEN operator is used to indicate that a certain value must lie
within a specified range, inclusive of boundaries. ksqlDB supports any
expression that resolves to a numeric or string value for comparison.

The following push query uses the between clause to select only records
that have an `event_id` between 10 and 20.

```sql
SELECT event
  FROM events
  WHERE event_id BETWEEN 10 AND 20
  EMIT CHANGES;
```



Page last revised on: {{ git_revision_date }}
